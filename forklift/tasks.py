from boto.s3.key import Key
import celery
from celery.exceptions import MaxRetriesExceededError
import datetime
import uuid

import forklift.loaders.fact.hourly as loaders
from forklift.db.base import rds_cache_engine, redshift_engine
from forklift.db.utils import cache_table, checkout_connection
from forklift import facebook
from forklift.loaders import neo_fbsync
from forklift.loaders.fbsync import FeedChunk, POSTS, LINKS, LIKES, TOP_WORDS, FBSyncLoader
from forklift.nlp import tfidf
from forklift.s3.utils import get_conn_s3, write_string_to_key, key_to_string
from celery.utils.log import get_task_logger

app = celery.Celery('forklift')
app.config_from_object('forklift.settings')
app.autodiscover_tasks(['forklift.tasks'])

logger = get_task_logger(__name__)


@app.task
def fbid_load_hour(hour):
    with checkout_connection() as connection:
        loaders.FbidHourlyFactLoader().load_hour(hour, connection, logger)


@app.task
def friend_fbid_load_hour(hour):
    with checkout_connection() as connection:
        loaders.FriendFbidHourlyFactLoader().load_hour(hour, connection, logger)


@app.task
def visit_load_hour(hour):
    with checkout_connection() as connection:
        loaders.VisitHourlyFactLoader().load_hour(hour, connection, logger)


@app.task
def misc_load_hour(hour):
    with checkout_connection() as connection:
        loaders.MiscHourlyFactLoader().load_hour(hour, connection, logger)


@app.task
def ip_load_hour(hour):
    with checkout_connection() as connection:
        loaders.IpHourlyFactLoader().load_hour(hour, connection, logger)


def key_name(version, prefix, unique_id):
    return "/".join((
        COMMON_OUTPUT_PREFIX,
        str(version),
        prefix,
        unique_id,
    ))


COMMON_OUTPUT_PREFIX = "transfer_batches"
POSTS_FOLDER = 'posts'
LINKS_FOLDER = 'links'
LIKES_FOLDER = 'likes'
TOP_WORDS_FOLDER = 'top_words'


class VectorizerTask(app.Task):
    abstract = True
    _vectorizer = None

    @property
    def vectorizer(self):
        if self._vectorizer is None:
            logger.info("Loading default vectorizer")
            self._vectorizer = tfidf.load_default_vectorizer(get_conn_s3())
            logger.info("Loaded default vectorizer")
        return self._vectorizer


@app.task(base=VectorizerTask, bind=True, default_retry_delay=5, max_retries=3, acks_late=True)
def fbsync_process(self, keys, version, out_bucket_name, unique_id):
    try:
        feed_chunk = FeedChunk(self.vectorizer, logger)
        s3_conn = get_conn_s3()
        for bucket_name, primary, secondary in keys:
            key = Key(
                bucket=s3_conn.get_bucket(bucket_name),
                name='{}_{}'.format(primary, secondary)
            )
            feed_chunk.add_feed_from_key(key)

        key_names = {
            POSTS: key_name(version, POSTS_FOLDER, unique_id),
            LINKS: key_name(version, LINKS_FOLDER, unique_id),
            LIKES: key_name(version, LIKES_FOLDER, unique_id),
            TOP_WORDS: key_name(version, TOP_WORDS_FOLDER, unique_id),
        }

        logger.info("Writing chunk to s3: %s", key_names)
        feed_chunk.write_s3(
            s3_conn,
            out_bucket_name,
            key_names
        )

        return (
            feed_chunk.counts[POSTS],
            feed_chunk.counts[LINKS],
            feed_chunk.counts[LIKES]
        )
    except Exception:
        identifier = "run {}/chunk {}".format(version, unique_id)
        logger.exception("%s failed due to error", identifier)
        try:
            self.retry()
        except MaxRetriesExceededError:
            logger.exception("%s had too many retries, failing", identifier)
            return (0,0,0)


@app.task
def fbsync_load(totals, out_bucket, version):
    total = reduce(
        lambda (posts, links, likes), (p, u, l): (posts+p, links+u, likes+l),
        totals,
        (0, 0, 0)
    )
    logger.info("Adding new data from run %s: posts = %s, user_posts = %s, likes = %s",
        version,
        *total
    )
    try:
        loader = FBSyncLoader(engine=redshift_engine, logger=logger)
        loader.add_new_data(
            out_bucket,
            COMMON_OUTPUT_PREFIX,
            version,
            POSTS_FOLDER,
            LINKS_FOLDER,
            LIKES_FOLDER,
            TOP_WORDS_FOLDER
        )
        logger.info("Done adding new data from run %s", version)
    except Exception:
        logger.exception("Worker processing run %s caught error", version)

    more_tables_to_sync = (
        'users',
        'user_aggregates',
    )
    for aggregate_table in more_tables_to_sync:
        cache_table(
            redshift_engine,
            rds_cache_engine,
            '{}_staging'.format(aggregate_table),
            aggregate_table,
            '{}_old'.format(aggregate_table),
            ','
        )


NEO_JSON_BUCKET = 'fbsync_neo_json'
NEO_CSV_BUCKET ='fbsync_neo_csv'


@app.task
def extract_url(url, asid, endpoint, update_ts=False):
    token = "implement dynamo get"
    data = facebook.utils.urlload(url, access_token=token)
    unique_identifier = uuid.uuid4()
    bucket_name = NEO_JSON_BUCKET
    key_name = "{}/{}".format(endpoint, unique_identifier)
    write_string_to_key(bucket_name, key_name, data)

    db_maxtime = "implement dynamo get"
    if update_ts:
        print "implement dynamo update"

    min_datetime = datetime.today()
    if data.get('data'):
        for item in data['data']:
            if item.get('created_time'):
                created_time = facebook.utils.parse_ts(item.get('created_time'))
                if created_time < min_datetime:
                    min_datetime = created_time
            next_comms = item.get('comments', {}).get('paging', {}).get('next', {})
            if next_comms:
                extract_url.delay(next_comms, asid, endpoint)

            next_likes = item.get('likes', {}).get('paging', {}).get('next', {})
            if next_likes:
                extract_url.delay(next_likes, asid, endpoint)

        if min_datetime < db_maxtime:
            next_url = data.get('paging', {}).get('next')
            if next_url:
                extract_url.delay(next_url, asid, endpoint)

    transform_page.delay(bucket_name, key_name, endpoint, asid)


@app.task
def transform_page(bucket_name, key_name, data_type, asid):
    input_data = key_to_string(bucket_name, key_name)
    TRANSFORM_MAP = {
        'statuses': neo_fbsync.transform_stream,
        'links': neo_fbsync.transform_stream,
        'photos': neo_fbsync.transform_stream,
        'photos/uploaded': neo_fbsync.transform_stream,
        'videos': neo_fbsync.transform_stream,
        'videos/uploaded': neo_fbsync.transform_stream,
        'permissions': neo_fbsync.transform_permissions,
        '': neo_fbsync.transform_public_profile,
        'activities': neo_fbsync.transform_activities,
        'interests': neo_fbsync.transform_interests,
        'likes': neo_fbsync.transform_likes,
        'taggable_friends': neo_fbsync.transform_taggable_friends,
    }

    for table, output_data in TRANSFORM_MAP[data_type](input_data, asid, data_type).iteritems():
        write_string_to_key(NEO_CSV_BUCKET, key_name, output_data)
        load_file.delay(NEO_CSV_BUCKET, key_name, table)


@app.task
def load_file(bucket_name, key_name, table_name, asid):
    # implement
    compute_top_words.delay(asid)


@app.task
def public_page_data(page_id):
    # retrieve from FB
    # upsert
    pass


@app.task
def compute_top_words(asid):
    # get corpus from redshift
    # compute
    # upsert (well, delete and replace really)
    pass
