from boto.s3.key import Key
import celery
from celery.exceptions import MaxRetriesExceededError

import forklift.loaders.fact.hourly as loaders
from forklift.db.base import redshift_engine
from forklift.db.utils import checkout_connection
from forklift.loaders.fbsync import FeedChunk, POSTS, LINKS, LIKES, TOP_WORDS, add_new_data
from forklift.nlp import tfidf
from forklift.s3.utils import get_conn_s3
import logging

app = celery.Celery('forklift')
app.config_from_object('forklift.settings')
app.autodiscover_tasks(['forklift.tasks'])

logger = logging.getLogger()


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


@app.task(base=VectorizerTask, bind=True, default_retry_delay=5, max_retries=3)
def fbsync_process(self, keys, version, out_bucket_name, unique_id):
    try:
        feed_chunk = FeedChunk(self.vectorizer)
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

        logger.info("Writing chunk to s3: {}".format(key_names))
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
    except Exception as exc:
        identifier = "run {}/chunk {}".format(version, unique_id)
        logger.warning("{} failed due to error {}, retrying".format(identifier, exc))
        try:
            self.retry()
        except MaxRetriesExceededError:
            logger.error("{} had too many retries, failing".format(identifier))
            return (0,0,0)


@app.task
def fbsync_load(totals, out_bucket, version):
    total = reduce(
        lambda (posts, links, likes), (p, u, l): (posts+p, links+u, likes+l),
        totals,
        (0, 0, 0)
    )
    logger.info("Adding new data from run {}: posts = {}, user_posts = {}, likes = {}".format(
        version,
        *total
    ))
    try:
        add_new_data(
            out_bucket,
            COMMON_OUTPUT_PREFIX,
            version,
            POSTS_FOLDER,
            LINKS_FOLDER,
            LIKES_FOLDER,
            TOP_WORDS_FOLDER,
            redshift_engine
        )
        logger.info("Done adding new data from run {}".format(version))
    except Exception as e:
        logger.error(
            "Worker processing run {} caught error. Original exception: {}".format(
                version, e
        ))
