from abc import ABCMeta, abstractproperty, abstractmethod

from boto.s3.key import Key
import celery
from celery.exceptions import MaxRetriesExceededError
import datetime
import uuid
from collections import defaultdict

from forklift.db.base import rds_cache_engine, redshift_engine
from forklift.db import utils as dbutils
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
        dbutils.cache_table(
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
def extract_url(url, entity_id, data_type, update_ts=False):
    token = "implement dynamo get"
    data = facebook.utils.urlload(url, access_token=token)
    unique_identifier = uuid.uuid4()
    bucket_name = NEO_JSON_BUCKET
    key_name = "{}/{}".format(data_type, unique_identifier)
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
                extract_url.delay(next_comms, item['id'], 'post_comments')

            next_likes = item.get('likes', {}).get('paging', {}).get('next', {})
            if next_likes:
                extract_url.delay(next_likes, item['id'], 'post_likes')

            next_tags = item.get('tags', {}).get('tags', {}).get('next', {})
            if next_tags:
                extract_url.delay(next_tags, item['id'], 'post_tags')

        if min_datetime < db_maxtime:
            next_url = data.get('paging', {}).get('next')
            if next_url:
                extract_url.delay(next_url, entity_id, data_type)

    transform_page.delay(bucket_name, key_name, data_type, entity_id)


@app.task
def transform_page(bucket_name, key_name, data_type, entity_id):
    input_data = key_to_string(bucket_name, key_name)
    TRANSFORM_MAP = {
        'statuses': neo_fbsync.transform_stream,
        'links': neo_fbsync.transform_stream,
        'photos': neo_fbsync.transform_stream,
        'uploaded_photos': neo_fbsync.transform_stream,
        'videos': neo_fbsync.transform_stream,
        'uploaded_videos': neo_fbsync.transform_stream,
        'permissions': neo_fbsync.transform_permissions,
        'public_profile': neo_fbsync.transform_public_profile,
        'activities': neo_fbsync.transform_activities,
        'interests': neo_fbsync.transform_interests,
        'page_likes': neo_fbsync.transform_page_likes,
        'post_likes': neo_fbsync.transform_post_likes,
        'post_comments': neo_fbsync.transform_post_comments,
        'post_tags': neo_fbsync.transform_post_tags,
        'taggable_friends': neo_fbsync.transform_taggable_friends,
    }

    for table, output_data in TRANSFORM_MAP[data_type](input_data, entity_id, data_type).iteritems():
        write_string_to_key(NEO_CSV_BUCKET, key_name, output_data)
        load_file.delay(NEO_CSV_BUCKET, key_name, table)


@app.task
def load_file(bucket_name, key_name, table_name, entity_id, entity_type):
    raw_table = dbutils.raw_table_name(table_name)
    logger.info(
        'Loading raw data into %s from s3://%s/%s',
        raw_table,
        bucket_name,
        key_name
    )
    with redshift_engine.connect() as connection:
        with connection.begin():
            dbutils.load_from_s3(
                connection,
                bucket_name,
                key_name,
                raw_table,
            )
    log_rowcount(raw_table, '%s rows loaded into %s')
    if entity_type == 'efid':
        compute_aggregates.delay(entity_id)


def log_rowcount(table_name, engine=None, custom_msg=None):
    engine = engine or redshift_engine
    msg = custom_msg or '%s records found in %s'
    logger.info(
        msg,
        dbutils.get_rowcount(table_name, engine=engine),
        table_name
    )


@app.task
def compute_top_words(efid):
    # get corpus from redshift
    # compute
    # upsert (well, delete and replace really)
    pass


@app.task
def compute_aggregates(efid):
    # post_aggregates - one row per post, dependencies = (posts)
    # user_post_aggregates - one row per post+user combo, dependencies =
    # (post_tags, post_likes, comment, locales)
    # edges - one row per primary+secondary combo (inbound only), dependencies =
    # (user_posts_aggregates)
    # user_timeline_aggregates - one row per user, dependencies = (posts)
    # poster_aggregates - one row per user, dependencies = user_post_aggregates
    # user_aggregates - one row per user, dependencies =
    # (user_timeline_aggregates, poster_aggregates)
    compute_top_words.delay(efid)
    compute_post_aggregates.delay(efid)
    compute_user_post_aggregates.delay(efid)


def upsert(efid, final_table, efid_column_name, bound_select_query, engine=None):
    engine = engine or redshift_engine
    connection = engine.connect()
    with connection.begin():
        connection.execute(
            "DELETE FROM {table} where {efid_column_name} = {efid}"
        )

        connection.execute(
            """INSERT INTO {table}
            {select_query}""".format(
                table=final_table,
                select_query=bound_select_query,
            )
        )


@app.task
def compute_edges(efid):
    sql = """
        select
            {efid} as efid_primary,
            case when efid_poster = {efid} then efid_user else efid_poster end as efid_secondary,
            count(distinct case when post_type = 'photo' and efid_poster != {efid} and efid_poster != efid_user and user_tagged then post_id else null end) as photo_tags,
            count(distinct case when post_type = 'photo' and efid_poster != {efid} and user_liked then post_id else null end) as photo_likes,
            count(distinct case when post_type = 'photo' and efid_poster != {efid} and user_commented then post_id else null end) as photo_comms,
            count(distinct case when post_type = 'photo' and efid_poster != {efid} and efid_poster = efid_user and user_tagged then post_id else null end) as photos_target,
            count(distinct case when post_type = 'video' and efid_poster != {efid} and efid_poster != efid_user and user_tagged then post_id else null end) as video_tags,
            count(distinct case when post_type = 'video' and efid_poster != {efid} and user_liked then post_id else null end) as video_likes,
            count(distinct case when post_type = 'video' and efid_poster != {efid} and user_commented then post_id else null end) as video_comms,
            count(distinct case when post_type = 'video' and efid_poster != {efid} and efid_poster = efid_user and user_tagged then post_id else null end) as videos_target,
            count(distinct case when post_type = 'photo' and efid_poster = {efid} and user_tagged then post_id else null end) as uploaded_photo_tags,
            count(distinct case when post_type = 'photo' and efid_poster = {efid} and user_liked then post_id else null end) as uploaded_photo_likes,
            count(distinct case when post_type = 'photo' and efid_poster = {efid} and user_commented then post_id else null end) as uploaded_photo_comms,
            count(distinct case when post_type = 'video' and efid_poster = {efid} and user_tagged then post_id else null end) as uploaded_video_tags,
            count(distinct case when post_type = 'video' and efid_poster = {efid} and user_liked then post_id else null end) as uploaded_video_likes,
            count(distinct case when post_type = 'video' and efid_poster = {efid} and user_commented then post_id else null end) as uploaded_video_comms,
            count(distinct case when post_type = 'status' and user_tagged then post_id else null end) as stat_tags,
            count(distinct case when post_type = 'status' and user_liked then post_id else null end) as stat_likes,
            count(distinct case when post_type = 'status' and user_commented then post_id else null end) as stat_comms,
            count(distinct case when post_type = 'link' and user_tagged then post_id else null end) as link_tags,
            count(distinct case when post_type = 'link' and user_liked then post_id else null end) as link_likes,
            count(distinct case when post_type = 'link' and user_commented then post_id else null end) as link_comms,
            count(distinct case when user_placed then post_id else null end) as place_tags
        from {user_posts}
        group by 1, 2
    """.format(
        user_posts=neo_fbsync.USER_POSTS_AGGREGATE_TABLE,
        efid=efid,
    )

    upsert(
        efid,
        neo_fbsync.EDGES_TABLE,
        'efid_primary',
        sql
    )

@app.task
def compute_post_aggregates(efid):
    sql = """
        select
            posts.post_id,
            count(distinct likes.efid) as num_likes,
            count(distinct tagged.tagged_efid) as num_tags,
            count(distinct comments.comment_id) as num_comments,
            count(distinct comments.efid_commenter) as num_users_commented
        from
            {posts} posts
            left join {likes} likes on (likes.post_id = posts.post_id)
            left join {comments} comments on (comments.post_id = posts.post_id)
            left join {tagged} tagged on (tagged.post_id = posts.post_id)
            where posts.efid = {efid}
    """.format(
        posts=neo_fbsync.POSTS_TABLE,
        likes=neo_fbsync.POST_LIKES_TABLE,
        comments=neo_fbsync.POST_COMMENTS_TABLE,
        tagged=neo_fbsync.POST_TAGS_TABLE,
        efid=efid,
    )
    upsert(
        efid,
        neo_fbsync.POST_AGGREGATES_TABLE,
        'efid',
        sql
    )


@app.task
def compute_user_post_aggregates(efid_poster):
    sql = """
        select
            {posts}.post_id,
            {posts}.efid as efid_poster,
            {posts}.type as post_type,
            coalesce({tagged}.tagged_efid, {likes}.efid, {comments}.efid_commenter) as efid_user,
            {tagged}.tagged_efid is not null as user_tagged,
            {likes}.efid is not null as user_likes,
            count({comments}.efid_commenter is not null) > 0 as user_commented,
            count(distinct {comments}.comment_id) as num_comments,
            {locales}.tagged_efid is not null as user_placed
        from
            {posts} posts
            left join {likes} likes on (likes.post_id = posts.post_id)
            left join {comments} comments on (comments.post_id = posts.post_id)
            left join {tagged} tagged on (tagged.post_id = posts.post_id)
            left join {locales} locales on (locales.post_id = posts.post_id)
        where {posts}.efid = {efid}
        group by 3
    """.format(
        posts=neo_fbsync.POSTS_TABLE,
        likes=neo_fbsync.POST_LIKES_TABLE,
        comments=neo_fbsync.POST_COMMENTS_TABLE,
        tagged=neo_fbsync.POST_TAGS_TABLE,
        efid=efid_poster,
    )

    upsert(
        efid_poster,
        neo_fbsync.USER_POST_AGGREGATES_TABLE,
        'efid_poster',
        sql
    )

    callback = compute_user_aggregates.s(efid_poster)
    celery.chord([
        compute_user_timeline_aggregates.s(efid_poster),
        compute_poster_aggregates.s(efid_poster),
        compute_edges.s(efid_poster),
    ])(callback)


@app.task
def compute_user_timeline_aggregates(efid):
    sql = """
        select
            efid,
            date(min(ts)) as first_activity,
            date(max(ts)) as last_activity,
            count(distinct case when type = 'status' then post_id else null end) as num_stat_upd
        from {posts}
        where efid = {efid}
    """.format(
        posts=neo_fbsync.POSTS_TABLE,
        efid=efid,
    )

    upsert(
        efid,
        neo_fbsync.USER_TIMELINE_AGGREGATES_TABLE,
        'efid',
        sql
    )


@app.task
def compute_poster_aggregates(efid):
    sql = """
        select
            efid_poster,
            count(distinct post_id) num_posts,
            count(distinct case when user_likes then post_id else null end) as num_mine_liked,
            count(distinct case when user_likes then efid_user else null end) as num_friends_liked,
            count(distinct case when user_commented then post_id else null end) as num_mine_commented_on,
            count(distinct case when user_commented then efid_user else null end) as num_friends_commented,
            count(distinct case when user_tagged then post_id else null end) as num_i_shared,
            count(distinct case when user_tagged then efid_user else null end) as num_friends_i_tagged,
            count(distinct efid_user) as num_friends_interacted_with_my_posts
        from {user_posts}
        where efid_poster = {efid}
        group by 1
    """.format(
        user_posts=neo_fbsync.USER_POST_AGGREGATES_TABLE,
        efid=efid
    )

    upsert(
        efid,
        neo_fbsync.POSTER_AGGREGATES_TABLE,
        efid,
        sql
    )


@app.task
def compute_user_aggregates(efid):
    sql = """select
        *,
        case when num_posts > 0 then (last_activity - first_activity) / num_posts else NULL end as avg_time_between_activity,
        case when num_posts > 0 then num_friends_interacted_with_my_posts / num_posts else NULL end as avg_friends_interacted_with_my_posts,
        case when num_posts_interacted_with > 0 then num_friends_i_interacted_with / num_posts_interacted_with else NULL end as avg_friends_i_interacted_with
    from (
        select
            bool_or(user_clients.fbid is not null) as primary,
            u.fbid,
            max({datediff_expression}) as age,
            max(first_activity) as first_activity,
            max(last_activity) as last_activity,
            count(distinct {edges_table}.fbid_source) as num_friends,
            max(num_posts) as num_posts,
            max(num_posts_interacted_with) as num_posts_interacted_with,
            max(num_i_like) as num_i_like,
            max(num_i_comm) as num_i_comm,
            max(num_shared_w_me) as num_shared_w_me,
            max(num_mine_liked) as num_mine_liked,
            max(num_mine_commented) as num_mine_commented,
            max(num_i_shared) as num_i_shared,
            max(num_stat_upd) as num_stat_upd,
            max(num_friends_interacted_with_my_posts) as num_friends_interacted_with_my_posts,
            max(num_friends_i_interacted_with) as num_friends_i_interacted_with,
        join {users} u using (fbid)
        left join {edges} on (u.fbid = {edges}.fbid_primary)
        left join {post_aggregates} on (u.efid = {post_aggregates_table}.efid)
        left join {poster_aggregates} me_as_poster on (u.efid = me_as_poster.efid_poster)
        left join user_clients on (u.efid = user_clients.efid)
        group by u.fbid
    ) sums
    """.format(
        users=neo_fbsync.USERS_TABLE,
        edges=neo_fbsync.EDGES_TABLE,
        post_aggregates=neo_fbsync.POST_AGGREGATES_TABLE,
        poster_aggregates=neo_fbsync.POSTER_AGGREGATES_TABLE,
        datediff_expression=neo_fbsync.datediff_expression(),
    )
    upsert(
        efid,
        neo_fbsync.USER_AGGREGATES_TABLE,
        efid,
        sql
    )


@app.task
def public_page_data(page_id):
    # retrieve from FB
    # upsert
    pass

class FBEntity(object):
    __metaclass__ = ABCMeta


class UserEntity(FBEntity):
    pass

class PostEntity(FBEntity):
    pass

class FBEndpoint(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def endpoint(self):
        pass

    @abstractproperty
    def transformer(self):
        pass

    def url(self, entity_id):
        return 'https://graph.facebook.com/v2.2/{}/{}'.format(
            entity_id,
            self.endpoint,
        )



class FBTransformer(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def loader(self):
        pass

    @abstractmethod
    def run(self, input_data, entity_id, data_type):
        pass

class StreamTransformer(object):

    def generate_csv(self, input_data, entity_id, data_type):
        output_lines = defaultdict(list)
        return output_lines


class CSVLoader(object):
    __
