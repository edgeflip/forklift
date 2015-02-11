from boto.s3.key import Key
import traceback
import celery
from celery.exceptions import MaxRetriesExceededError
from datetime import datetime
import sys
import os
import unicodecsv
import re
import tempfile
import uuid
import json

from forklift.db.base import rds_cache_engine, redshift_engine, RDSCacheSession, RDSSourceSession
from forklift.db import utils as dbutils
from forklift import facebook
from forklift.loaders import neo_fbsync
from forklift.loaders.fbsync import FeedChunk, POSTS, LINKS, LIKES, TOP_WORDS, FBSyncLoader
from forklift.models.fbsync import FBSyncPageTask, FBSyncRunList, FBSyncRun
from forklift.models.raw import FBToken
from forklift.nlp import tfidf
from forklift.s3.utils import get_conn_s3, write_string_to_key, key_to_string, write_file_to_key, get_bucket
from celery.utils.log import get_task_logger


app = celery.Celery('forklift')
app.config_from_object('forklift.settings')
#app.conf.CELERY_ALWAYS_EAGER = True
#app.conf.CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
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


NEO_JSON_BUCKET = 'fbsync-neo-json'
NEO_CSV_BUCKET ='fbsync-neo-csv'


def always_be_crawling(efid, appid, run_id, stop_datetime):
    print "running but stopping at", stop_datetime
    for crawl_type, endpoint in neo_fbsync.ENDPOINTS.iteritems():
        if endpoint.entity_type == neo_fbsync.USER_ENTITY_TYPE:
            extract_url.delay(
                neo_fbsync.get_user_endpoint(efid, endpoint.endpoint),
                run_id,
                efid,
                appid,
                crawl_type,
                stop_datetime=stop_datetime
            )


class ExpiredTokenError(IOError):
    # TODO: what should we really inherit from?
    pass


@app.task(bind=True, default_retry_delay=5, max_retries=5)
def extract_url(
    self,
    url,
    run_id,
    efid,
    appid,
    crawl_type,
    post_id=None,
    from_asid=None,
    stop_datetime=None
):
    try:
        prod_session = RDSSourceSession()
        token = prod_session.query(FBToken).get((efid, appid))
        if token is None or token.expiration < datetime.today():
            raise ExpiredTokenError("Expired token for {}".format(efid))
        db_session = RDSCacheSession()

        # update the task log
        unique_identifier = str(uuid.uuid4())
        key_name = "{}/{}/{}.json".format(run_id, crawl_type, unique_identifier)
        task_log, new = dbutils.get_one_or_create(
            db_session,
            FBSyncPageTask,
            efid=efid,
            crawl_type=crawl_type,
            url=url,
        )
        task_log.last_run_id = run_id
        task_log.key_name = key_name
        db_session.commit()

        data = facebook.utils.urlload(url, access_token=token.access_token, logger=logger)
        bucket_name = NEO_JSON_BUCKET

        min_datetime = datetime.today()
        if data.get('data') or crawl_type == 'public_profile':
            write_string_to_key(bucket_name, key_name, json.dumps(data))
            logger.info("Successfully wrote raw data to %s", key_name)

            for item in data.get('data', []):
                post_from = item.get('from', {}).get('id')
                if item.get('created_time'):
                    created_time = facebook.utils.parse_ts(item.get('created_time'))
                    if created_time < min_datetime:
                        min_datetime = created_time
                next_comms = item.get('comments', {}).get('paging', {}).get('next', {})
                if next_comms:
                    logger.info("Found comments in need of pagination for post_id %s", item['id'])
                    extract_url.delay(next_comms, run_id, efid, appid, 'post_comments', post_id=item['id'], from_asid=post_from)

                next_likes = item.get('likes', {}).get('paging', {}).get('next', {})
                if next_likes:
                    logger.info("Found likes in need of pagination for post_id %s", item['id'])
                    extract_url.delay(next_likes, run_id, efid, appid, 'post_likes', post_id=item['id'], from_asid=post_from)

                next_tags = item.get('tags', {}).get('tags', {}).get('next', {})
                if next_tags:
                    logger.info("Found tags in need of pagination for post_id %s", item['id'])
                    extract_url.delay(next_tags, run_id, efid, appid, 'post_tags', post_id=item['id'], from_asid=post_from)

            task_log.extracted = datetime.today()
            task_log.post_id = post_id
            task_log.post_from = from_asid
            db_session.commit()
        else:
            task_log.transformed = task_log.extracted = task_log.loaded = datetime.today()
            task_log.key_name = None
            db_session.commit()
            if(
                data.get('error', {}).get('type', "") == 'OAuthException'
                and data['error'].get('error_subcode') in [458, 459, 460, 463, 464, 467]
            ):
                token.expiration = datetime.today() # TODO: parse the real expiration from the message?
                prod_session.commit()
                raise ExpiredTokenError("Expired token for {}, return value = {}".format(efid, data))

        next_url = data.get('paging', {}).get('next')
        if next_url and stop_datetime and min_datetime > stop_datetime:
            logger.info("More items are available for efid %d, crawl_type %s", efid, crawl_type)
            extract_url.delay(next_url, run_id, efid, appid, crawl_type, post_id=post_id, from_asid=from_asid, stop_datetime=stop_datetime)
        else:
            pages_to_transform = db_session.query(FBSyncPageTask).filter_by(
                efid=efid,
                crawl_type=crawl_type,
                transformed=None,
                last_run_id=str(run_id),
            ).all()
            if len(pages_to_transform) > 0:
                logger.info(
                    "Done with extraction for efid %d, crawl_type %s, run_id %s, kicking off %d transform jobs",
                    efid,
                    crawl_type,
                    run_id,
                    len(pages_to_transform)
                )
            for page in pages_to_transform:
                transform_page.delay(
                    bucket_name,
                    page.key_name,
                    page.crawl_type,
                    efid,
                    appid,
                    run_id,
                    page.url,
                    page.post_id,
                    page.post_from
                )
    except ExpiredTokenError, exc:
        logger.error("Giving up on run_id %s, efid %d, url %s due to \"%s\"", run_id, efid, url, exc)
    except Exception, exc:
        try:
            logger.error("Retrying extraction of run_id %s, efid %d, url %s due to \"%s\"", run_id, efid, url, exc)
            self.retry(exc=exc)
        except MaxRetriesExceededError:
            logger.error("Too many retries, giving up on run_id %s, efid %d, url %s", run_id, efid, url)
            task_log.transformed = task_log.extracted = task_log.loaded = datetime.today()
            task_log.key_name = None
            db_session.commit()


@app.task(bind=True, default_retry_delay=2, max_retries=2)
def transform_page(self, bucket_name, json_key_name, crawl_type, efid, appid, run_id, url, post_id=None, from_asid=None):
    try:
        input_string = key_to_string(bucket_name, json_key_name)

        db_session = RDSCacheSession()
        task_set = db_session.query(FBSyncPageTask).filter_by(
            efid=efid,
            crawl_type=crawl_type,
            url=url,
        )
        task_log = task_set.one()
        if not task_log:
            logger.error("Task log not found")

        if input_string:
            input_data = json.loads(input_string)
            transformer = neo_fbsync.ENDPOINTS[crawl_type].transformer

            output = transformer(
                input_data,
                efid,
                appid,
                crawl_type,
                post_id=post_id,
                post_from=from_asid
            )
            if not output:
                logger.error("No transformed data found")
            else:
                logger.info("Got transformed data")

            for table, output_data in output.iteritems():
                file_obj = tempfile.NamedTemporaryFile()
                writer = unicodecsv.writer(file_obj, delimiter=',')
                for row in output_data:
                    writer.writerow(row)
                logger.info("Trying to write {}".format(table))
                unique_identifier = str(uuid.uuid4())
                csv_key_name = "{}/{}/{}.csv".format(run_id, table, unique_identifier)
                file_obj.seek(0, os.SEEK_SET)
                write_file_to_key(NEO_CSV_BUCKET, csv_key_name, file_obj)

        task_log.transformed = datetime.today()
        db_session.commit()
        check_load.delay(run_id)

    except Exception, exc:
        try:
            cleaned_url = re.sub("access_token=.*&", "", url)
            logger.error("Retrying transformation of run_id %s, efid %d, url %s due to \"%s\"", run_id, efid, cleaned_url, traceback.format_exc())
            self.retry(exc=exc)
        except MaxRetriesExceededError:
            logger.error("Too many retries, giving up on run_id %s, efid %d, url %s", run_id, efid, url)
            task_log.transformed = task_log.loaded = datetime.today()
            task_log.key_name = None
            db_session.commit()


@app.task
def check_load(run_id):
    transform_not_done = rds_cache_engine.execute("""
        select 1
        from fbsync_run_lists rl
        join fbsync_page_tasks pt on (rl.run_id = pt.last_run_id and rl.efid = pt.efid)
        where rl.run_id = '{}'
        and (
            pt.extracted is null or
            pt.transformed is null
        )
    """.format(run_id)).fetchone()
    if transform_not_done:
        print "transform not done"
        return

    print "transform done, so let's check load"
    load_not_started = rds_cache_engine.execute(
        "update fbsync_runs set load_started = true where run_id = '{}' and load_started = false returning 1".format(run_id)
    ).fetchone()
    if load_not_started:
        print "load not started"
        load_run.delay(run_id)
    else:
        print "load started"


def incremental_table_name(table_base, version):
    return "{}_{}".format(table_base, version)


@app.task
def load_run(run_id):
    try:
        bucket_name = NEO_CSV_BUCKET
        bucket = get_bucket(bucket_name)
        prefix = "{}/".format(run_id)
        paths = [t.name for t in bucket.list(prefix=prefix, delimiter='/')]
        for path in paths:
            table_name = 'v2_' + path.replace(prefix, '').replace('/', '')
            inc_table = incremental_table_name(table_name, run_id)
            with redshift_engine.connect() as connection:
                with connection.begin():
                    dbutils.create_new_table(inc_table, table_name, connection)
                    dbutils.load_from_s3(
                        connection,
                        bucket_name,
                        path,
                        inc_table,
                        delim=',',
                    )
    except Exception, e:
        print "died at", e


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
                delim=',',
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

