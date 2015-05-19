import traceback
import celery
from celery.exceptions import MaxRetriesExceededError
from datetime import datetime
import os
import unicodecsv
import re
import tempfile
import uuid
import json
from kombu import Exchange, Queue

from forklift.db.base import rds_cache_engine, redshift_engine, RDSCacheSession
from forklift.db import utils as dbutils
from forklift import facebook
from forklift.loaders import fbsync
from forklift.models.fbsync import FBSyncPageTask, FBSyncRunList
from forklift.models.magnus import FBUserToken, FBAppUser
from forklift.s3.utils import write_string_to_key, key_to_string, write_file_to_key, get_bucket
from forklift.utils import batcher
from celery.utils.log import get_task_logger


app = celery.Celery('forklift')
app.config_from_object('forklift.settings')
app.autodiscover_tasks(['forklift.tasks'])

logger = get_task_logger(__name__)
logger.propagate = False

JSON_BUCKET = 'fbsync-json'
CSV_BUCKET = 'fbsync-csv'


class ExpiredTokenError(IOError):
    # TODO: what should we really inherit from?
    pass


@app.task(bind=True, default_retry_delay=5, max_retries=5)
def extract_url(
    self,
    url,
    run_id,
    efid,
    fbid,
    appid,
    crawl_type,
    post_id=None,
    from_asid=None,
    stop_datetime=None,
):
    try:
        db_session = RDSCacheSession()
        token = db_session.query(FBUserToken).\
            filter(FBUserToken.app_user_id==FBAppUser.app_user_id).\
            filter(FBAppUser.efid==efid).\
            filter(FBAppUser.fb_app_id==appid).\
            filter(FBUserToken.expiration > datetime.today()).\
            one()
        if token is None:
            raise ExpiredTokenError("Expired token for {}".format(efid))

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
        bucket_name = JSON_BUCKET

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
                    extract_url.delay(next_comms, run_id, efid, fbid, appid, 'post_comments', post_id=item['id'], from_asid=post_from)

                next_likes = item.get('likes', {}).get('paging', {}).get('next', {})
                if next_likes:
                    logger.info("Found likes in need of pagination for post_id %s", item['id'])
                    extract_url.delay(next_likes, run_id, efid, fbid, appid, 'post_likes', post_id=item['id'], from_asid=post_from)

                next_tags = item.get('tags', {}).get('tags', {}).get('next', {})
                if next_tags:
                    logger.info("Found tags in need of pagination for post_id %s", item['id'])
                    extract_url.delay(next_tags, run_id, efid, fbid, appid, 'post_tags', post_id=item['id'], from_asid=post_from)

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
                db_session.commit()
                raise ExpiredTokenError("Expired token for {}, return value = {}".format(efid, data))

        next_url = data.get('paging', {}).get('next')
        if next_url and stop_datetime and min_datetime > stop_datetime:
            logger.info("More items are available for efid %d, crawl_type %s", efid, crawl_type)
            extract_url.delay(next_url, run_id, efid, fbid, appid, crawl_type, post_id=post_id, from_asid=from_asid, stop_datetime=stop_datetime)
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
        db_session.close()
        task_log = task_set.one()
        if not task_log:
            logger.error("Task log not found")

        if input_string:
            input_data = json.loads(input_string)
            transformer = fbsync.ENDPOINTS[crawl_type].transformer

            output = transformer(
                input_data,
                efid,
                appid,
                crawl_type,
                post_id=post_id,
                post_from=from_asid
            )
            if not output:
                logger.error("No transformed data produced")

            for table, output_data in output.iteritems():
                file_obj = tempfile.NamedTemporaryFile()
                writer = unicodecsv.writer(file_obj, delimiter=',')
                for row in output_data:
                    writer.writerow(row)
                unique_identifier = str(uuid.uuid4())
                csv_key_name = "{}/{}/{}.csv".format(run_id, table, unique_identifier)
                logger.info("Writing to {}".format(csv_key_name))
                file_obj.seek(0, os.SEEK_SET)
                write_file_to_key(CSV_BUCKET, csv_key_name, file_obj)

        db_session = RDSCacheSession()
        task_log.transformed = datetime.today()
        db_session.merge(task_log)
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
    try:
        transform_not_done = rds_cache_engine.execute("""
            select 1
            from "forklift"."fbsync_run_lists" rl
            join "forklift"."fbsync_page_tasks" pt on (rl.run_id = pt.last_run_id and rl.efid = pt.efid)
            where rl.run_id = '{}'
            and (
                pt.extracted is null or
                pt.transformed is null
            )
        """.format(run_id)).fetchone()
        if transform_not_done:
            logger.info("Transform phase for run_id %s not done yet", run_id)
            return

        load_not_started = rds_cache_engine.execute(
            """update "forklift"."fbsync_runs" set load_started = true where run_id = '{}' and load_started = false returning 1""".format(run_id)
        ).fetchone()

        if load_not_started:
            logger.info("Kicking off load for run_id %s", run_id)
            load_run.delay(run_id)
        else:
            logger.info("Load already started for run_id %s, skipping", run_id)
    except Exception, exc:
        logger.error("check_load could not run because of '%s'", exc)


@app.task(bind=True, default_retry_delay=5, max_retries=5)
def load_run(self, run_id):
    try:
        bucket_name = CSV_BUCKET
        bucket = get_bucket(bucket_name)
        prefix = "{}/".format(run_id)
        paths = [t.name for t in bucket.list(prefix=prefix, delimiter='/')]
        logger.info("%d loadable paths found for run_id %s", len(paths), run_id)
        tables = []
        for path in paths:
            table_name = path.replace(prefix, '').replace('/', '')
            logger.info("Loading table %s", table_name)
            tables.append(table_name)
            inc_table = fbsync.incremental_table_name(table_name, run_id)
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
    except Exception, exc:
        logger.error("Retrying load of run_id %s due to \"%s\"", run_id, exc)
        self.retry(exc=exc)

    merge_run.delay(run_id, tables)


@app.task(bind=True, default_retry_delay=5, max_retries=5)
def merge_run(self, run_id, table_names):
    try:
        for table_name in table_names:
            pkey = fbsync.PRIMARY_KEYS[table_name]
            inc_table = fbsync.incremental_table_name(table_name, run_id)
            logger.info("Merging table %s using join fields: %s", inc_table, pkey)
            with redshift_engine.connect() as connection:
                with connection.begin():
                    connection.execute("""
                        insert into {full_table}
                        select {inc_table}.* from {inc_table}
                        left join {full_table} using ({join_conditions})
                        where {full_table}.{column_name} is null
                    """.format(
                        full_table=table_name,
                        inc_table=inc_table,
                        join_conditions=",".join(pkey),
                        column_name=pkey[0],
                    ))
        with redshift_engine.connect() as connection:
            with connection.begin():
                connection.execute("""
                    create table {affected_efids} as select efid from {users_inc}
                """.format(
                    affected_efids=fbsync.affected_efids(run_id),
                    users_inc=fbsync.incremental_table_name(fbsync.USERS_TABLE, run_id),
                ))
    except Exception, exc:
        logger.error("Retrying merge of run_id %s due to \"%s\"", run_id, exc)
        self.retry(exc=exc)

    clean_up_incremental_tables.delay(run_id, table_names)
    compute_aggregates.delay(run_id)


@app.task(bind=True, default_retry_delay=5, max_retries=5)
def clean_up_incremental_tables(self, run_id, table_names):
    try:
        for table_name in table_names:
            inc_table = fbsync.incremental_table_name(table_name, run_id)
            logger.info("Cleaning up incremental table %s", inc_table)
            with redshift_engine.connect() as connection:
                dbutils.drop_table_if_exists(inc_table, connection)
    except Exception, exc:
        logger.error("Retrying incremental table cleanup of run_id %s due to \"%s\"", run_id, exc)
        self.retry(exc=exc)


@app.task
def compute_aggregates(run_id):
    # post_aggregates - one row per post, dependencies = (posts)
    # user_post_aggregates - one row per post+user combo, dependencies =
    # (post_tags, post_likes, comment, locales)
    # edges - one row per primary+secondary combo (inbound only), dependencies =
    # (user_posts_aggregates)
    # user_timeline_aggregates - one row per user, dependencies = (posts)
    # poster_aggregates - one row per user, dependencies = user_post_aggregates
    # user_aggregates - one row per user, dependencies =
    # (user_timeline_aggregates, poster_aggregates)
    compute_post_aggregates.delay(run_id)
    compute_user_post_aggregates.delay(run_id)


@app.task
def compute_edges(run_id):
    try:
        sql = """
            select
                efid_wall as efid_primary,
                case when efid_poster = efid_wall then efid_user else efid_poster end as efid_secondary,
                count(distinct case when post_type = 'photo' and efid_poster != efid_wall and efid_poster != efid_user and user_tagged then post_id else null end) as photo_tags,
                count(distinct case when post_type = 'photo' and efid_poster != efid_wall and user_likes then post_id else null end) as photo_likes,
                count(distinct case when post_type = 'photo' and efid_poster != efid_wall and user_commented then post_id else null end) as photo_comms,
                count(distinct case when post_type = 'photo' and efid_poster != efid_wall and efid_poster = efid_user and user_tagged then post_id else null end) as photos_target,
                count(distinct case when post_type = 'video' and efid_poster != efid_wall and efid_poster != efid_user and user_tagged then post_id else null end) as video_tags,
                count(distinct case when post_type = 'video' and efid_poster != efid_wall and user_likes then post_id else null end) as video_likes,
                count(distinct case when post_type = 'video' and efid_poster != efid_wall and user_commented then post_id else null end) as video_comms,
                count(distinct case when post_type = 'video' and efid_poster != efid_wall and efid_poster = efid_user and user_tagged then post_id else null end) as videos_target,
                count(distinct case when post_type = 'photo' and efid_poster = efid_wall and user_tagged then post_id else null end) as uploaded_photo_tags,
                count(distinct case when post_type = 'photo' and efid_poster = efid_wall and user_likes then post_id else null end) as uploaded_photo_likes,
                count(distinct case when post_type = 'photo' and efid_poster = efid_wall and user_commented then post_id else null end) as uploaded_photo_comms,
                count(distinct case when post_type = 'video' and efid_poster = efid_wall and user_tagged then post_id else null end) as uploaded_video_tags,
                count(distinct case when post_type = 'video' and efid_poster = efid_wall and user_likes then post_id else null end) as uploaded_video_likes,
                count(distinct case when post_type = 'video' and efid_poster = efid_wall and user_commented then post_id else null end) as uploaded_video_comms,
                count(distinct case when post_type = 'status' and user_tagged then post_id else null end) as stat_tags,
                count(distinct case when post_type = 'status' and user_likes then post_id else null end) as stat_likes,
                count(distinct case when post_type = 'status' and user_commented then post_id else null end) as stat_comms,
                count(distinct case when post_type = 'link' and user_tagged then post_id else null end) as link_tags,
                count(distinct case when post_type = 'link' and user_likes then post_id else null end) as link_likes,
                count(distinct case when post_type = 'link' and user_commented then post_id else null end) as link_comms,
                count(distinct case when user_placed then post_id else null end) as place_tags
            from {user_posts}
            where {user_posts}.efid_wall in {affected_efid_subquery}
            group by 1, 2
        """
        bindings = {
            'user_posts': fbsync.USER_POST_AGGREGATES_TABLE,
        }

        fbsync.upsert(
            run_id,
            fbsync.EDGES_TABLE,
            'efid_primary',
            sql,
            bindings,
            logger=logger
        )
    except Exception, exc:
        logger.exception("edge computation for run_id %s failed due to \"%s\"", run_id, exc)
        raise


@app.task
def compute_post_aggregates(run_id):
    try:
        sql = """
            select
                {posts}.post_id,
                {posts}.efid,
                count(distinct {likes}.liker_efid) as num_likes,
                count(distinct {tagged}.tagged_efid) as num_tags,
                count(distinct {comments}.comment_id) as num_comments,
                count(distinct {comments}.commenter_id) as num_users_commented
            from
                {posts}
                left join {likes} on ({likes}.post_id = {posts}.post_id)
                left join {comments} on ({comments}.post_id = {posts}.post_id)
                left join {tagged} on ({tagged}.post_id = {posts}.post_id)
                where {posts}.efid in {affected_efid_subquery}
                GROUP BY 1, 2
        """
        bindings = {
            'posts': fbsync.POSTS_TABLE,
            'likes': fbsync.POST_LIKES_TABLE,
            'comments': fbsync.POST_COMMENTS_TABLE,
            'tagged': fbsync.POST_TAGS_TABLE,
        }
        fbsync.upsert(
            run_id,
            fbsync.POST_AGGREGATES_TABLE,
            'efid',
            sql,
            bindings,
            logger=logger
        )
    except Exception, exc:
        logger.exception("post aggregation for run_id %s failed due to \"%s\"", run_id, exc)
        raise


@app.task
def compute_user_post_aggregates(run_id):
    try:
        allids = "allids_{}".format(run_id)
        sql = """
            select
                {allids}.efid as efid_user,
                {posts}.post_id,
                max({posts}.efid) as efid_wall,
                max({posts}.post_from) as efid_poster,
                max({posts}.post_type) as post_type,
                bool_or({tagged}.tagged_efid is not null) as user_tagged,
                bool_or({likes}.liker_efid is not null) as user_likes,
                bool_or({comments}.commenter_id is not null) as user_commented,
                count(distinct {comments}.comment_id) as num_comments,
                bool_or({locales}.tagged_efid is not null) as user_placed
            from
                (
                    select distinct tagged_efid as efid, post_id from v2_post_tags
                    union
                    select distinct liker_efid as efid, post_id from v2_post_likes
                    union
                    select distinct commenter_id as efid, post_id from v2_post_comments
                    union
                    select distinct tagged_efid as efid, post_id from v2_user_locales
                ) {allids}
                join {posts} on ({allids}.post_id = {posts}.post_id)
                left join {likes} on ({allids}.efid = {likes}.liker_efid and {likes}.post_id = {posts}.post_id)
                left join {comments} on ({allids}.efid = {comments}.commenter_id and {comments}.post_id = {posts}.post_id)
                left join {tagged} on ({allids}.efid = {tagged}.tagged_efid and {tagged}.post_id = {posts}.post_id)
                left join {locales} on ({allids}.efid = {locales}.tagged_efid and {locales}.post_id = {posts}.post_id)
            where {posts}.efid in {affected_efid_subquery}
            group by 1, 2
        """
        bindings = {
            'posts': fbsync.POSTS_TABLE,
            'likes': fbsync.POST_LIKES_TABLE,
            'comments': fbsync.POST_COMMENTS_TABLE,
            'tagged': fbsync.POST_TAGS_TABLE,
            'locales': fbsync.USER_LOCALES_TABLE,
            'allids': allids,
        }

        fbsync.upsert(
            run_id,
            fbsync.USER_POST_AGGREGATES_TABLE,
            'efid_wall',
            sql,
            bindings,
            logger=logger
        )

        callback = compute_user_aggregates.s(run_id)
        celery.chord([
            compute_user_timeline_aggregates.s(run_id),
            compute_poster_aggregates.s(run_id),
            compute_edges.s(run_id),
        ])(callback)
    except Exception, exc:
        logger.exception("user_post aggregation for run_id %s failed due to \"%s\"", run_id, exc)
        raise


@app.task
def compute_user_timeline_aggregates(run_id):
    try:
        sql = """
            select
                efid,
                date(min(post_ts)) as first_activity,
                date(max(post_ts)) as last_activity,
                count(distinct case when post_type = 'statuses' then post_id else null end) as num_stat_upd
            from {posts}
            where efid in {affected_efid_subquery}
            group by 1
        """
        bindings = {
            'posts': fbsync.POSTS_TABLE,
        }

        fbsync.upsert(
            run_id,
            fbsync.USER_TIMELINE_AGGREGATES_TABLE,
            'efid',
            sql,
            bindings,
            logger=logger
        )
    except Exception, exc:
        logger.exception("user_timeline aggregation for run_id %s failed due to \"%s\"", run_id, exc)
        raise


@app.task
def compute_poster_aggregates(run_id):
    try:
        sql = """
            select
                efid_poster,
                count(distinct {posts}.post_id) as num_posts,
                count(distinct case when user_likes then {user_posts}.post_id else null end) as num_mine_liked,
                count(distinct case when user_likes then efid_user else null end) as num_friends_liked,
                count(distinct case when user_commented then {user_posts}.post_id else null end) as num_mine_commented_on,
                count(distinct case when user_commented then efid_user else null end) as num_friends_commented,
                count(distinct case when user_tagged then {user_posts}.post_id else null end) as num_i_shared,
                count(distinct case when user_tagged then efid_user else null end) as num_friends_i_tagged,
                count(distinct efid_user) as num_friends_interacted_with_my_posts,
                count(distinct {user_posts}.post_id) as num_posts_with_edges
            from {posts}
            join {user_posts} on ({user_posts}.efid_wall = {posts}.efid)
            where efid_poster in {affected_efid_subquery}
            group by 1
        """
        bindings = {
            'posts': fbsync.POSTS_TABLE,
            'user_posts': fbsync.USER_POST_AGGREGATES_TABLE,
        }

        fbsync.upsert(
            run_id,
            fbsync.POSTER_AGGREGATES_TABLE,
            'efid_poster',
            sql,
            bindings,
            logger=logger
        )
    except Exception, exc:
        logger.exception("poster aggregation for run_id %s failed due to \"%s\"", run_id, exc)
        raise


@app.task
def compute_user_aggregates(totals, run_id):
    try:
        sql = """select
            *,
            case when num_posts > 0 then (last_activity - first_activity) / num_posts else NULL end as avg_days_between_activity,
            case when num_posts > 0 then round(num_post_interactors / num_posts::decimal, 4) else NULL end as avg_post_interactors
        from (
            select
                u.efid,
                max({datediff_expression}) as age,
                max(first_activity) as first_activity,
                max(last_activity) as last_activity,
                count({taggable_friends}.friend_name) as num_taggable_friends,
                count(distinct {edges}.efid_secondary) as num_person_edges,
                max(num_posts) as num_posts,
                max(num_posts_with_edges) as num_posts_with_edges,
                max(num_mine_liked) as num_posts_liked,
                max(num_mine_commented_on) as num_posts_commented_on,
                max(num_i_shared) as num_posts_shared,
                max(num_stat_upd) as num_stat_updates,
                max(num_friends_interacted_with_my_posts) as num_post_interactors
            from {users} u
            left join {edges} on (u.efid = {edges}.efid_primary)
            left join {post_aggregates} on (u.efid = {post_aggregates}.efid)
            left join {poster_aggregates} me_as_poster on (u.efid = me_as_poster.efid_poster)
            left join {timeline_aggregates} on (u.efid = {timeline_aggregates}.efid)
            left join {taggable_friends} on (u.efid = {taggable_friends}.efid)
            where u.efid in {affected_efid_subquery}
            group by u.efid
        ) sums
        """
        bindings = {
            'users': fbsync.USERS_TABLE,
            'edges': fbsync.EDGES_TABLE,
            'post_aggregates': fbsync.POST_AGGREGATES_TABLE,
            'poster_aggregates': fbsync.POSTER_AGGREGATES_TABLE,
            'timeline_aggregates': fbsync.USER_TIMELINE_AGGREGATES_TABLE,
            'taggable_friends': fbsync.USER_FRIENDS_TABLE,
            'datediff_expression': fbsync.datediff_expression(),
        }
        fbsync.upsert(
            run_id,
            fbsync.USER_AGGREGATES_TABLE,
            'efid',
            sql,
            bindings,
            logger=logger
        )

        with redshift_engine.connect() as connection:
            dbutils.drop_table_if_exists(
                fbsync.affected_efids(run_id),
                connection
            )

    except Exception, exc:
        logger.exception("user aggregation for run_id %s failed due to \"%s\"", run_id, exc)
        raise

    cache_tables.delay(run_id)


@app.task
def cache_tables(run_id):
    try:
        tables_to_sync = (
            (fbsync.USERS_TABLE, 'efid', None),
            (fbsync.USER_AGGREGATES_TABLE, 'efid', None),
            (fbsync.USER_ACTIVITIES_TABLE, None, ('efid',)),
            (fbsync.USER_LIKES_TABLE, None, ('efid',)),
            (fbsync.USER_PERMISSIONS_TABLE, None, ('efid',)),
            (fbsync.USER_LOCALES_TABLE, None, ('tagged_efid',)),
            (fbsync.USER_LANGUAGES_TABLE, None, ('efid',)),
        )
        for aggregate_table, primary_key, indexed_columns in tables_to_sync:
            dbutils.cache_table(
                redshift_engine,
                rds_cache_engine,
                '{}_staging'.format(aggregate_table),
                aggregate_table,
                '{}_old'.format(aggregate_table),
                ',',
                primary_key=primary_key,
                indexed_columns=indexed_columns,
            )

        batch_push.delay(run_id)
    except Exception, exc:
        logger.exception("Table caching failed due to \"%s\"", exc)


@app.task
def batch_push(run_id):
    try:
        db_session = RDSCacheSession()
        query = db_session.query(FBSyncRunList).\
            filter_by(run_id=str(run_id)).\
            values(FBSyncRunList.efid)

        queue = Queue('capuchin_efid_batch', Exchange('capuchin'), 'capuchin.efid_batch')
        with app.producer_or_acquire(None) as producer:
            for batch in batcher(query, 100):
                batch = batch[0]
                logger.info("Sending batch %s", batch)
                producer.publish(
                    batch,
                    exchange=queue.exchange,
                    routing_key=queue.routing_key,
                    declare=[queue],
                    serializer='json',
                )
    except Exception, exc:
        logger.exception("Batch push failed due to \"%s\"", exc)
