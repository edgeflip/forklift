import datetime
from collections import defaultdict
from cStringIO import StringIO
import forklift.db.utils as dbutils
import httplib
import json
import logging
import socket
import ssl
import tempfile
import time
import unicodecsv
from urlparse import urlparse

from forklift.s3.utils import write_string_to_key

DB_TEXT_LEN = 4096
MAX_RECORDS_TO_DEDUPE = 100000000
UNIQUE_POST_ID_TABLE = 'fbid_post_ids'
POSTS_TABLE = 'posts_pipeline_test'
USER_POSTS_TABLE = 'user_posts_pipeline_test'
LIKES_TABLE = 'page_likes_pipeline_test'
TOP_WORDS_TABLE = 'top_words_pipeline_test'
POST_AGGREGATES_TABLE = 'post_aggregates_pipeline_test'
INTERACTOR_AGGREGATES_TABLE = 'user_interactor_aggregates_pipeline_test'
POSTER_AGGREGATES_TABLE = 'user_poster_aggregates_pipeline_test'
USER_AGGREGATES_TABLE = 'user_aggregates_pipeline_test'
AFFECTED_TABLES = (
    POSTS_TABLE,
    USER_POSTS_TABLE,
    LIKES_TABLE,
    TOP_WORDS_TABLE,
    POST_AGGREGATES_TABLE,
    INTERACTOR_AGGREGATES_TABLE,
    POSTER_AGGREGATES_TABLE,
    USER_AGGREGATES_TABLE,
)
USERS_TABLE = 'users'
EDGES_TABLE = 'edges'
USER_CLIENTS_TABLE = 'user_clients'
TOP_WORDS_COUNT = 20
DEFAULT_DELIMITER = "\t"
POSTS = 'posts'
LINKS = 'links'
LIKES = 'likes'
TOP_WORDS = 'top_words'
ENTITIES = (POSTS, LINKS, LIKES, TOP_WORDS)


logger = logging.getLogger(__name__)


def incremental_table_name(table_base, version):
    return "{}_{}".format(table_base, version)


def raw_table_name(table_base):
    return "{}_raw".format(table_base)


def old_table_name(table_base):
    return "{}_old".format(table_base)


def create_sql(table_name, version=None):
    if(
        table_name == raw_table_name(POSTS_TABLE) or
        table_name == raw_table_name(incremental_table_name(POSTS_TABLE, version))
    ):
        return """
            CREATE TABLE {table} (
                fbid_user BIGINT NOT NULL,
                fbid_post VARCHAR(64) NOT NULL,
                ts TIMESTAMP NOT NULL,
                type VARCHAR(64) NOT NULL,
                app VARCHAR(256),
                post_from VARCHAR(256),
                link VARCHAR(2048),
                domain VARCHAR(1024),
                story VARCHAR({text_len}),
                description VARCHAR({text_len}),
                caption VARCHAR({text_len}),
                message VARCHAR({text_len}),
                num_likes INT,
                num_comments INT,
                num_shares INT,
                num_users_commented INT
            )
        """.format(table=table_name, text_len=DB_TEXT_LEN)
    elif (
        table_name == raw_table_name(USER_POSTS_TABLE) or
        table_name == raw_table_name(incremental_table_name(USER_POSTS_TABLE, version))
    ):
        return """
            CREATE TABLE {table} (
                fbid_post VARCHAR(64) NOT NULL,
                fbid_user BIGINT NOT NULL,
                fbid_poster BIGINT NOT NULL,
                user_to BOOLEAN,
                user_like BOOLEAN,
                user_comment BOOLEAN,
                num_comments INT,
                comment_text VARCHAR({text_len})
            )
        """.format(table=table_name, text_len=DB_TEXT_LEN)
    elif (
        table_name == raw_table_name(LIKES_TABLE) or
        table_name == raw_table_name(incremental_table_name(LIKES_TABLE, version))
    ):
        return """
            CREATE TABLE {table} (
                fbid BIGINT NOT NULL,
                page_id BIGINT NOT NULL
            )
        """.format(table=table_name)
    elif (
        table_name == raw_table_name(TOP_WORDS_TABLE) or
        table_name == raw_table_name(incremental_table_name(TOP_WORDS_TABLE, version))
    ):
        return """
            CREATE TABLE {table} (
                fbid BIGINT NOT NULL,
                top_words VARCHAR(4096)
            )
        """.format(table=table_name)
    elif table_name == POST_AGGREGATES_TABLE:
        return """
            CREATE TABLE {table} (
                fbid BIGINT NOT NULL,
                first_activity DATE,
                last_activity DATE,
                num_stat_upd INT
            )
        """.format(table=table_name)
    elif table_name == INTERACTOR_AGGREGATES_TABLE:
        return """
            CREATE TABLE {table} (
                fbid_user BIGINT NOT NULL,
                num_posts_interacted_with INT,
                num_i_like INT,
                num_i_comm INT,
                num_shared_w_me INT,
                num_friends_i_interacted_with INT
            )
        """.format(table=table_name)
    elif table_name == POSTER_AGGREGATES_TABLE:
        return """
            CREATE TABLE {table} (
                fbid_poster BIGINT NOT NULL,
                num_posts INT,
                num_mine_liked INT,
                num_mine_commented INT,
                num_i_shared_with INT,
                num_friends_interacted_with_my_posts INT
            )
        """.format(table=table_name)
    elif table_name == USER_AGGREGATES_TABLE:
        return """
            CREATE TABLE {table} (
                is_primary BOOLEAN,
                fbid BIGINT NOT NULL,
                age INT,
                first_activity DATE,
                last_activity DATE,
                num_friends INT,
                num_posts INT,
                num_posts_interacted_with INT,
                num_i_like INT,
                num_i_comm INT,
                num_shared_w_me INT,
                num_mine_liked INT,
                num_mine_commented INT,
                num_i_shared_with INT,
                num_stat_upd INT,
                num_friends_interacted_with_my_posts INT,
                num_friends_i_interacted_with INT,
                top_words VARCHAR(4096),
                avg_time_between_activity INT,
                avg_friends_interacted_with_my_posts INT,
                avg_friends_i_interacted_with INT
            )
        """.format(table=table_name)
    elif table_name == USERS_TABLE:
        return """
            CREATE TABLE {table} (
                fbid BIGINT NOT NULL,
                birthday DATE,
                fname varchar(256),
                lname varchar(256),
                email varchar(256),
                gender varchar(256),
                city varchar(256),
                state varchar(256),
                country varchar(256),
                activities varchar({text_len}),
                affiliations varchar({text_len}),
                books varchar({text_len}),
                devices varchar(256),
                friend_request_count INT,
                has_timeline varchar(256),
                interests varchar({text_len}),
                languages varchar({text_len}),
                likes_count INT,
                movies varchar({text_len}),
                music varchar({text_len}),
                political varchar(1024),
                profile_update_time TIMESTAMP,
                quotes varchar({text_len}),
                relationship_status varchar(256),
                religion varchar(256),
                sports varchar({text_len}),
                tv varchar({text_len}),
                wall_count INT,
                updated TIMESTAMP
            )
        """.format(table=table_name, text_len=DB_TEXT_LEN)
    elif table_name == EDGES_TABLE:
        return """
            CREATE TABLE {table} (
                fbid_source bigint,
                fbid_target bigint,
                post_likes integer,
                post_comms integer,
                stat_likes integer,
                stat_comms integer,
                wall_posts integer,
                wall_comms integer,
                tags integer,
                photos_target integer,
                photos_other integer,
                mut_friends integer,
                updated timestamp
            )
        """.format(table=table_name)
    elif table_name == USER_CLIENTS_TABLE:
        return """
            CREATE TABLE {table} (
                fbid bigint,
                client_id int
            )
        """.format(table=table_name)
    else:
        raise ValueError('Table {} not recognized'.format(table_name))


def dedupe(table_with_dupes, staging_table_name, version, engine):
    sql = dedupe_sql(staging_table_name, version).format(
        new_table=staging_table_name,
        raw_table=table_with_dupes
    )
    logger.info(
        'Deduping records from %s into staging table %s',
        table_with_dupes,
        staging_table_name
    )
    logger.info(sql)
    with engine.connect() as connection:
        connection.execute(sql)
    logger.info(
        '%s records after deduping',
        dbutils.get_rowcount(staging_table_name, engine=engine)
    )


def dedupe_sql(base_table_name, version):
    if (
        base_table_name == POSTS_TABLE or
        base_table_name == incremental_table_name(POSTS_TABLE, version)
    ):
        return """
            create table {new_table} as
            select
                max(fbid_user) as fbid_user,
                fbid_post,
                max(ts) as ts,
                max(type) as type,
                max(app) as app,
                max(post_from) as post_from,
                max(link) as link,
                max(domain) as domain,
                max(story) as story,
                max(description) as description,
                max(caption) as caption,
                max(message) as message,
                max(num_likes) as num_likes,
                max(num_comments) as num_comments,
                max(num_shares) as num_shares,
                max(num_users_commented) as num_users_commented
            from {raw_table}
            group by fbid_post
        """
    elif (
        base_table_name == USER_POSTS_TABLE or
        base_table_name == incremental_table_name(USER_POSTS_TABLE, version)
    ):
        return """
            create table {new_table} as
            select
                fbid_post,
                fbid_user,
                max(fbid_poster) as fbid_poster,
                bool_or(user_to) as user_to,
                bool_or(user_like) as user_like,
                bool_or(user_comment) as user_comment,
                max(num_comments) as num_comments,
                max(comment_text) as comment_text
            from {raw_table}
            group by fbid_post, fbid_user
        """
    elif (
        base_table_name == LIKES_TABLE or
        base_table_name == incremental_table_name(LIKES_TABLE, version)
    ):
        return """
            create table {new_table} as
            select
                fbid,
                page_id
            from {raw_table}
            group by
                fbid,
                page_id
        """
    elif (
        base_table_name == TOP_WORDS_TABLE or
        base_table_name == incremental_table_name(TOP_WORDS_TABLE, version)
    ):
        return """
            create table {new_table} as
            select fbid, max(top_words) as top_words
            from {raw_table}
            group by fbid
        """
    else:
        raise ValueError('Table {} not recognized'.format(base_table_name))


# when FBSync just grabbed some new data and we want to merge it in
def add_new_data(bucket_name, common_prefix, version, posts_folder, user_posts_folder, likes_folder, top_words_folder, engine):
    posts_incremental = incremental_table_name(POSTS_TABLE, version)
    user_posts_incremental = incremental_table_name(USER_POSTS_TABLE, version)
    likes_incremental = incremental_table_name(LIKES_TABLE, version)
    top_words_incremental = incremental_table_name(TOP_WORDS_TABLE, version)
    incremental_tables = (
        posts_incremental,
        user_posts_incremental,
        likes_incremental,
        top_words_incremental
    )
    with engine.connect() as connection:
        for incremental_table in incremental_tables:
            dbutils.drop_table_if_exists(incremental_table, connection)

    load(
        bucket_name,
        common_prefix,
        version,
        posts_folder,
        raw_table_name(posts_incremental),
        engine
    )
    load(
        bucket_name,
        common_prefix,
        version,
        user_posts_folder,
        raw_table_name(user_posts_incremental),
        engine
    )
    load(
        bucket_name,
        common_prefix,
        version,
        likes_folder,
        raw_table_name(likes_incremental),
        engine
    )
    load(
        bucket_name,
        common_prefix,
        version,
        top_words_folder,
        raw_table_name(top_words_incremental),
        engine
    )

    merge_in_smaller_batches(
        raw_table_name(posts_incremental),
        posts_incremental,
        POSTS_TABLE,
        find_new_posts,
        merge_posts,
        version,
        engine
    )
    merge_in_smaller_batches(
        raw_table_name(user_posts_incremental),
        user_posts_incremental,
        USER_POSTS_TABLE,
        find_new_user_posts,
        merge_user_posts,
        version,
        engine
    )
    merge_in_smaller_batches(
        raw_table_name(likes_incremental),
        likes_incremental,
        LIKES_TABLE,
        find_new_likes,
        merge_likes,
        version,
        engine
    )
    merge_in_smaller_batches(
        raw_table_name(top_words_incremental),
        top_words_incremental,
        TOP_WORDS_TABLE,
        find_new_top_words,
        merge_top_words,
        version,
        engine
    )

    # get posts and related aggregates
    updated_posts_table = calculate_users_with_new_posts(
        raw_table_name(posts_incremental),
        engine
    )
    merge_post_aggregates(updated_posts_table, POSTS_TABLE, POST_AGGREGATES_TABLE, engine)
    # post interactions and related aggregates
    outbound_users_table = calculate_users_with_outbound_interactions(
        raw_table_name(user_posts_incremental),
        engine
    )

    inbound_users_table = calculate_users_with_inbound_interactions(
        raw_table_name(user_posts_incremental),
        engine
    )
    merge_interactor_aggregates(outbound_users_table, USER_POSTS_TABLE, INTERACTOR_AGGREGATES_TABLE, engine)
    merge_poster_aggregates(inbound_users_table, USER_POSTS_TABLE, POSTER_AGGREGATES_TABLE, engine)

    merge_user_aggregates(
        updated_posts_table,
        inbound_users_table,
        outbound_users_table,
        USER_AGGREGATES_TABLE,
        engine
    )

    for table in AFFECTED_TABLES:
        dbutils.optimize(table, logger, engine)

    for incremental_table in incremental_tables:
        dbutils.drop_table_if_exists(incremental_table, engine)

def merge_in_smaller_batches(
    raw_table,
    incremental_table,
    final_table,
    finder_routine,
    merger_routine,
    version,
    engine
):

    with engine.connect() as connection:
        new_table = finder_routine(
            raw_table,
            final_table,
            connection
        )
        unmerged_row_count = dbutils.get_rowcount(new_table, engine=engine)
        while unmerged_row_count > 0:
            logger.info("Remaining unmerged rows: {}".format(unmerged_row_count))
            dedupe(
                new_table,
                incremental_table,
                version,
                connection
            )
            merger_routine(incremental_table, final_table, connection)
            dbutils.drop_table(incremental_table, connection)
            dbutils.drop_table(new_table, connection)
            new_table = finder_routine(
                raw_table,
                final_table,
                connection
            )
            unmerged_row_count = dbutils.get_rowcount(new_table, engine=engine)

def load(bucket_name, common_prefix, version, source_folder, raw_table, engine):
    path = '/'.join((common_prefix, version, source_folder))
    logger.info(
        'Loading raw data into %s from s3://%s/%s',
        raw_table,
        bucket_name,
        path
    )
    with engine.connect() as connection:
        dbutils.load_from_s3(
            connection,
            bucket_name,
            path,
            raw_table,
            create_statement=create_sql(raw_table, version)
        )
    logger.info(
        '%s rows loaded into %s',
        dbutils.get_rowcount(raw_table, engine=engine),
        raw_table
    )


def calculate_users_with_new_posts(
    posts_incremental_table,
    engine
):
    temp_table = posts_incremental_table + '_updated'
    dbutils.drop_table_if_exists(temp_table, engine)
    logger.info(
        'Populating list of users with updated posts from %s',
        posts_incremental_table
    )
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE {temp_table} AS
            SELECT distinct fbid_user as fbid
            FROM {incremental_table}
        """.format(
            temp_table=temp_table,
            incremental_table=posts_incremental_table
        ))

    logger.info(
        '%s updated users found',
        dbutils.get_rowcount(temp_table, engine=engine)
    )
    return temp_table


def calculate_users_with_outbound_interactions(
    user_posts_incremental_table,
    engine
):
    temp_table = user_posts_incremental_table + '_outbound_updated'
    dbutils.drop_table_if_exists(temp_table, engine)
    logger.info(
        'Populating list of users with updated post interactions from %s',
        user_posts_incremental_table
    )
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE {temp_table} AS
            SELECT distinct fbid_user as fbid_user
            FROM {incremental_table}
        """.format(
            temp_table=temp_table,
            incremental_table=user_posts_incremental_table
        ))

    logger.info(
        '%s updated users found',
        dbutils.get_rowcount(temp_table, engine=engine)
    )
    return temp_table


def calculate_users_with_inbound_interactions(
    user_posts_incremental_table,
    engine
):
    connection = engine.connect()
    temp_table = user_posts_incremental_table + '_inbound_updated'
    dbutils.drop_table_if_exists(temp_table, connection)
    # 1. get list of user ids with new data
    logger.info(
        'Populating list of posters with updated post interactions from %s',
        user_posts_incremental_table
    )
    with connection.begin():
        connection.execute("""
            CREATE TABLE {temp_table} AS
            SELECT distinct fbid_poster as fbid_poster
            FROM {incremental_table}
        """.format(
            temp_table=temp_table,
            incremental_table=user_posts_incremental_table
        ))

    logger.info(
        '%s updated users found',
        dbutils.get_rowcount(temp_table, engine=engine)
    )

    return temp_table

def find_new_posts(raw_table, final_table, engine):
    temp_table = raw_table + '_new'
    dbutils.drop_table_if_exists(temp_table, engine)
    # populate list of new post ids
    logger.info(
        'Populating list of new posts from %s compared with records in %s',
        raw_table,
        final_table
    )
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE {temp_table} AS
            SELECT {raw_table}.*
            FROM {raw_table}
            LEFT JOIN {final_table} USING (fbid_post)
            WHERE {final_table}.fbid_post is NULL
            LIMIT {limit}
        """.format(
            temp_table=temp_table,
            raw_table=raw_table,
            final_table=final_table,
            limit=MAX_RECORDS_TO_DEDUPE,
        ))

    logger.info(
        '%s new posts found',
        dbutils.get_rowcount(temp_table, engine=engine)
    )
    return temp_table


def find_new_user_posts(incremental_table, final_table, engine):
    temp_table = incremental_table + '_new'
    # populate list of new post ids
    dbutils.drop_table_if_exists(temp_table, engine)
    logger.info(
        'Populating list of new records from %s compared with records in %s',
        incremental_table,
        final_table
    )
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE {temp_table} AS
            SELECT {incremental_table}.*
            FROM {incremental_table}
            LEFT JOIN {final_table} USING (fbid_post, fbid_user)
            WHERE {final_table}.fbid_post is NULL
        """.format(
            temp_table=temp_table,
            incremental_table=incremental_table,
            final_table=final_table
        ))

    logger.info(
        '%s new records found out of %s in total',
        dbutils.get_rowcount(temp_table, engine=engine),
        dbutils.get_rowcount(incremental_table, engine=engine),
    )
    return temp_table


def find_new_likes(incremental_table, final_table, engine):
    temp_table = incremental_table + '_new'
    dbutils.drop_table_if_exists(temp_table, engine)
    logger.info(
        'Populating list of new records from %s compared with %s',
        incremental_table, final_table
    )
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE {temp_table} AS
            SELECT distinct fbid, page_id
            FROM {incremental_table}
            LEFT JOIN {final_table} USING (fbid, page_id)
            WHERE {final_table}.fbid is NULL
        """.format(
            temp_table=temp_table,
            incremental_table=incremental_table,
            final_table=final_table
        ))

    logger.info(
        '%s new records found',
        dbutils.get_rowcount(temp_table, engine=engine)
    )
    return temp_table

def find_new_top_words(incremental_table, final_table, engine):
    temp_table = incremental_table + '_new'
    dbutils.drop_table_if_exists(temp_table, engine)
    logger.info(
        'Populating list of new top words from %s compared with %s',
        incremental_table, final_table
    )
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE {temp_table} AS
            SELECT fbid, max({incremental_table}.top_words) as top_words
            FROM {incremental_table}
            LEFT JOIN {final_table} USING (fbid)
            WHERE {final_table}.fbid is NULL
            GROUP BY 1
        """.format(
            temp_table=temp_table,
            incremental_table=incremental_table,
            final_table=final_table
        ))

    logger.info(
        '%s new top_words found',
        dbutils.get_rowcount(temp_table, engine=engine)
    )
    return temp_table

# take deduped new posts from 'incremental_table' and merge them into 'final_table'
def merge_posts(deduped_table, final_table, connection):
    logger.info(
        'Inserting new rows from %s into %s',
        deduped_table,
        final_table
    )
    connection.execute("""
        INSERT INTO {final_table}
        SELECT {deduped_table}.*
        FROM {deduped_table}
    """.format(
        final_table=final_table,
        deduped_table=deduped_table,
    ))

# take deduped new user_posts from 'incremental_table' and merge them into 'final_table'
def merge_user_posts(deduped_table, final_table, connection):
    # insert new versions into final table
    logger.info(
        'Inserting new rows from %s into %s',
        deduped_table,
        final_table
    )
    connection.execute("""
        INSERT INTO {final_table}
        SELECT {deduped_table}.*
        FROM {deduped_table}
    """.format(
        final_table=final_table,
        deduped_table=deduped_table,
    ))

def merge_likes(deduped_table, final_table, connection):
    # insert new versions into final table
    logger.info(
        'Inserting new rows from %s into %s',
        deduped_table,
        final_table
    )
    connection.execute("""
        INSERT INTO {final_table} (fbid, page_id)
        SELECT fbid, page_id
        FROM {deduped_table}
    """.format(
        final_table=final_table,
        deduped_table=deduped_table,
    ))


def merge_top_words(deduped_table, final_table, connection):
    # insert new versions into final table
    logger.info(
        'Inserting new rows from %s into %s',
        deduped_table,
        final_table
    )
    connection.execute("""
        INSERT INTO {final_table} (fbid, top_words)
        SELECT fbid, top_words
        FROM {deduped_table}
    """.format(
        final_table=final_table,
        deduped_table=deduped_table,
    ))


def merge_post_aggregates(
    updated_users_table,
    posts_full_table,
    final_aggregate_table,
    connection
):
    newconn = connection.connect()
    with newconn.begin():
        # 2. delete from final table with those userids
        newconn.execute("""
            DELETE
            FROM {final_table}
            WHERE fbid in (select distinct fbid from {temp_table})
        """.format(
            final_table=final_aggregate_table,
            temp_table=updated_users_table
        ))

        # 3. find posts that are in temp and insert into final
        newconn.execute("""
            INSERT INTO {final_table}
            SELECT
                fbid_user as fbid,
                date(min(ts)) as first_activity,
                date(max(ts)) as last_activity,
                count(distinct case when type = 'status' then fbid_post else null end) as num_stat_upd
            FROM {temp_table}
            JOIN {full_table} on ({full_table}.fbid_user = {temp_table}.fbid)
            GROUP BY 1
        """.format(
            final_table=final_aggregate_table,
            temp_table=updated_users_table,
            full_table=posts_full_table
        ))


def merge_interactor_aggregates(
    updated_users_table,
    user_posts_full_table,
    final_aggregate_table,
    connection
):
    newconn = connection.connect()
    with newconn.begin():
        # delete from final table with changed userids
        newconn.execute("""
            DELETE
            FROM {final_table}
            WHERE fbid_user in (select distinct fbid_user from {temp_table})
        """.format(
            final_table=final_aggregate_table,
            temp_table=updated_users_table
        ))

        # find posts that are in temp and insert into final
        newconn.execute("""
            INSERT INTO {final_table}
            SELECT
                {temp_table}.fbid_user,
                count(distinct fbid_post) as num_posts_interacted_with,
                count(distinct case when user_like then fbid_post else null end) as num_i_like,
                count(distinct case when user_comment then fbid_post else null end) as num_i_comm,
                count(distinct case when user_to then fbid_post else null end) as num_shared_w_me,
                count(distinct fbid_poster) as num_friends_i_interacted_with
            FROM {temp_table}
            JOIN {full_table} on ({full_table}.fbid_user = {temp_table}.fbid_user)
            GROUP BY 1
        """.format(
            final_table=final_aggregate_table,
            temp_table=updated_users_table,
            full_table=user_posts_full_table
        ))


def merge_poster_aggregates(
    updated_users_table,
    user_posts_full_table,
    final_aggregate_table,
    connection
):
    newconn = connection.connect()
    with newconn.begin():
        # delete from final table with changed userids
        newconn.execute("""
            DELETE
            FROM {final_table}
            WHERE fbid_poster in (select distinct fbid_poster from {temp_table})
        """.format(
            final_table=final_aggregate_table,
            temp_table=updated_users_table
        ))

        # find posts that are in temp and insert into final
        newconn.execute("""
            INSERT INTO {final_table}
            SELECT
                {temp_table}.fbid_poster,
                count(distinct fbid_post) num_posts,
                count(distinct case when user_like then fbid_post else null end) as num_mine_liked,
                count(distinct case when user_comment then fbid_post else null end) as num_mine_commented,
                count(distinct case when user_to then fbid_post else null end) as num_i_shared_with,
                count(distinct fbid_user) as num_friends_interacted_with_my_posts
            FROM {temp_table}
            JOIN {full_table} on ({full_table}.fbid_poster = {temp_table}.fbid_poster)
            GROUP BY 1
        """.format(
            final_table=final_aggregate_table,
            temp_table=updated_users_table,
            full_table=user_posts_full_table
        ))



# functionified as a hack to get around differences between testing (stock
# postgres) and production (redshift postgres) syntaxes
def datediff_expression():
    return "datediff('year', birthday, getdate())"


def merge_user_aggregates(
    updated_post_users_table,
    updated_inbound_interactions_table,
    updated_outbound_interactions_table,
    final_aggregate_table,
    engine
):
    temp_table = final_aggregate_table + '_updated'
    logger.info('Finding users who had updated aggregates this batch')
    with engine.connect() as connection:
        connection.execute("""
            CREATE TEMPORARY TABLE {temp_table} AS
            select distinct fbid from (
                select distinct fbid as fbid from {posts}
                union select distinct fbid_user as fbid from {outbound}
                union select distinct fbid_poster as fbid from {inbound}
            ) sources
        """.format(
            temp_table=temp_table,
            posts=updated_post_users_table,
            outbound=updated_outbound_interactions_table,
            inbound=updated_inbound_interactions_table
        ))

        logger.info(
            '%s updated users found',
            dbutils.get_rowcount(temp_table, connection=connection)
        )

        # delete from final table with changed userids
        connection.execute("""
            DELETE
            FROM {final_table}
            WHERE fbid in (select distinct fbid from {temp_table})
        """.format(
            final_table=final_aggregate_table,
            temp_table=temp_table
        ))

        # find posts that are in temp and insert into final
        connection.execute("""
            INSERT INTO {final_table}
select
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
        max(num_i_shared_with) as num_i_shared_with,
        max(num_stat_upd) as num_stat_upd,
        max(num_friends_interacted_with_my_posts) as num_friends_interacted_with_my_posts,
        max(num_friends_i_interacted_with) as num_friends_i_interacted_with,
        max(top_words) as top_words
    from {temp_table}
    join {users_table} u using (fbid)
    left join {edges_table} on (u.fbid = {edges_table}.fbid_target)
    left join {post_aggregates_table} on (u.fbid = {post_aggregates_table}.fbid)
    left join {interactor_aggregates_table} me_as_interactor on (u.fbid = me_as_interactor.fbid_user)
    left join {poster_aggregates_table} me_as_poster on (u.fbid = me_as_poster.fbid_poster)
    left join {top_words_table} on (u.fbid = {top_words_table}.fbid)
    left join user_clients on (u.fbid = user_clients.fbid)
    group by u.fbid
) sums
        """.format(
            final_table=final_aggregate_table,
            temp_table=temp_table,
            users_table=USERS_TABLE,
            edges_table=EDGES_TABLE,
            post_aggregates_table=POST_AGGREGATES_TABLE,
            interactor_aggregates_table=INTERACTOR_AGGREGATES_TABLE,
            poster_aggregates_table=POSTER_AGGREGATES_TABLE,
            top_words_table=TOP_WORDS_TABLE,
            datediff_expression=datediff_expression(),
        ))

# Despite what the docs say, datetime.strptime() format doesn't like %z
# see: http://stackoverflow.com/questions/526406/python-time-to-age-part-2-timezones/526450#526450
def parse_ts(time_string):
    tz_offset_hours = int(time_string[-5:]) / 100  # we're ignoring the possibility of minutes here
    tz_delt = datetime.timedelta(hours=tz_offset_hours)
    return datetime.datetime.strptime(time_string[:-5], "%Y-%m-%dT%H:%M:%S") - tz_delt


class FeedPostFromJson(object):
    """Each post contributes a single post line, and multiple user-post lines to the db"""

    def __init__(self, post_json):
        self.post_id = str(post_json['id'])
        # self.post_ts = post_json['updated_time']
        self.post_ts = parse_ts(post_json['updated_time']).strftime("%Y-%m-%d %H:%M:%S")
        self.post_type = post_json['type']
        self.post_app = post_json['application']['id'] if 'application' in post_json else ""
        self.post_from = post_json['from']['id'] if 'from' in post_json else ""
        self.post_link = post_json.get('link', "")
        try:
            self.post_link_domain = urlparse(self.post_link).hostname if (self.post_link) else ""
        except ValueError: # handling invalid Ipv6 address errors
            self.post_link_domain = ""

        self.post_story = post_json.get('story', "")
        self.post_description = post_json.get('description', "")
        self.post_caption = post_json.get('caption', "")
        self.post_message = post_json.get('message', "")

        if 'to' in post_json and 'data' in post_json['to']:
            self.to_ids = set()
            self.to_ids.update([user['id'] for user in post_json['to']['data']])
        if 'likes' in post_json and 'data' in post_json['likes']:
            self.like_ids = set()
            self.like_ids.update([user['id'] for user in post_json['likes']['data']])
        if 'comments' in post_json and 'data' in post_json['comments']:
            self.comments = [{
                'comment_id': comment['id'],
                'commenter_id': comment['from']['id'],
                'message': comment['message']
            } for comment in post_json['comments']['data']]
            self.commenters = defaultdict(list)
            for comment in self.comments:
                self.commenters[comment['commenter_id']].append(comment['message'])


class FeedFromS3(object):
    """Holds an entire feed from a single user crawl"""

    def __init__(self, key):
        self.initialize()
        prim_id, fbid = key.name.split("_")

        feed_json = None
        with tempfile.TemporaryFile() as fp:
            max_retries = 3
            retries = 0
            while retries < max_retries:
                try:
                    key.get_contents_to_file(fp)
                    break
                except ssl.SSLError:
                    retries += 1
            fp.seek(0)
            feed_json = json.load(fp)
        try:
            feed_json_list = feed_json['data']
        except KeyError:
            logger.debug("no data in feed %s" % key.name)
            logger.debug(str(feed_json))
            raise

        logger.debug(
            "\tread feed json with %d posts from %s",
            len(feed_json_list),
            key.name
        )

        self.user_id = fbid
        self.posts = []
        while feed_json_list:
            post_json = feed_json_list.pop()
            try:
                self.posts.append(FeedPostFromJson(post_json))
            except StandardError:
                logger.debug("error parsing: " + str(post_json))
                logger.debug("full feed: " + str(feed_json_list))
                raise

        if 'likes' in feed_json:
            self.page_likes = feed_json['likes']

    def initialize(self):
        self.output_formatter = {
            POSTS: self.post_lines,
            LINKS: self.link_lines,
            LIKES: self.like_lines,
            TOP_WORDS: self.top_word_lines,
        }


    def transform_field(self, field, delim):
        if isinstance(field, basestring):
            return field.replace(delim, " ").replace("\n", " ").replace("\x00", "").encode('utf8', 'ignore')
        else:
            return str(field)

    @property
    def post_corpus(self):
        corpus = ""
        for post in self.posts:
            corpus += post.post_message + " "
        return corpus

    def post_lines(self, delim):
        post_lines = []
        for post in self.posts:
            post_fields = (
                self.user_id,
                post.post_id,
                post.post_ts,
                post.post_type,
                post.post_app,
                post.post_from,
                post.post_link,
                post.post_link_domain,
                post.post_story,
                post.post_description,
                post.post_caption,
                post.post_message,
                len(post.like_ids) if hasattr(post, 'like_ids') else 0,
                len(post.comments) if hasattr(post, 'comments') else 0,
                len(post.to_ids) if hasattr(post, 'to_ids') else 0,
                len(post.commenters) if hasattr(post, 'commenters') else 0,
            )
            post_lines.append(
                tuple(self.transform_field(field, delim) for field in post_fields)
            )
        return post_lines

    def link_lines(self, delim):
        link_lines = []
        for p in self.posts:
            user_ids = None
            if hasattr(p, 'to_ids'):
                user_ids = p.to_ids
            if hasattr(p, 'like_ids'):
                if user_ids:
                    user_ids = user_ids.union(p.like_ids)
                else:
                    user_ids = p.like_ids
            if hasattr(p, 'commenters'):
                commenter_ids = p.commenters.keys()
                if user_ids:
                    user_ids = user_ids.union(commenter_ids)
                else:
                    user_ids = commenter_ids
            if user_ids:
                for user_id in user_ids:
                    has_to = "1" if hasattr(p, 'to_ids') and user_id in p.to_ids else ""
                    has_like = "1" if hasattr(p, 'like_ids') and user_id in p.like_ids else ""
                    if hasattr(p, 'commenters') and user_id in commenter_ids:
                        has_comm = "1"
                        # we're doing bag of words so separating comments
                        # more granularly than this shouldn't matter
                        comment_text = " ".join(p.commenters[user_id])
                        num_comments = str(len(p.commenters[user_id]))
                    else:
                        has_comm = ""
                        comment_text = ""
                        num_comments = "0"

                    link_fields = (
                        p.post_id,
                        user_id,
                        self.user_id,
                        has_to,
                        has_like,
                        has_comm,
                        num_comments,
                        comment_text
                    )
                    link_lines.append(
                        self.transform_field(f, delim) for f in link_fields
                    )
        return link_lines


    def like_lines(self, delim):
        if not hasattr(self, 'page_likes'):
            return ()
        return tuple(
            tuple(str(f) for f in (self.user_id, like))
            for like in self.page_likes
        )

    def top_word_lines(self, delim, vectorizer, k=TOP_WORDS_COUNT):
        tfidf = vectorizer.transform([self.post_corpus])[0].toarray()
        top_k = tfidf.argsort()[0][::-1][:k]
        feature_names = vectorizer.get_feature_names()
        return [(
            self.user_id,
            " ".join(self.transform_field(feature_names[y], delim) for y in top_k if tfidf[0][y] > 0.0),
        )]


    def get_output_lines(self, entity, delim=DEFAULT_DELIMITER, **kwargs):
        formatter = self.output_formatter[entity]
        if entity == TOP_WORDS:
            return formatter(delim, kwargs.pop('vectorizer'))
        else:
            return formatter(delim)



class FeedChunk(object):
    def __init__(self, vectorizer):
        self.counts = dict()
        self.strings = dict()
        self.writers = dict()
        self.vectorizer = vectorizer
        for entity in ENTITIES:
            self.counts[entity] = 0
            self.strings[entity] = StringIO()
            self.writers[entity] = unicodecsv.writer(
                self.strings[entity],
                encoding='utf-8',
                delimiter="\t"
            )

    def add_feed_from_key(self, key):
        feed = None
        for attempt in range(3):
            try:
                feed = FeedFromS3(key)
                break
            except (httplib.IncompleteRead, socket.error):
                time.sleep(1)
                pass
            except KeyError:
                break
        if feed is not None:
            self.merge_feed(feed)

    def merge_feed(self, feed):
        for entity in ENTITIES:
            lines = feed.get_output_lines(entity, vectorizer=self.vectorizer)
            for line in lines:
                self.writers[entity].writerow(line)
            self.counts[entity] += len(lines)

    def write_s3(self, conn_s3, bucket_name, key_names):
        bucket = conn_s3.get_bucket(bucket_name)
        for entity in ENTITIES:
            write_string_to_key(
                bucket,
                key_names[entity],
                self.strings[entity].getvalue()
            )

