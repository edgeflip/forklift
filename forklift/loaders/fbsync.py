import logging
import forklift.db.utils as dbutils

DB_TEXT_LEN = 4096
UNIQUE_POST_ID_TABLE = 'fbid_post_ids'
BUCKET_NAME = 'redshift_transfer_tristan'
POSTS_TABLE = 'posts'
USER_POSTS_TABLE = 'user_posts'

logger = logging.getLogger(__name__)


def incremental_table_name(table_base):
    return "{}_incremental".format(table_base)


def raw_table_name(table_base):
    return "{}_raw".format(table_base)


def old_table_name(table_base):
    return "{}_old".format(table_base)


def create_sql(table_name):
    if table_name == raw_table_name(POSTS_TABLE) or table_name == raw_table_name(incremental_table_name(POSTS_TABLE)):
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
                message VARCHAR({text_len})
            )
        """.format(table=table_name, text_len=DB_TEXT_LEN)
    elif table_name == raw_table_name(USER_POSTS_TABLE) or table_name == raw_table_name(incremental_table_name(USER_POSTS_TABLE)):
        return """
            CREATE TABLE {table} (
                fbid_user VARCHAR(64) NOT NULL,
                fbid_post VARCHAR(64) NOT NULL,
                user_to BOOLEAN,
                user_like BOOLEAN,
                user_comment BOOLEAN
            )
        """.format(table=table_name)
    else:
        raise ValueError('Table {} not recognized'.format(table_name))


def dedupe(table_with_dupes, final_table_name, connection):
    deduped_table_name = "{}_deduped".format(final_table_name)
    dbutils.drop_table_if_exists(deduped_table_name, connection)
    sql = dedupe_sql(final_table_name).format(
        new_table=deduped_table_name,
        raw_table=table_with_dupes
    )
    logger.info('Deduping records from %s into staging table %s', table_with_dupes, deduped_table_name)
    connection.execute(sql)
    logger.info('%s records after deduping', dbutils.get_rowcount(deduped_table_name, connection))
    dbutils.deploy_table(
        final_table_name,
        deduped_table_name,
        old_table_name(final_table_name),
        connection
    )


def dedupe_sql(base_table_name):
    if base_table_name == POSTS_TABLE or base_table_name == incremental_table_name(POSTS_TABLE):
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
                max(message) as message
            from {raw_table}
            group by fbid_post
        """
    elif base_table_name == USER_POSTS_TABLE or base_table_name == incremental_table_name(USER_POSTS_TABLE):
        return """
            create table {new_table} as
            select
                fbid_post,
                max(fbid_user) as fbid_user,
                bool_or(user_to) as user_to,
                bool_or(user_like) as user_like,
                bool_or(user_comment) as user_comment
            from {raw_table}
            group by fbid_post
        """
    else:
        raise ValueError('Table {} not recognized'.format(base_table_name))


# when FBSync just grabbed some new data and we want to merge it in
def add_new_data(bucket_name, posts_folder, user_posts_folder, connection):
    posts_incremental = incremental_table_name(POSTS_TABLE)
    user_posts_incremental = incremental_table_name(USER_POSTS_TABLE)

    load_and_dedupe(bucket_name, posts_folder, posts_incremental, connection)
    load_and_dedupe(bucket_name, user_posts_folder, user_posts_incremental, connection)

    merge_posts(posts_incremental, POSTS_TABLE, connection)
    merge_user_posts(user_posts_incremental, USER_POSTS_TABLE, connection)


def load_and_dedupe(bucket_name, source_folder, table_name, connection, optimize=False):
    raw_table = raw_table_name(table_name)
    logger.info('Loading raw data into %s from s3://%s/%s', raw_table, bucket_name, source_folder)
    dbutils.load_from_s3(
        connection,
        bucket_name,
        source_folder,
        raw_table,
        create_statement=create_sql(raw_table)
    )
    logger.info('%s rows loaded into %s', dbutils.get_rowcount(raw_table, connection), raw_table)
    dedupe(raw_table, table_name, connection)
    if optimize:
        logger.info('Optimizing table %s', table_name)
        optimize(table_name, connection)



# take deduped new posts from 'incremental_table' and merge them into 'final_table'
def merge_posts(incremental_table, final_table, connection):
    with connection.begin():
        temp_table = incremental_table + '_unique'
        # populate list of new post ids
        dbutils.drop_table_if_exists(temp_table, connection)
        logger.info('Populating list of new post ids from %s compared with records in %s', incremental_table, final_table)
        connection.execute("""
            CREATE TEMPORARY TABLE {temp_table} AS
            SELECT distinct(fbid_post) fbid_post
            FROM {incremental_table}
            LEFT JOIN {final_table} USING (fbid_post)
            WHERE {final_table}.fbid_post is NULL
        """.format(
            temp_table=temp_table,
            incremental_table=incremental_table,
            final_table=final_table
        ))

        logger.info('%s new posts found', dbutils.get_rowcount(temp_table, connection))

        # insert new versions into final table
        logger.info('Inserting new rows from %s into %s', incremental_table, final_table)
        connection.execute("""
            INSERT INTO {final_table}
            SELECT {incremental_table}.*
            FROM {temp_table}
            JOIN {incremental_table} using (fbid_post)
        """.format(
            final_table=final_table,
            incremental_table=incremental_table,
            temp_table=temp_table,
        ))

    dbutils.optimize(final_table, connection)


# take deduped new user_posts from 'incremental_table' and merge them into 'final_table'
def merge_user_posts(incremental_table, final_table, connection):
    with connection.begin():
        temp_table = incremental_table + '_unique'
        # populate list of new post ids
        dbutils.drop_table_if_exists(temp_table, connection)
        logger.info('Populating list of new post ids from %s compared with records in %s', incremental_table, final_table)
        connection.execute("""
            CREATE TEMPORARY TABLE {temp_table} AS
            SELECT distinct fbid_post, fbid_user
            FROM {incremental_table}
            LEFT JOIN {final_table} USING (fbid_post, fbid_user)
            WHERE {final_table}.fbid_post is NULL
        """.format(
            temp_table=temp_table,
            incremental_table=incremental_table,
            final_table=final_table
        ))

        logger.info('%s new user_posts found', dbutils.get_rowcount(temp_table, connection))

        # insert new versions into final table
        logger.info('Inserting new rows from %s into %s', incremental_table, final_table)
        connection.execute("""
            INSERT INTO {final_table}
            SELECT {incremental_table}.*
            FROM {temp_table}
            JOIN {incremental_table} using (fbid_post, fbid_user)
        """.format(
            final_table=final_table,
            incremental_table=incremental_table,
            temp_table=temp_table,
        ))

    dbutils.optimize(final_table, connection)
