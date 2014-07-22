import logging
import forklift.db.utils as dbutils
import datetime
from collections import defaultdict
import json
import logging
import tempfile
from urlparse import urlparse

logger = logging.getLogger(__name__)

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
        prim_id, fbid = key.name.split("_")

        feed_json = None
        with tempfile.TemporaryFile() as fp:
            key.get_contents_to_file(fp)
            fp.seek(0)
            feed_json = json.load(fp)
        try:
            feed_json_list = feed_json['data']
        except KeyError:
            logger.debug("no data in feed %s" % key.name)
            logger.debug(str(feed_json))
            raise

        logger.debug("\tread feed json with %d posts from %s" % (len(feed_json_list), key.name))

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

    def transform_field(self, field, delim):
        if isinstance(field, basestring):
            return field.replace(delim, " ").replace("\n", " ").replace("\x00", "").encode('utf8', 'ignore')
        else:
            return str(field)


    def get_post_lines(self, delim="\t"):
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
            post_lines.append(delim.join(self.transform_field(field, delim) for field in post_fields))
        return post_lines

    def get_link_lines(self, delim="\t"):
        link_lines = []
        for p in self.posts:
            user_ids = None
            comment_ids = None
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
                        # we're doing bag of words so separating comments more granularly than this shouldn't matter
                        comment_text = " ".join(p.commenters[user_id])
                        num_comments = str(len(p.commenters[user_id]))
                    else:
                        has_comm = ""
                        comment_text = ""
                        num_comments = "0"

                    link_fields = (p.post_id, user_id, self.user_id, has_to, has_like, has_comm, num_comments, comment_text)
                    link_lines.append(delim.join(f.encode('utf8', 'ignore') for f in link_fields))
        return link_lines


    def get_like_lines(self, delim="\t"):
        if not hasattr(self, 'page_likes'):
            return ()
        return (
            delim.join(str(f) for f in (self.user_id, like))
            for like in self.page_likes
        )

