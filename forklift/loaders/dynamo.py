import datetime
from collections import defaultdict
from boto.dynamodb import exceptions
import logging
from cStringIO import StringIO
import time
import unicodecsv

from forklift.db import utils as dbutils
from forklift.models.dynamo import IncomingEdge, User
from forklift.s3.utils import write_file_to_key

USER_COLUMNS = (
    'fbid',
    'birthday',
    'fname',
    'lname',
    'email',
    'gender',
    'city',
    'state',
    'country',
    'activities',
    'affiliations',
    'books',
    'devices',
    'friend_request_count',
    'has_timeline',
    'interests',
    'languages',
    'likes_count',
    'movies',
    'music',
    'political',
    'profile_update_time',
    'quotes',
    'relationship_status',
    'religion',
    'sports',
    'tv',
    'wall_count',
    'updated',
)

EDGE_COLUMNS = (
    'fbid_source',
    'fbid_target',
    'post_likes',
    'post_comms',
    'stat_likes',
    'stat_comms',
    'wall_posts',
    'wall_comms',
    'tags',
    'photos_target',
    'photos_other',
    'mut_friends',
    'updated',
)
MAX_STRINGLEN = 4096
S3_BUCKET = "warehouse-forklift"
USERS_TABLE = 'users'
EDGES_TABLE = 'edges'


def transform_field(field):
    string_representation = field
    if isinstance(field, set):
        string_representation = str(list(field))
    if isinstance(field, list):
        string_representation = str(field)
    if isinstance(string_representation, basestring):
        string_representation = string_representation.replace("\t", " ") \
                                                     .replace("\n", " ") \
                                                     .replace("\x00", "")
        return string_representation[:MAX_STRINGLEN/2]

    return string_representation


class DynamoLoader(object):

    def __init__(self, logger, redshift_connection):
        self.logger = logger
        self.redshift_connection = redshift_connection
        self.s3_bucket = S3_BUCKET

    def oneweek_expression(self):
        return "dateadd(week, -1, getdate())"

    def get_primaries(self):
        # distinct primaries missing from users
        result = self.redshift_connection.execute("""
            SELECT DISTINCT(user_clients.fbid) AS fbid FROM {} u
            RIGHT JOIN user_clients using (fbid)
            WHERE
        u.fname IS NULL or
        (u.fname = ''AND updated < {})
        """.format(USERS_TABLE, self.oneweek_expression()))
        fbids = [row['fbid'] for row in result.fetchall()]
        return fbids

    def get_secondaries(self):
        # distinct source edges missing from users
        result = self.redshift_connection.execute("""
            SELECT DISTINCT(e.fbid_source) AS fbid FROM {} u
        RIGHT JOIN {} e ON u.fbid=e.fbid_source
        WHERE u.fname IS NULL or
        (u.fname = '' AND u.updated < {})
        """.format(USERS_TABLE, EDGES_TABLE, self.oneweek_expression()))
        fbids = [row['fbid'] for row in result.fetchall()]
        return fbids

    def get_missing_edges(self):
        result = self.redshift_connection.execute("""
            SELECT DISTINCT u.fbid from {} u
            LEFT JOIN visitors on (u.fbid = visitors.fbid)
            LEFT JOIN user_clients on (u.fbid = user_clients.fbid)
            LEFT JOIN {} e on (e.fbid_target = u.fbid)
            WHERE
                e.fbid_target is null
                AND COALESCE(visitors.fbid, user_clients.fbid) is not null
            """.format(USERS_TABLE, EDGES_TABLE))

        return [row['fbid'] for row in result.fetchall()]

    def edges_to_key(self, fbids, key_name):
        stringfile = StringIO()
        writer = unicodecsv.writer(stringfile, encoding='utf-8', delimiter="\t")
        i = 0
        found = 0
        not_found = 1
        for fbid in fbids:
            i += 1
            found = 0
            not_found = 0
            time.sleep(1)
            logging.info('Seeking edge relationships for fbid {}'.format(fbid))
            result = IncomingEdge.items.query(fbid_target__eq=fbid)
            if not result:
                not_found += 1
            else:
                found += 1
            self.logger.info("found %s edges from fbid %s",
                len(result),
                fbid
            )

            for edge in result:
                d = defaultdict(lambda: 0)
                d.update(edge)
                edge = d
                writer.writerow(
                    [transform_field(edge[field]) for field in EDGE_COLUMNS]
                )

        stringfile.seek(0)
        write_file_to_key(self.s3_bucket, key_name, stringfile)

    def fbids_to_key(self, fbids, keyname):
        stringfile = StringIO()
        writer = unicodecsv.writer(stringfile, encoding='utf-8', delimiter="\t")
        for fbid in fbids:
            data = defaultdict(lambda: None)
            try:
                self.logger.debug('Seeking key %s in dynamo', fbid)
                dyndata = User.items.get_item(fbid=fbid)
                data.update(dyndata)

                if 'birthday' in data and data['birthday']:
                    data['birthday'] = data['birthday'].date()

                if 'updated' not in data or not data['updated']:
                    # some sort of blank row
                    # track updated just to know when we went looking for it
                    data['updated'] = datetime.datetime.utcnow()


            except exceptions.DynamoDBKeyNotFoundError:
                data['updated'] = datetime.datetime.now()

            writer.writerow([
                fbid if field == 'fbid' else transform_field(data[field])
                for field in USER_COLUMNS
            ])

        stringfile.seek(0)
        write_file_to_key(self.s3_bucket, keyname, stringfile)

    def load_users(self, key_name, staging_table, final_table):
        self.logger.info("loading users")
        dbutils.drop_table_if_exists(staging_table, self.redshift_connection)
        self.logger.info("creating staging table")
        dbutils.create_new_table(
            staging_table,
            final_table,
            self.redshift_connection
        )
        with self.redshift_connection.begin():
            self.logger.info("copying from s3 into staging table")
            dbutils.load_from_s3(
                self.redshift_connection,
                self.s3_bucket,
                key_name,
                staging_table
            )
            self.logger.info("making room for new records")
            self.redshift_connection.execute(
                'delete from {} where fbid in (select distinct fbid from {})'.format(
                    final_table,
                    staging_table
                )
            )
            self.logger.info("inserting new records")
            self.redshift_connection.execute(
                'insert into {} select * from {}'.format(
                    final_table,
                    staging_table
                )
            )

    def sync(self):
        primaries_key = "primaries.csv"
        secondaries_key = "secondaries.csv"
        edges_key = "edges.csv"

        fbids = self.get_primaries()
        self.logger.info("found %s primaries", len(fbids))
        if len(fbids) > 0:
            self.fbids_to_key(fbids, primaries_key)
            self.load_users(
                primaries_key,
                'users_staging',
                USERS_TABLE
            )

        fbids = self.get_missing_edges()
        self.logger.info("found %s users missing edges", len(fbids))
        if len(fbids) > 0:
            self.edges_to_key(fbids, edges_key)
            dbutils.load_from_s3(
                self.redshift_connection,
                self.s3_bucket,
                edges_key,
                EDGES_TABLE
            )

        fbids = self.get_secondaries()
        self.logger.info("found %s secondaries", len(fbids))
        if len(fbids) > 0:
            self.fbids_to_key(fbids, secondaries_key)
            self.load_users(
                secondaries_key,
                'users_staging',
                USERS_TABLE
            )
