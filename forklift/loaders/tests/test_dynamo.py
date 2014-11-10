import logging
from forklift.db.base import redshift_engine
from forklift.db.utils import get_rowcount
from forklift.loaders.dynamo import DynamoLoader, USERS_TABLE, EDGES_TABLE
from forklift.models.dynamo import User, IncomingEdge, Token
from faraday import db
from mock import patch
import datetime
import os
import tempfile
from unittest import TestCase
logger = logging.getLogger(__name__)


class DynamoTestCase(TestCase):

    def setUp(self):
        db.build()

    def tearDown(self):
        db.destroy()


class DynamoSyncTestCase(DynamoTestCase):

    @patch('forklift.db.utils.load_from_s3')
    @patch('forklift.loaders.dynamo.write_file_to_key')
    def test_dynamo_sync(self, write_mock, load_mock):
        self.s3_writes = {}

        def fake_write(bucket_name, key_name, file_obj):
            file_obj.seek(0, os.SEEK_SET)
            contents = file_obj.read()
            self.s3_writes["{}/{}".format(bucket_name, key_name)] = contents
            file_obj.seek(0, os.SEEK_SET)

        def fake_load(
            connection,
            bucket_name,
            key_name,
            table_name,
            delimiter="\t"
        ):
            contents = self.s3_writes["{}/{}".format(bucket_name, key_name)]
            file_obj = tempfile.NamedTemporaryFile()
            file_obj.write(contents)
            file_obj.seek(0, os.SEEK_SET)
            from_conn = redshift_engine.raw_connection()
            from_curs = from_conn.cursor()
            query = "COPY {} FROM STDIN with DELIMITER '{}' NULL ''".format(
                table_name,
                delimiter
            )
            from_curs.copy_expert(
                query,
                file_obj
            )
            from_conn.commit()
        write_mock.side_effect = fake_write
        load_mock.side_effect = fake_load

        redshift_engine.execute('delete from {}'.format(USERS_TABLE))
        redshift_engine.execute('delete from {}'.format(EDGES_TABLE))
        redshift_engine.execute('delete from user_clients')

        # set up preliminary dynamo and redshift data
        primary = 5
        secondary = 3
        bday = datetime.datetime(1980, 12, 8, 7, 0, 0)
        activities = 'Sitting here watching the wheels go round and round'
        music = 'Plastic Ono Band'
        for fbid in (primary, secondary):
            User.items.create(
                fbid=fbid,
                fname="John{}".format(fbid),
                lname="Lennon{}".format(fbid),
                birthday=bday,
                activities=activities,
                music=music,
            )
        redshift_engine.execute(
            'insert into user_clients (fbid) values ({})'.format(primary)
        )
        IncomingEdge.items.create(
            fbid_target=primary,
            fbid_source=secondary,
            post_likes=1,
            stat_comms=3,
            wall_comms=5,
        )
        Token.items.create(
            fbid=primary,
            appid=1, # doesn't matter
            expires=datetime.datetime.utcnow(),
        )
        loader = DynamoLoader(logger, redshift_engine.connect())

        # run the sync process
        loader.sync()

        # primaries and secondaries should be in redshift now
        self.assertEqual(get_rowcount(USERS_TABLE, redshift_engine), 2)
        result = redshift_engine.execute('select * from {}'.format(USERS_TABLE))
        for row in result:
            self.assertEqual("John{}".format(row['fbid']), row['fname'])
            self.assertEqual("Lennon{}".format(row['fbid']), row['lname'])
            self.assertEqual(bday.date(), row['birthday'])
            self.assertEqual(str([unicode(music)]), row['music'])
            self.assertEqual(str([unicode(activities)]), row['activities'])

        # as well as the edge that brought the secondary there
        self.assertEqual(get_rowcount(EDGES_TABLE, redshift_engine), 1)
        result = redshift_engine.execute('select * from {}'.format(EDGES_TABLE))
        for row in result:
            self.assertEqual(primary, row['fbid_target'])
            self.assertEqual(secondary, row['fbid_source'])
            self.assertEqual(1, row['post_likes'])
            self.assertEqual(3, row['stat_comms'])
            self.assertEqual(5, row['wall_comms'])
