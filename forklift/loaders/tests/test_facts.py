import datetime
from mock import patch
from sqlalchemy import Integer, BigInteger, DateTime, String
from sqlalchemy.exc import ProgrammingError

from forklift.db.utils import staging_table, drop_table_if_exists, create_new_table, get_rowcount
from forklift.models.base import Base
from forklift.models.raw import Event, Visit, Visitor
from forklift.warehouse.definition import HourlyAggregateTable
from forklift.warehouse.columns import Fact, Dimension
import forklift.loaders.fact.hourly as loaders
import forklift.loaders.fbsync as fbsync

from forklift.testing import ForkliftTestCase

import logging
logger = logging.getLogger(__name__)

class LoaderTestCase(ForkliftTestCase):

    @classmethod
    def visitor_templates(cls):
        return [{
            'fbid': 4,
            'visits': [{
                'events': [
                    {'type': 'authorized'},
                    {'type': 'generated'},
                    {'type': 'shown'},
                    {'type': 'faces_page_rendered'},
                    {'type': 'donated'},
                ],
                'ip': '127.0.0.1',
            }, {
                'events': [
                    {'type': 'authorized'},
                    {'type': 'donated'},
                    {'type': 'generated', 'out_of_range': True},
                    {'type': 'shown', 'out_of_range': True},
                ],
                'ip': '8.8.8.8',
            }],
        }, {
            'fbid': 6,
            'visits': [{
                'events': [
                    {'type': 'authorized'},
                    {'type': 'generated'},
                    {'type': 'shown'},
                    {'type': 'faces_page_rendered'},
                    {'type': 'shared', 'friend_fbid': 999},
                    {'type': 'donated'},
                ],
                'ip': '192.168.1.188',
            }],
        }, {
            'fbid': None,
            'visits': [{
                'events': [
                    {'type': 'stuff'},
                ],
                'ip': '123.45.67.89',
            }],
        }]

    @classmethod
    def setUpClass(cls):
        super(LoaderTestCase, cls).setUpClass()
        cls.campaign_id = 6
        cls.hour = datetime.datetime(2014,2,1,2,0)
        cls.in_range = datetime.datetime(2014,2,1,2,30)
        cls.out_of_range = datetime.datetime(2014,2,1,4)

        for visitor_template in cls.visitor_templates():
            visitor = Visitor(fbid=visitor_template['fbid'], created=cls.in_range, updated=cls.in_range)
            cls.session.add(visitor)
            cls.session.commit()
            for visit_template in visitor_template['visits']:
                visit = Visit(ip=visit_template['ip'], visitor_id=visitor.visitor_id, created=cls.in_range, updated=cls.in_range)
                cls.session.add(visit)
                cls.session.commit()
                for event_template in visit_template['events']:
                    timestamp = cls.out_of_range if 'out_of_range' in event_template else cls.in_range
                    friend_fbid = event_template['friend_fbid'] if 'friend_fbid' in event_template else None
                    event = Event(
                        event_type=event_template['type'],
                        visit_id=visit.visit_id,
                        created=timestamp,
                        campaign_id=cls.campaign_id,
                        updated=timestamp,
                        event_datetime=timestamp,
                        friend_fbid=friend_fbid,
                    )
                    cls.session.add(event)
                    cls.session.commit()


class TestFactsHourly(HourlyAggregateTable):
    slug = 'test'
    facts = (
        Fact(
            slug='donated',
            pretty_name='Unique Event Ids Donated',
            expression="count(distinct case when events.type='donated' then events.event_id else null end)",
        ),
    )

    extra_dimensions = (
        Dimension(
            slug='event_id',
            pretty_name='Event ID',
            column_name='event_id',
            source_table='events',
            datatype=Integer,
        ),
    )

class TestLoader(loaders.HourlyFactLoader):
    aggregate_table = TestFactsHourly
    joins = (
        'join visits using (visit_id)',
    )
    dimension_source = 'visits'


class GenericHourlyLoaderTestCase(LoaderTestCase):
    loader = TestLoader()

    @classmethod
    def setUpClass(cls):
        super(GenericHourlyLoaderTestCase, cls).setUpClass()
        cls.destination_table = 'test_facts_hourly'
        cls.transaction = cls.connection.begin_nested()
        cls.connection.execute("""
            create table {} (
                hour timestamp,
                campaign_id integer,
                event_id integer,
                donated integer
            )
        """.format(cls.destination_table))


    @classmethod
    def tearDownClass(cls):
        cls.transaction.rollback()


    def setUp(self):
        self.transaction = self.connection.begin_nested()


    def tearDown(self):
        self.transaction.rollback()


    def get_donated_total(self):
        return self.connection.execute("""
            select sum(donated) as donated
            from {} where campaign_id = {} and hour = '{}'
        """.format(self.destination_table, self.campaign_id, self.hour))

    def test_load(self):
        self.loader.load_hour(self.hour, self.connection, logger)
        self.assertSingleResult({'donated': 3}, self.get_donated_total())


    def test_dimensions(self):
        self.assertEqual(
            ['campaign_id', 'event_id'],
            self.loader.dimensions()
        )


    def test_upsert(self):
        event_id = 5
        new_val = 3

        self.connection.execute("""
            insert into {} values ('{}', {}, {}, {})
        """.format(self.destination_table, self.hour, self.campaign_id, event_id, 8))


        with staging_table(self.destination_table, self.connection) as staging_table_name:
            self.connection.execute("""
                insert into {} values ('{}', {}, {}, {})
            """.format(staging_table_name, self.hour, self.campaign_id, event_id, new_val))
            self.loader.upsert(self.hour, staging_table_name, self.destination_table, self.connection)

        self.assertSingleResult({'donated': new_val}, self.get_donated_total())


    def test_upsert_atomicity(self):
        old_val = 23
        event_id = 9
        self.connection.execute("""
            insert into {} values ('{}', {}, {}, {})
        """.format(self.destination_table, self.hour, self.campaign_id, event_id, old_val))

        with staging_table(self.destination_table, self.connection) as staging_table_name:
            self.connection.execute("""
                insert into {} values ('{}', {}, {}, {})
            """.format(staging_table_name, self.hour, self.campaign_id, event_id, 6))

            # cause the upsert to break towards the end by mangling the staging table name
            # and make sure the old data is still intact
            try:
                self.loader.upsert(self.hour, 'fake_staging_table_name', self.destination_table, self.connection)
            except ProgrammingError:
                pass

        self.assertSingleResult({'donated': old_val}, self.get_donated_total())



class EdgeflipLoaderTestCase(LoaderTestCase):
    def test_stage_hour_fbid(self):
        loader = loaders.FbidHourlyFactLoader()
        with staging_table(loader.destination_table, self.connection) as staging_table_name:
            loader.stage_hour(self.hour, staging_table_name, self.connection)
            result = self.connection.execute("""
                select
                    sum(fbids_authorized) as fbids_authorized,
                    sum(fbids_generated_friends) as fbids_generated_friends,
                    sum(fbids_shown_friends) as fbids_shown_friends,
                    sum(fbids_face_pages) as fbids_face_pages,
                    sum(fbids_shared) as fbids_shared
                from {} where campaign_id = {} and hour = '{}'
            """.format(staging_table_name, self.campaign_id, self.hour))

            expected = {
                'fbids_authorized': 2,
                'fbids_generated_friends': 2,
                'fbids_shown_friends': 2,
                'fbids_face_pages': 2,
                'fbids_shared': 1,
            }

            self.assertSingleResult(expected, result)


    def test_stage_hour_visit(self):
        loader = loaders.VisitHourlyFactLoader()
        with staging_table(loader.destination_table, self.connection) as staging_table_name:
            loader.stage_hour(self.hour, staging_table_name, self.connection)
            result = self.connection.execute("""
                select
                    sum(visits_shown_friend_sugg) as visits_shown_friend_sugg,
                    sum(authorized_visits) as authorized_visits,
                    sum(visits_with_shares) as visits_with_shares
                from {} where campaign_id = {} and hour = '{}'
            """.format(staging_table_name, self.campaign_id, self.hour))

            expected = {
                'visits_shown_friend_sugg': 2,
                'authorized_visits': 3,
                'visits_with_shares': 1,
            }

            self.assertSingleResult(expected, result)


    def test_stage_hour_ip(self):
        loader = loaders.IpHourlyFactLoader()
        with staging_table(loader.destination_table, self.connection) as staging_table_name:
            loader.stage_hour(self.hour, staging_table_name, self.connection)
            result = self.connection.execute("""
                select sum(ips_authorized) as ips_authorized
                from {} where campaign_id = {} and hour = '{}'
            """.format(staging_table_name, self.campaign_id, self.hour))

            expected = {
                'ips_authorized': 3,
            }

            self.assertSingleResult(expected, result)


    def test_stage_hour_friend(self):
        loader = loaders.FriendFbidHourlyFactLoader()
        with staging_table(loader.destination_table, self.connection) as staging_table_name:
            loader.stage_hour(self.hour, staging_table_name, self.connection)
            result = self.connection.execute("""
                select sum(friends_shared_with) as friends_shared_with
                from {} where campaign_id = {} and hour = '{}'
            """.format(staging_table_name, self.campaign_id, self.hour))

            expected = {
                'friends_shared_with': 1,
            }

            self.assertSingleResult(expected, result)


    def test_stage_hour_misc(self):
        loader = loaders.MiscHourlyFactLoader()
        with staging_table(loader.destination_table, self.connection) as staging_table_name:
            loader.stage_hour(self.hour, staging_table_name, self.connection)
            result = self.connection.execute("""
                select sum(authorizations) as authorizations
                from {} where campaign_id = {} and hour = '{}'
            """.format(staging_table_name, self.campaign_id, self.hour))

            expected = {
                'authorizations': 3,
            }

            self.assertSingleResult(expected, result)


class FBSyncTestCase(ForkliftTestCase):
    EXISTING_POST_ID = "54_244"

    @classmethod
    def setUpClass(cls):
        super(FBSyncTestCase, cls).setUpClass()
        # set up posts tables
        cls.posts_table = fbsync.POSTS_TABLE
        cls.posts_raw_table = fbsync.raw_table_name(cls.posts_table)
        cls.posts_incremental_table = fbsync.incremental_table_name(cls.posts_table)
        cls.create_tables(cls.posts_table)

        cls.user_posts_table = fbsync.USER_POSTS_TABLE
        cls.user_posts_raw_table = fbsync.raw_table_name(cls.user_posts_table)
        cls.user_posts_incremental_table = fbsync.incremental_table_name(cls.user_posts_table)
        cls.create_tables(cls.user_posts_table)

        cls.page_likes_table = fbsync.LIKES_TABLE
        cls.page_likes_raw_table = fbsync.raw_table_name(cls.page_likes_table)
        cls.page_likes_incremental_table = fbsync.incremental_table_name(cls.page_likes_table)
        cls.create_tables(cls.page_likes_table)

        # insert some common data that most of the tests will use (a post with a dupe)
        cls.insertDummyPost(cls.posts_raw_table)
        cls.insertDummyPost(cls.posts_raw_table)


    @classmethod
    def create_tables(cls, table_name):
        drop_table_if_exists(fbsync.raw_table_name(table_name), cls.connection)
        cls.connection.execute(fbsync.create_sql(fbsync.raw_table_name(table_name)))
        drop_table_if_exists(table_name, cls.connection)
        create_new_table(table_name, fbsync.raw_table_name(table_name), cls.connection)
        drop_table_if_exists(fbsync.incremental_table_name(table_name), cls.connection)
        create_new_table(fbsync.incremental_table_name(table_name), fbsync.raw_table_name(table_name), cls.connection)
        create_new_table(fbsync.raw_table_name(fbsync.incremental_table_name(table_name)), table_name, cls.connection)


    def setUp(self):
        self.transaction = self.connection.begin_nested()


    def tearDown(self):
        self.transaction.rollback()


    @classmethod
    def insertDummyPost(cls, table, post_type='stuff', post_id=None):
        post_id = post_id or cls.EXISTING_POST_ID
        # beginning data is inserted into the raw table, that is, the one which houses duplicates
        # can't use the ORM here because no primary key can possibly be involved when dealing with dupes like these
        ts = datetime.datetime(2014,2,1,2,0)
        cls.connection.execute(
            "insert into {} (fbid_post, fbid_user, ts, type) values (%s, %s, %s, %s)".format(table),
            (post_id, 65, ts, post_type)
        )


    def insertDummyUserPost(self, post_id, user_id, table=None):
        table = table or fbsync.raw_table_name(self.user_posts_table)
        self.connection.execute(
            "insert into {} (fbid_post, fbid_user, fbid_poster, user_to) values (%s, %s, %s, %s)".format(table),
            (post_id, user_id, 90, True)
        )


    def assert_num_results(self, num, table, post_id=None):
        post_id = post_id or self.EXISTING_POST_ID
        self.assertSingleResult(
            { 'count': num },
            self.connection.execute(
                'select count(*) as count from {} where fbid_post = %s'.format(table),
                (post_id,)
            )
        )

    def test_dedupe(self):
        # 1. make sure the raw table is set up with a dupe and the final table doesn't have anything yet
        self.assert_num_results(2, self.posts_raw_table)
        self.assert_num_results(0, self.posts_table)

        # 2. run the routine
        fbsync.dedupe(self.posts_raw_table, self.posts_table, self.connection)

        # 3. make sure that the data got transferred to the final table without the dupe
        self.assert_num_results(2, self.posts_raw_table)
        self.assert_num_results(1, self.posts_table)


    @patch('forklift.db.utils.optimize')
    def test_merge_posts(self, optimize_mock):
        new_post_id = "123_456"

        # 1. put some pre-existing data in the final table, but make sure it's clean beforehand
        self.assert_num_results(0, self.posts_table)
        self.assert_num_results(0, self.posts_table, post_id=new_post_id)
        self.__class__.insertDummyPost(table=self.posts_table)

        # 2. put some new data in the incremental table
        self.__class__.insertDummyPost(table=self.posts_incremental_table, post_id=new_post_id)

        # 3. run merge
        fbsync.merge_posts(self.posts_incremental_table, self.posts_table, self.connection)

        # 4. make sure that data got transferred into the final table alongside the old stuff
        self.assert_num_results(1, self.posts_table)
        self.assert_num_results(1, self.posts_table, post_id=new_post_id)

        # 5. and we do want to optimize the tables after loading
        self.assertEqual(optimize_mock.call_count, 1)


    @patch('forklift.db.utils.optimize')
    def test_merge_user_posts(self, optimize_mock):
        old_user_id = 40
        new_user_id = 50
        # 1. pre-existing data
        self.insertDummyUserPost(self.EXISTING_POST_ID, old_user_id, table=self.user_posts_table)

        # 2. new data to load
        self.insertDummyUserPost(self.EXISTING_POST_ID, old_user_id, table=self.user_posts_incremental_table)
        self.insertDummyUserPost(self.EXISTING_POST_ID, new_user_id, table=self.user_posts_incremental_table)

        # 3. do it
        fbsync.merge_user_posts(self.user_posts_incremental_table, self.user_posts_table, self.connection)

        # 4. make sure we have data, tied to the correct post
        self.assertEqual(2, get_rowcount(self.user_posts_table, self.connection))
        self.assert_num_results(2, self.user_posts_table, post_id=self.EXISTING_POST_ID)


    # more of an integration test, minus the s3 stuff of course
    @patch('forklift.db.utils.optimize')
    @patch('forklift.db.utils.load_from_s3')
    def test_add_new_data(self, load_mock, optimize_mock):
        first_post_id = '1_2'
        first_user = '1'

        # 1. pre-existing data
        self.__class__.insertDummyPost(table=self.posts_table, post_id=first_post_id)
        self.insertDummyUserPost(first_post_id, first_user, table=self.user_posts_table)

        second_post_id = '2_3'
        # 2. insert data with duplicates into the raw table
        def fake_load_from_s3(connection, bucket_name, key_name, table_name, create_statement=None):
            if table_name.startswith('posts'):
                self.__class__.insertDummyPost(table=fbsync.raw_table_name(self.posts_incremental_table), post_id=second_post_id)
                self.__class__.insertDummyPost(table=fbsync.raw_table_name(self.posts_incremental_table), post_id=second_post_id)
            else:
                self.insertDummyUserPost(second_post_id, first_user, table=fbsync.raw_table_name(self.user_posts_incremental_table))
                self.insertDummyUserPost(second_post_id, first_user, table=fbsync.raw_table_name(self.user_posts_incremental_table))
        load_mock.side_effect = fake_load_from_s3

        # 3. the s3 arguments don't really matter here
        fbsync.add_new_data('stuff', 'stuff', 'stuff', 'stuff', 'stuff', self.connection)

        # 4. verify both tables received correct data
        self.assertEqual(2, get_rowcount(self.posts_table, self.connection))
        self.assertEqual(2, get_rowcount(self.user_posts_table, self.connection))

