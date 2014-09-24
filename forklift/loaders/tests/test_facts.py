import datetime
from mock import patch
from sqlalchemy import Integer
from sqlalchemy.exc import ProgrammingError

from forklift.db.base import redshift_engine as engine
from forklift.db.utils import staging_table, drop_table_if_exists, create_new_table, get_rowcount
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
        # FBSync code has to do more manual transaction control,
        # so use the engine directly and drop the tables when we're done
        with engine.connect() as connection:
            cls.version = 123456
            cls.posts_table = fbsync.POSTS_TABLE
            cls.posts_raw_table = fbsync.raw_table_name(cls.posts_table)
            cls.posts_incremental_table = fbsync.incremental_table_name(cls.posts_table, cls.version)
            cls.create_tables(cls.posts_table, connection)

            cls.user_posts_table = fbsync.USER_POSTS_TABLE
            cls.user_posts_raw_table = fbsync.raw_table_name(cls.user_posts_table)
            cls.user_posts_incremental_table = fbsync.incremental_table_name(cls.user_posts_table, cls.version)
            cls.create_tables(cls.user_posts_table, connection)

            cls.page_likes_table = fbsync.LIKES_TABLE
            cls.page_likes_raw_table = fbsync.raw_table_name(cls.page_likes_table)
            cls.page_likes_incremental_table = fbsync.incremental_table_name(cls.page_likes_table, cls.version)
            cls.create_tables(cls.page_likes_table, connection)

            cls.top_words_table = fbsync.TOP_WORDS_TABLE
            cls.top_words_raw_table = fbsync.raw_table_name(cls.top_words_table)
            cls.top_words_incremental_table = fbsync.incremental_table_name(cls.top_words_table, cls.version)
            cls.create_tables(cls.top_words_table, connection)

            for table_name in (
                fbsync.POST_AGGREGATES_TABLE,
                fbsync.INTERACTOR_AGGREGATES_TABLE,
                fbsync.POSTER_AGGREGATES_TABLE,
                fbsync.USER_AGGREGATES_TABLE,
                fbsync.USERS_TABLE,
                fbsync.EDGES_TABLE,
                fbsync.USER_CLIENTS_TABLE,
            ):
                drop_table_if_exists(table_name, connection)
                connection.execute(fbsync.create_sql(table_name))

            # insert some common data that most of the tests will use (a post with a dupe)
            cls.insertDummyPost(connection, cls.posts_raw_table)
            cls.insertDummyPost(connection, cls.posts_raw_table)


    @classmethod
    def create_tables(cls, table_name, connection):
        drop_table_if_exists(fbsync.raw_table_name(table_name), connection)
        connection.execute(fbsync.create_sql(fbsync.raw_table_name(table_name), cls.version))
        drop_table_if_exists(table_name, connection)
        create_new_table(table_name, fbsync.raw_table_name(table_name), connection)
        drop_table_if_exists(fbsync.incremental_table_name(table_name, cls.version), connection)
        create_new_table(fbsync.incremental_table_name(table_name, cls.version), fbsync.raw_table_name(table_name), connection)
        create_new_table(fbsync.raw_table_name(fbsync.incremental_table_name(table_name, cls.version)), table_name, connection)


    def ensureCleanTable(self, incremental_table, raw_table, connection):
        drop_table_if_exists(incremental_table, connection)
        create_new_table(
            incremental_table,
            raw_table,
            connection
        )

    def setUp(self):
        self.transaction = self.connection.begin_nested()
        fbsync.datediff_expression = lambda: "DATE_PART('year', now()) - DATE_PART('year', birthday)"


    def tearDown(self):
        self.transaction.rollback()


    @classmethod
    def insertDummyPost(cls, connection, table, post_type='stuff', post_id=None, user_id=None, ts=None):
        post_id = post_id or cls.EXISTING_POST_ID
        user_id = user_id or 65
        # beginning data is inserted into the raw table, that is, the one which houses duplicates
        # can't use the ORM here because no primary key can possibly be involved when dealing with dupes like these
        ts = ts or datetime.datetime(2014,2,1,2,0)
        connection.execute(
            "insert into {} (fbid_post, fbid_user, ts, type) values (%s, %s, %s, %s)".format(table),
            (post_id, user_id, ts, post_type)
        )


    def insertDummyUserPost(self, connection, post_id, user_id, table=None, poster_id=None, to=True, comm=False, like=False):
        table = table or fbsync.raw_table_name(self.user_posts_table)
        poster_id = poster_id or 90
        connection.execute(
            "insert into {} (fbid_post, fbid_user, fbid_poster, user_to, user_comment, user_like) values (%s, %s, %s, %s, %s, %s)".format(table),
            (post_id, user_id, poster_id, to, comm, like)
        )

    def insertDummyPageLike(self, connection, user_id):
        page_id = 834
        table = self.page_likes_incremental_table
        connection.execute(
            "insert into {} (fbid, page_id) values (%s, %s)".format(table),
            (user_id, page_id)
        )

    def insertDummyTopWords(self, connection, user_id):
        top_words = "obama romney fisticuffs"
        table = self.top_words_incremental_table
        connection.execute(
            "insert into {} (fbid, top_words) values (%s, %s)".format(table),
            (user_id, top_words)
        )

    def assert_num_results(self, num, table, post_id=None):
        post_id = post_id or self.EXISTING_POST_ID
        self.assertSingleResult(
            { 'count': num },
            engine.execute(
                'select count(*) as count from {} where fbid_post = %s'.format(table),
                (post_id,)
            )
        )

    def test_dedupe(self):
        # 1. make sure the raw table is set up with a dupe and the final table doesn't have anything yet
        self.assert_num_results( 2, self.posts_raw_table)
        self.assert_num_results( 0, self.posts_table)

        # 2. run the routine
        with engine.connect() as connection:
            drop_table_if_exists(self.posts_incremental_table, connection)
        fbsync.dedupe(self.posts_raw_table, self.posts_incremental_table, self.version, engine)

        # 3. make sure that the data got transferred to the final table without the dupe
        self.assert_num_results( 2, self.posts_raw_table)
        self.assert_num_results( 1, self.posts_incremental_table)


    def test_merge_posts(self):
        new_post_id = "123_456"

        with engine.connect() as connection:
            self.ensureCleanTable(
                self.posts_incremental_table,
                self.posts_raw_table,
                connection
            )

            connection.execute('truncate table {}'.format(self.posts_table))

            self.__class__.insertDummyPost(connection, table=self.posts_incremental_table, post_id=new_post_id)

            fbsync.merge_posts(self.posts_incremental_table, self.posts_table, connection)

        # 4. make sure that data got transferred into the final table alongside the old stuff
        self.assert_num_results(1, self.posts_table, post_id=new_post_id)


    def test_merge_user_posts(self):
        with engine.connect() as connection:
            connection.execute('truncate {}'.format(self.user_posts_table))
            self.ensureCleanTable(
                self.user_posts_incremental_table,
                self.user_posts_raw_table,
                connection
            )
            old_user_id = 40
            new_user_id = 50
            # 1. new data to load
            self.insertDummyUserPost(connection, self.EXISTING_POST_ID, old_user_id, table=self.user_posts_incremental_table)
            self.insertDummyUserPost(connection, self.EXISTING_POST_ID, new_user_id, table=self.user_posts_incremental_table)

            # 2. do it
            fbsync.merge_user_posts(self.user_posts_incremental_table, self.user_posts_table, connection)

            # 3. make sure we have data, tied to the correct post
            self.assertEqual(2, get_rowcount(self.user_posts_table, connection))
            self.assert_num_results( 2, self.user_posts_table, post_id=self.EXISTING_POST_ID)


    def test_merge_likes(self):
        with engine.connect() as connection:
            self.ensureCleanTable(
                self.page_likes_incremental_table,
                self.page_likes_raw_table,
                connection
            )
            first_user = 45
            second_user = 55
            self.insertDummyPageLike(connection, first_user)
            self.insertDummyPageLike(connection, second_user)

            fbsync.merge_likes(self.page_likes_incremental_table, self.page_likes_table, connection)

            self.assertEqual(2, get_rowcount(self.page_likes_table, connection))

    def test_merge_top_words(self):
        with engine.connect() as connection:
            self.ensureCleanTable(
                self.top_words_incremental_table,
                self.top_words_raw_table,
                connection
            )
            first_user = 45
            second_user = 55
            self.insertDummyTopWords(connection, first_user)
            self.insertDummyTopWords(connection, second_user)

            fbsync.merge_top_words(self.top_words_incremental_table, self.top_words_table, connection)

            self.assertEqual(2, get_rowcount(self.top_words_table, connection))

    def test_calculate_users_with_new_posts(self):
        post_id = 45
        with engine.connect() as connection:
            self.ensureCleanTable(
                self.posts_incremental_table,
                self.posts_raw_table,
                connection
            )
            self.__class__.insertDummyPost(connection, table=self.posts_incremental_table, post_id=post_id)
        updated_table = fbsync.calculate_users_with_new_posts(self.posts_incremental_table, engine)
        self.assertEqual(1, get_rowcount(updated_table, engine))

    def test_calculate_users_with_outbound_interactions(self):
        with engine.connect() as connection:
            table = self.user_posts_incremental_table
            self.ensureCleanTable(
                table,
                self.user_posts_raw_table,
                connection
            )
            self.insertDummyUserPost(
                connection,
                post_id=1,
                user_id=45,
                table=table,
            )
            self.insertDummyUserPost(
                connection,
                post_id=2,
                user_id=45,
                table=table,
            )
            self.insertDummyUserPost(
                connection,
                post_id=3,
                user_id=85,
                table=table,
            )
        updated_table = fbsync.calculate_users_with_outbound_interactions(
            table, engine
        )
        self.assertEqual(2, get_rowcount(updated_table, engine))

    def test_calculate_users_with_inbound_interactions(self):
        with engine.connect() as connection:
            table = self.user_posts_incremental_table
            self.ensureCleanTable(
                table,
                self.user_posts_raw_table,
                connection
            )
            self.insertDummyUserPost(
                connection,
                post_id=1,
                user_id=45,
                table=table,
            )
            self.insertDummyUserPost(
                connection,
                post_id=2,
                user_id=85,
                table=table,
            )
        updated_table = fbsync.calculate_users_with_inbound_interactions(
            table, engine
        )
        self.assertEqual(1, get_rowcount(updated_table, engine))

    def test_merge_post_aggregates(self):
        updated_users_table = 'updated'
        with engine.connect() as connection:
            connection.execute('truncate {}'.format(fbsync.POST_AGGREGATES_TABLE))
            connection.execute('create table {} (fbid bigint)'.format(updated_users_table))
            connection.execute('insert into {} values (1)'.format(updated_users_table))
            connection.execute('insert into {} values (2)'.format(updated_users_table))

            jan = datetime.date(2014, 1, 1)
            feb = datetime.date(2014, 2, 1)
            mar = datetime.date(2014, 3, 1)
            self.insertDummyPost(
                connection,
                self.posts_table,
                post_type='status',
                user_id=1,
                post_id=1,
                ts=jan
            )
            self.insertDummyPost(
                connection,
                self.posts_table,
                post_type='status',
                user_id=1,
                post_id=2,
                ts=feb
            )
            self.insertDummyPost(
                connection,
                self.posts_table,
                post_type='status',
                user_id=2,
                post_id=3,
                ts=mar
            )

        try:
            fbsync.merge_post_aggregates(
                updated_users_table,
                self.posts_table,
                fbsync.POST_AGGREGATES_TABLE,
                engine
            )

            self.assertEqual(
                2,
                get_rowcount(fbsync.POST_AGGREGATES_TABLE, engine)
            )
            results = engine.execute(
                "select * from {}".format(fbsync.POST_AGGREGATES_TABLE)
            )
            for row in results:
                print row
                (fbid, first, last, num_st) = row
                if fbid == 1:
                    self.assertEqual(first, jan)
                    self.assertEqual(last, feb)
                    self.assertEqual(num_st, 2)
                else:
                    self.assertEqual(first, mar)
                    self.assertEqual(last, mar)
                    self.assertEqual(num_st, 1)

            with engine.connect() as connection:
                self.insertDummyPost(
                    connection,
                    self.posts_table,
                    post_type='status',
                    user_id=2,
                    post_id=4,
                    ts=mar
                )

            fbsync.merge_post_aggregates(
                updated_users_table,
                self.posts_table,
                fbsync.POST_AGGREGATES_TABLE,
                engine
            )

            results = engine.execute(
                "select * from {}".format(fbsync.POST_AGGREGATES_TABLE)
            )
            for row in results:
                print row
                (fbid, first, last, num_st) = row
                if fbid == 1:
                    self.assertEqual(first, jan)
                    self.assertEqual(last, feb)
                    self.assertEqual(num_st, 2)
                else:
                    self.assertEqual(first, mar)
                    self.assertEqual(last, mar)
                    self.assertEqual(num_st, 2)
        finally:
            engine.execute('DROP TABLE {}'.format(updated_users_table))

    def test_merge_interactor_aggregates(self):
        updated_users_table = 'updated'
        with engine.connect() as connection:
            connection.execute('truncate {}'.format(self.user_posts_table))
            connection.execute('truncate {}'.format(fbsync.INTERACTOR_AGGREGATES_TABLE))
            connection.execute('create table {} (fbid_user bigint)'.format(updated_users_table))
            connection.execute('insert into {} values (1)'.format(updated_users_table))
            connection.execute('insert into {} values (2)'.format(updated_users_table))

            self.insertDummyUserPost(connection, 1, 1, like=True, table=self.user_posts_table)
            self.insertDummyUserPost(connection, 1, 2, like=True, comm=True, table=self.user_posts_table)
            self.insertDummyUserPost(connection, 2, 2, like=True, table=self.user_posts_table)
            self.insertDummyUserPost(connection, 3, 2, table=self.user_posts_table)

        try:
            fbsync.merge_interactor_aggregates(
                updated_users_table,
                self.user_posts_table,
                fbsync.INTERACTOR_AGGREGATES_TABLE,
                engine
            )

            results = engine.execute(
                "select * from {}".format(fbsync.INTERACTOR_AGGREGATES_TABLE)
            )
            for row in results:
                (fbid, num_p, num_i_like, num_i_comm, num_shared, num_friends) = row
                if fbid == 1:
                    self.assertEqual(num_p, 1)
                    self.assertEqual(num_shared, 1)
                    self.assertEqual(num_i_like, 1)
                    self.assertEqual(num_i_comm, 0)
                    self.assertEqual(num_friends, 1)
                else:
                    self.assertEqual(num_p, 3)
                    self.assertEqual(num_shared, 3)
                    self.assertEqual(num_i_like, 2)
                    self.assertEqual(num_i_comm, 1)
                    self.assertEqual(num_friends, 1)
        finally:
            engine.execute('DROP TABLE {}'.format(updated_users_table))

    def test_merge_poster_aggregates(self):
        updated_users_table = 'updated'
        with engine.connect() as connection:
            connection.execute('truncate {}'.format(fbsync.POSTER_AGGREGATES_TABLE))
            connection.execute('create table {} (fbid_poster bigint)'.format(updated_users_table))
            connection.execute('insert into {} values (1)'.format(updated_users_table))
            connection.execute('insert into {} values (2)'.format(updated_users_table))

            self.insertDummyUserPost(connection, 1, 99, poster_id=1, like=True, table=self.user_posts_table)
            self.insertDummyUserPost(connection, 1, 98, poster_id=1, like=True, comm=True, table=self.user_posts_table)
            self.insertDummyUserPost(connection, 2, 99, poster_id=2, like=True, table=self.user_posts_table)
            self.insertDummyUserPost(connection, 3, 99, poster_id=2, table=self.user_posts_table)

        try:
            fbsync.merge_poster_aggregates(
                updated_users_table,
                self.user_posts_table,
                fbsync.POSTER_AGGREGATES_TABLE,
                engine
            )

            results = engine.execute(
                "select * from {}".format(fbsync.POSTER_AGGREGATES_TABLE)
            )
            for row in results:
                print row
                (fbid, posts, like, comm, share, num_friends) = row
                if fbid == 1:
                    self.assertEqual(posts, 1)
                    self.assertEqual(share, 1)
                    self.assertEqual(like, 1)
                    self.assertEqual(comm, 1)
                    self.assertEqual(num_friends, 2)
                else:
                    self.assertEqual(posts, 2)
                    self.assertEqual(share, 2)
                    self.assertEqual(like, 1)
                    self.assertEqual(comm, 0)
                    self.assertEqual(num_friends, 1)
        finally:
            engine.execute('DROP TABLE {}'.format(updated_users_table))

    # more of an integration test, minus the s3 stuff of course
    @patch('forklift.db.utils.optimize')
    @patch('forklift.db.utils.load_from_s3')
    def test_add_new_data(self, load_mock, optimize_mock):
        first_post_id = '1_2'
        first_user = '1'

        with engine.connect() as connection:
            # 1. pre-existing data
            self.__class__.insertDummyPost(connection, table=self.posts_table, post_id=first_post_id)
            self.insertDummyUserPost(connection, first_post_id, first_user, table=self.user_posts_table)

        second_post_id = '2_3'
        # 2. insert data with duplicates into the raw table
        def fake_load_from_s3(connection, bucket_name, key_name, table_name, create_statement=None):
            if table_name.startswith('posts'):
                self.__class__.insertDummyPost(connection, table=fbsync.raw_table_name(self.posts_incremental_table), post_id=second_post_id)
                self.__class__.insertDummyPost(connection, table=fbsync.raw_table_name(self.posts_incremental_table), post_id=second_post_id)
            else:
                self.insertDummyUserPost(connection, second_post_id, first_user, table=fbsync.raw_table_name(self.user_posts_incremental_table))
                self.insertDummyUserPost(connection, second_post_id, first_user, table=fbsync.raw_table_name(self.user_posts_incremental_table))
        load_mock.side_effect = fake_load_from_s3

        # 3. the s3 arguments don't really matter here
        fbsync.add_new_data('sourcefolder','doesntmatter',str(self.version),'1','1','1','1', engine)

        with engine.connect() as connection:
            # 4. verify both tables received correct data
            self.assertEqual(2, get_rowcount(self.posts_table, connection))
            self.assertEqual(2, get_rowcount(self.user_posts_table, connection))

            # 5. and we do want to optimize the tables after loading
            self.assertEqual(optimize_mock.call_count, 8)
