import datetime
from sqlalchemy import Integer, BigInteger, DateTime, String
from sqlalchemy.exc import ProgrammingError

from forklift.db.utils import staging_table, drop_table_if_exists
from forklift.models.base import Base
from forklift.models.raw import Event, Visit, Visitor
from forklift.warehouse.definition import HourlyAggregateTable
from forklift.warehouse.columns import Fact, Dimension
import forklift.loaders.fact.hourly as loaders

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


class HourlyLoaderTestCase(LoaderTestCase):
    loader = TestLoader()
    def setUp(self):
        super(HourlyLoaderTestCase, self).setUp()
        self.destination_table = 'test_facts_hourly'
        self.inner_transaction = self.connection.begin_nested()
        self.connection.execute("""
            create table {} (
                hour timestamp,
                campaign_id integer,
                event_id integer,
                donated integer
            )
        """.format(self.destination_table))

    def tearDown(self):
        self.inner_transaction.rollback()

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



class FbidFactLoaderTestCase(LoaderTestCase):
    def test_load(self):
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


class VisitFactLoaderTestCase(LoaderTestCase):
    def test_load(self):
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


class IpFactLoaderTestCase(LoaderTestCase):
    def test_stage_hour(self):
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


class FriendFbidFactLoaderTestCase(LoaderTestCase):
    def test_stage_hour(self):
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


class MiscFactLoaderTestCase(LoaderTestCase):
    def test_stage_hour(self):
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
