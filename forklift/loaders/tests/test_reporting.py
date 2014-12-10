import datetime
from mock import patch
import os
import re
from sqlalchemy.orm.session import Session

from forklift.db.base import redshift_engine, rds_cache_engine, rds_source_engine
from forklift.models.raw import Event, Visit, Visitor, Client, Campaign, CampaignProperty, EdgeflipFbid
from forklift.models.base import Base
from forklift.testing import ForkliftTestCase, ForkliftTransactionalTestCase

import forklift.loaders.reporting as reporting

class ReportingTestCase(ForkliftTestCase):

    def setUp(self):
        super(ReportingTestCase, self).setUp()
        timestamp = datetime.datetime(2014,2,1,2,0)
        self.event = Event(
            event_type='stuff',
            visit_id=1,
            created=timestamp,
            campaign_id=1,
            updated=timestamp,
            event_datetime=timestamp,
        )
        self.connection = redshift_engine.connect()
        Base.metadata.create_all(redshift_engine)
        self.session = Session(self.connection)
        self.session.add(self.event)
        self.session.commit()


    def tearDown(self):
        self.connection.execute('delete from events');
        self.connection.close()

    def xtest_refresh_aggregate_table(self):
        test_query = 'select count(*) as ec from events'
        test_tablename = 'eventcount'
        reporting.refresh_aggregate_table(
            redshift_engine,
            test_tablename,
            test_query
        )
        self.assertSingleResult(
            { 'ec': 1 },
            self.connection.execute('select ec from {}'.format(test_tablename))
        )

    @patch('forklift.loaders.reporting.copy_to_redshift')
    @patch('forklift.db.utils.unload_to_s3')
    @patch('forklift.db.utils.key_to_local_file')
    def xtest_process(self, s3_to_fs_mock, rs_to_s3_mock, copy_mock):
        delimiter = ','

        aggregate_queries = (
            ('eventcount', 'select count(*) as ec from events'),
        )

        raw_tables = {
            'events': 'event_id',
        }

        cached_raw_tables = []

        def fake_s3_to_fs(bucket_name, s3_key_name, file_obj):
            from_curs = redshift_engine.raw_connection().cursor()
            from_curs.copy_expert(
                "COPY eventcount TO STDOUT with DELIMITER '{}'".format(delimiter),
                file_obj
            )
            file_obj.seek(0, os.SEEK_SET)


        s3_to_fs_mock.side_effect = fake_s3_to_fs

        reporting.process(
            rds_source_engine,
            redshift_engine,
            rds_cache_engine,
            delim=delimiter,
            raw_tables=raw_tables,
            aggregate_queries=aggregate_queries,
            cached_raw_tables=cached_raw_tables,
        )
        self.assertSingleResult(
            { 'ec': 1 },
            rds_cache_engine.connect().execute(
                'select ec from {}'.format(aggregate_queries[0][0])
            )
        )

class ReportingNewTestCase(ForkliftTransactionalTestCase):

    @classmethod
    def visitor_templates(cls):
        return [{
            'fbid': 4,
            'visits': [{
                'events': [
                    {'type': 'initial_redirect'},
                    {'type': 'incoming_redirect'},
                    {'type': 'authorized'},
                    {'type': 'generated'},
                    {'type': 'shown', 'friend_fbid': 997},
                    {'type': 'faces_page_rendered'},
                ],
                'ip': '127.0.0.1',
            }, {
                'events': [
                    {'type': 'initial_redirect'},
                    {'type': 'incoming_redirect'},
                    {'type': 'authorized'},
                    {'type': 'generated', 'out_of_range': True},
                    {'type': 'faces_page_rendered', 'out_of_range': True},
                    {'type': 'shown', 'friend_fbid': 998, 'out_of_range': True},
                ],
                'ip': '8.8.8.8',
            }],
        }, {
            'fbid': 6,
            'visits': [{
                'events': [
                    {'type': 'initial_redirect'},
                    {'type': 'incoming_redirect'},
                    {'type': 'authorized'},
                    {'type': 'generated'},
                    {'type': 'shown', 'friend_fbid': 999},
                    {'type': 'shown', 'friend_fbid': 1000},
                    {'type': 'shown', 'friend_fbid': 1001},
                    {'type': 'faces_page_rendered'},
                    {'type': 'share_click'},
                    {'type': 'shared', 'friend_fbid': 999},
                    {'type': 'shared', 'friend_fbid': 1000},
                    {'type': 'clickback', 'friend_fbid': 1000},
                ],
                'ip': '192.168.1.188',
            }],
        }, {
            'fbid': 7,
            'visits': [{
                'events': [
                    {'type': 'initial_redirect'},
                ],
                'ip': '123.45.67.89',
            }],
        }, {
            'fbid': 8,
            'visits': [{
                'events': [
                    {'type': 'initial_redirect'},
                    {'type': 'incoming_redirect'},
                    {'type': 'authorized'},
                    {'type': 'generated'},
                    {'type': 'shown', 'friend_fbid': 1001},
                    {'type': 'faces_page_rendered'},
                    {'type': 'share_click'},
                ],
                'ip': '192.168.1.188',
            }],
        }]

    def setUp(self):
        super(ReportingNewTestCase, self).setUp()
        self.connection.execute('delete from events')
        self.connection.execute('delete from visits')
        self.connection.execute('delete from visitors')
        self.campaign_id = 6
        self.hour = datetime.datetime(2014,2,1,2,0)
        self.in_range = datetime.datetime(2014,2,1,2,30)
        self.out_of_range = datetime.datetime(2014,2,1,4)

        self.session.merge(Client(client_id=1))
        self.session.merge(Campaign(campaign_id=self.campaign_id, client_id=1))
        self.session.merge(CampaignProperty(campaign_id=self.campaign_id, root_campaign_id=self.campaign_id))
        self.session.merge(EdgeflipFbid(fbid=999))
        self.session.commit()

        for visitor_template in self.visitor_templates():
            visitor = Visitor(fbid=visitor_template['fbid'], created=self.in_range, updated=self.in_range)
            self.session.add(visitor)
            self.session.commit()
            for visit_template in visitor_template['visits']:
                visit = Visit(ip=visit_template['ip'], visitor_id=visitor.visitor_id, created=self.in_range, updated=self.in_range)
                self.session.add(visit)
                self.session.commit()
                for event_template in visit_template['events']:
                    timestamp = self.out_of_range if 'out_of_range' in event_template else self.in_range
                    friend_fbid = event_template['friend_fbid'] if 'friend_fbid' in event_template else None
                    event = Event(
                        event_type=event_template['type'],
                        visit_id=visit.visit_id,
                        created=timestamp,
                        campaign_id=self.campaign_id,
                        updated=timestamp,
                        event_datetime=timestamp,
                        friend_fbid=friend_fbid,
                    )
                    self.session.add(event)
                    self.session.commit()

    def tearDown(self):
        self.connection.execute('delete from events')
        self.connection.execute('delete from visits')
        self.connection.execute('delete from visitors')
        self.connection.close()

    def test_aggregate_queries(self):
        for (aggregate_table, aggregate_query) in reporting.AGGREGATE_QUERIES:
            reporting.refresh_aggregate_table(
                self.connection,
                aggregate_table,
                aggregate_query
            )

        result = self.connection.execute("""
            select * from {} where campaign_id = {} and hour = '{}'
        """.format('campaignhourly', self.campaign_id, self.hour))

        expected = {
            'audience': 2,
            'auth_fails': 0,
            'authorized_visits': 4,
            'clickbacks': 1,
            'clicks': 0,
            'distinct_faces_shown': 4,
            'failed_visits': 1,
            'initial_redirects': 5,
            'total_faces_shown': 5,
            'total_shares': 2,
            'uniq_users_authorized': 3,
            'users_facepage_rendered': 3,
            'users_generated_faces': 3,
            'users_shown_faces': 3,
            'users_who_shared': 1,
            'visits': 4,
            'visits_facepage_rendered': 4,
            'visits_generated_faces': 4,
            'visits_shown_faces': 4,
            'visits_with_share_clicks': 2,
            'visits_with_shares': 1,
        }

        self.assertSingleResult(expected, result)
