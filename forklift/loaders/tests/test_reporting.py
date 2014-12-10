import datetime
from mock import patch
import os
from sqlalchemy.orm.session import Session

from forklift.db.base import redshift_engine, rds_cache_engine, rds_source_engine
from forklift.models.raw import Event, Visit, Visitor, Client, Campaign, CampaignProperty, EdgeflipFbid
from forklift.models.base import Base
from forklift.testing import ForkliftTestCase, ForkliftTransactionalTestCase

import forklift.loaders.reporting as reporting

class ReportingTestCase(ForkliftTestCase):

    @classmethod
    def setUpClass(cls):
        super(ReportingTestCase, cls).setUpClass()
        timestamp = datetime.datetime(2014,2,1,2,0)
        cls.event = Event(
            event_id=1,
            event_type='stuff',
            visit_id=1,
            created=timestamp,
            campaign_id=1,
            updated=timestamp,
            event_datetime=timestamp,
        )
        cls.connection = redshift_engine.connect()
        Base.metadata.create_all(redshift_engine)
        cls.session = Session(cls.connection)
        cls.session.merge(cls.event)
        cls.session.commit()


    @classmethod
    def tearDownClass(cls):
        cls.session.rollback()

    def test_refresh_aggregate_table(self):
        test_query = "select count(*) as ec from events where event_id = 1"
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
    def test_process(self, s3_to_fs_mock, rs_to_s3_mock, copy_mock):
        delimiter = ','

        aggregate_queries = (
            ('eventcount', "select count(*) as ec from events where event_id = 1"),
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

class ReportingAggregatesTestCase(ForkliftTestCase):

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
                ],
                'ip': '127.0.0.2',
                'other_campaign': True,
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

    @classmethod
    def setUpClass(cls):
        super(ReportingAggregatesTestCase, cls).setUpClass()
        cls.connection = redshift_engine.connect()
        Base.metadata.create_all(redshift_engine)
        cls.session = Session(cls.connection)
        cls.campaign_id = 6
        cls.other_campaign_id = 7
        cls.hour = datetime.datetime(2014,2,1,2,0)
        cls.in_range = datetime.datetime(2014,2,1,2,30)
        cls.out_of_range = datetime.datetime(2014,2,1,4)

        cls.session.merge(Client(client_id=1))
        cls.session.merge(Campaign(campaign_id=cls.campaign_id, client_id=1))
        cls.session.merge(Campaign(campaign_id=cls.other_campaign_id, client_id=1))
        cls.session.merge(CampaignProperty(
            campaign_property_id=cls.campaign_id,
            campaign_id=cls.campaign_id,
            root_campaign_id=cls.campaign_id,
        ))
        cls.session.merge(CampaignProperty(
            campaign_property_id=cls.other_campaign_id,
            campaign_id=cls.other_campaign_id,
            root_campaign_id=cls.other_campaign_id,
        ))
        cls.session.merge(EdgeflipFbid(fbid=999))

        # explicitly define eventids so even if tearDown doesn't happen, we can
        # merge instead of adding unnecessary data
        unique_id = 1

        for visitor_template in cls.visitor_templates():
            visitor = Visitor(visitor_id=unique_id, fbid=visitor_template['fbid'], created=cls.in_range, updated=cls.in_range)
            unique_id += 1
            cls.session.merge(visitor)
            for visit_template in visitor_template['visits']:
                visit = Visit(visit_id=unique_id, ip=visit_template['ip'], visitor_id=visitor.visitor_id, created=cls.in_range, updated=cls.in_range)
                unique_id += 1
                cls.session.merge(visit)
                for event_template in visit_template['events']:
                    timestamp = cls.out_of_range if 'out_of_range' in event_template else cls.in_range
                    friend_fbid = event_template['friend_fbid'] if 'friend_fbid' in event_template else None
                    campaign_id = cls.other_campaign_id if 'other_campaign' in visit_template else cls.campaign_id
                    event = Event(
                        event_id=unique_id,
                        event_type=event_template['type'],
                        visit_id=visit.visit_id,
                        created=timestamp,
                        campaign_id=campaign_id,
                        updated=timestamp,
                        event_datetime=timestamp,
                        friend_fbid=friend_fbid,
                    )
                    unique_id += 1
                    cls.session.merge(event)

        cls.session.commit()
        for (aggregate_table, aggregate_query) in reporting.AGGREGATE_QUERIES:
            reporting.refresh_aggregate_table(
                cls.connection,
                aggregate_table,
                aggregate_query
            )

    @classmethod
    def tearDownClass(cls):
        cls.session.rollback()

    def test_clientrollups(self):
        result = self.connection.execute(
            "select * from {} where client_id = {}" \
            .format('clientrollups', 1)
        )

        expected = {
            'audience': 2,
            'auth_fails': 0,
            'authorized_visits': 4,
            'clickbacks': 1,
            'clicks': 0,
            'distinct_faces_shown': 5,
            'failed_visits': 2,
            'initial_redirects': 6,
            'total_faces_shown': 6,
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

    def test_campaignrollups(self):
        result = self.connection.execute(
            "select * from campaignrollups where campaign_id = {}" \
            .format(self.campaign_id)
        )

        expected = {
            'audience': 2,
            'auth_fails': 0,
            'authorized_visits': 4,
            'clickbacks': 1,
            'clicks': 0,
            'distinct_faces_shown': 5,
            'failed_visits': 1,
            'initial_redirects': 5,
            'total_faces_shown': 6,
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

    def test_campaignhourly(self):
        result = self.connection.execute(
            "select * from campaignhourly where campaign_id = {} and hour = '{}'" \
            .format(self.campaign_id, self.hour)
        )

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
