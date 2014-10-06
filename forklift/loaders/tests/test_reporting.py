import datetime
from mock import patch
import os
from sqlalchemy.orm.session import Session
import tempfile

from forklift.db.base import redshift_engine, rds_cache_engine, rds_source_engine
from forklift.models.raw import Event
from forklift.models.base import Base
from forklift.testing import ForkliftTestCase
from forklift.db.utils import get_rowcount

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

    def test_refresh_aggregate_table(self):
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
    def test_process(self, s3_to_fs_mock, rs_to_s3_mock, copy_mock):
        delimiter = ','

        reporting.AGGREGATES = {
            'eventcount': 'select count(*) as ec from events',
        }

        reporting.RAW_TABLES = {
            'events': 'event_id',
        }

        def fake_s3_to_fs(bucket_name, s3_key_name, file_obj):
            from_curs = redshift_engine.raw_connection().cursor()
            from_curs.copy_expert(
                "COPY eventcount TO STDOUT with DELIMITER '{}'".format(delimiter),
                file_obj
            )
            file_obj.seek(0, os.SEEK_SET)


        s3_to_fs_mock.side_effect = fake_s3_to_fs

        reporting.process(rds_source_engine, redshift_engine, rds_cache_engine)
        self.assertSingleResult(
            { 'ec': 1 },
            rds_cache_engine.connect().execute(
                'select ec from {}'.format(reporting.AGGREGATES.keys()[0])
            )
        )

