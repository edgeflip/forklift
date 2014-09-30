import datetime
from forklift.db.base import redshift_engine
from forklift.db.utils import drop_table_if_exists, get_rowcount
from forklift.models.raw import Event
from sqlalchemy.orm.session import Session
from forklift.models.base import Base
from forklift.testing import ForkliftTestCase

import forklift.loaders.reporting as reporting

class ReportingTestCase(ForkliftTestCase):

    def setUp(self):
        super(ReportingTestCase, self).setUp()
        timestamp = datetime.datetime(2014,2,1,2,0)
        event = Event(
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
        self.session.add(event)
        self.session.commit()

    def tearDown(self):
        self.connection.execute('delete from events');
        self.connection.close()

    def test_refresh_aggregate_table(self):
        test_query = 'select count(*) as ec from events'
        test_tablename = 'eventcount'
        reporting.refresh_aggregate_table(
            self.connection,
            test_tablename,
            test_query
        )
        rows = get_rowcount(test_tablename, self.connection)
        self.assertEquals(rows, 1)
