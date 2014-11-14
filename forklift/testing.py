from unittest import TestCase
from mock import patch
from sqlalchemy.orm.session import Session
from sqlalchemy.engine import Connection
import faraday
from forklift.db.base import redshift_engine as engine
from forklift.models.base import Base


# Don't run things in a nested transaction
# because maybe we're doing DDL stuff in the tests and want to control
# rollbacks more granularly
class ForkliftTestMixin(object):

    @classmethod
    def setUpClass(cls):
        global_patches = (
            patch.multiple(
                faraday.conf.settings,
                PREFIX='test',
                LOCAL_ENDPOINT='localhost:4646',
            ),
        )
        for patch_ in global_patches:
            patch_.start()

    @classmethod
    def tearDownClass(cls):
        patch.stopall()

    def assertSingleResult(self, expected, result):
        num_results = 0
        for row in result:
            num_results += 1
            for (fact_key, expected_count) in expected.items():
                self.assertEqual(row[fact_key], expected_count)
        self.assertEquals(num_results, 1)


class ForkliftTestCase(ForkliftTestMixin, TestCase):
    pass


# The standard test case, run the whole thing in a nested transaction
# and roll back at the end
class ForkliftTransactionalTestCase(ForkliftTestCase):


    @classmethod
    def setUpClass(cls):
        super(ForkliftTransactionalTestCase, cls).setUpClass()
        cls.connection = engine.connect()
        Base.metadata.create_all(engine)
        cls.__transaction = cls.connection.begin_nested()
        cls.session = Session(cls.connection)
        global_patches = (
            patch('sqlalchemy.engine.Connection.begin', Connection.begin_nested),
        )
        for patch_ in global_patches:
            patch_.start()

    @classmethod
    def tearDownClass(cls):
        patch.stopall()
        cls.session.close()
        cls.__transaction.rollback()
