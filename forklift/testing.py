from unittest import TestCase
from mock import patch
from sqlalchemy.orm.session import Session
from sqlalchemy.engine import Connection
from forklift.db.base import engine
from forklift.models.base import Base


class ForkliftTestCase(TestCase):
    global_patches = (
        patch('sqlalchemy.engine.Connection.begin', Connection.begin_nested),
    )


    @classmethod
    def setUpClass(cls):
        cls.connection = engine.connect()
        Base.metadata.create_all(engine)
        cls.__transaction = cls.connection.begin_nested()
        cls.session = Session(cls.connection)
        for patch_ in cls.global_patches:
            patch_.start()

    @classmethod
    def tearDownClass(cls):
        for patch_ in cls.global_patches:
            patch_.stop()
        cls.session.close()
        cls.__transaction.rollback()


    def assertSingleResult(self, expected, result):
        num_results = 0
        for row in result:
            num_results += 1
            for (fact_key, expected_count) in expected.items():
                self.assertEqual(row[fact_key], expected_count)
        assert(num_results == 1)

