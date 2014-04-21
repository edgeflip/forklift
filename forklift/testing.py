from unittest import TestCase
from sqlalchemy.orm.session import Session
from forklift.db.base import engine
from forklift.models.base import Base


class ForkliftTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.connection = engine.connect()
        Base.metadata.create_all(engine)
        cls.__transaction = cls.connection.begin_nested()
        cls.session = Session(cls.connection)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        cls.__transaction.rollback()


    def setUp(self):
        self.connection = self.__class__.connection


    def assertSingleResult(self, expected, result):
        num_results = 0
        for row in result:
            num_results += 1
            for (fact_key, expected_count) in expected.items():
                self.assertEqual(row[fact_key], expected_count)
        assert(num_results == 1)

