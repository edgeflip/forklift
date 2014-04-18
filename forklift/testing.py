from unittest import TestCase
from sqlalchemy.orm.session import Session
from forklift.db.base import engine
from forklift.models.base import Base


class ForkliftTestCase(TestCase):
    def setUp(self):
        self.connection = engine.connect()
        Base.metadata.create_all(engine)
        self.__transaction = self.connection.begin_nested()
        self.session = Session(self.connection)

    def tearDown(self):
        self.session.close()
        self.__transaction.rollback()


    def assertSingleResult(self, expected, result):
        num_results = 0
        for row in result:
            num_results += 1
            for (fact_key, expected_count) in expected.items():
                self.assertEqual(row[fact_key], expected_count)
        assert(num_results == 1)

