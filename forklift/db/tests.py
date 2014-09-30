from sqlalchemy.exc import ProgrammingError
from forklift.testing import ForkliftTestCase, ForkliftTransactionalTestCase
from forklift.db.utils import staging_table, load_from_s3, deploy_table, drop_table_if_exists
from mock import Mock, patch
from forklift.db.base import redshift_engine

class DbTestCase(ForkliftTestCase):
    def test_deploy_table(self):
        first_table = 'test1'
        second_table = 'test2'
        def assert_result_is(result, connection):
            self.assertSingleResult({'test': result}, connection.execute("SELECT * FROM {}".format(first_table)))

        with redshift_engine.connect() as connection:
            drop_table_if_exists(first_table, connection)
            drop_table_if_exists(first_table, connection)
            connection.execute("CREATE TABLE {} AS SELECT 1 AS test".format(first_table))
            connection.execute("CREATE TABLE {} AS SELECT 2 AS test".format(second_table))
            assert_result_is(1, connection)
            deploy_table(first_table, second_table, 'test_old', connection)
            assert_result_is(2, connection)


    def test_load_from_s3(self):
        connectionMock = Mock()
        expected_bucket = 'test_bucket'
        expected_key = 'test_key'
        expected_table = 'test_table'
        load_from_s3(connectionMock, expected_bucket, expected_key, expected_table)
        self.assertEqual(connectionMock.execute.call_count, 1)
        query = connectionMock.execute.call_args[0][0]
        self.assertIn(
            "COPY {} FROM 's3://{}/{}'".format(expected_table, expected_bucket, expected_key),
            query
        )


    @patch('forklift.db.utils.drop_table_if_exists')
    def test_load_from_s3_with_create(self, drop_table_mock):
        connectionMock = Mock()
        create = 'test query'
        load_from_s3(connectionMock, 'bucket', 'key', 'table', create_statement=create)
        self.assertEqual(connectionMock.execute.call_count, 2)
        connectionMock.execute.assert_any_call(create)
        self.assertEqual(drop_table_mock.call_count, 1)



class StagingTableTestCase(ForkliftTransactionalTestCase):
    def test_staging_table(self):
        real_table = 'test_real_table'
        self.connection.execute('create table {} (col1 integer, col2 integer)'.format(real_table))
        save_staging_table_name = None
        with staging_table(real_table, self.connection) as staging_table_name:
            save_staging_table_name = staging_table_name
            self.connection.execute('insert into {} values (1, 1)'.format(staging_table_name))
            self.assertSingleResult(
                {'col1': 1, 'col2': 1},
                self.connection.execute('select * from {}'.format(staging_table_name))
            )

        with self.assertRaises(ProgrammingError):
            self.connection.execute('select * from {}'.format(save_staging_table_name))
