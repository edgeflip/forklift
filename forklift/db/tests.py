from nose.tools import assert_raises
from sqlalchemy.exc import ProgrammingError
from forklift.testing import ForkliftTestCase
from forklift.db.utils import staging_table

class DbTestCase(ForkliftTestCase):
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

        assert_raises(ProgrammingError, self.connection.execute, 'select * from {}'.format(save_staging_table_name))
