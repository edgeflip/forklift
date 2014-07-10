from logging import debug, info
from sqlalchemy.exc import ProgrammingError
from contextlib import contextmanager
from forklift.db.base import engine
from forklift.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


DOES_NOT_EXIST_MESSAGE_TEMPLATE = '"{0}" does not exist'

# no 'drop table if exists', so just swallow the error
def drop_table_if_exists(table, connection):
    try:
        with connection.begin():
            debug('Dropping table {}'.format(table))
            connection.execute("DROP TABLE {0}".format(table))
    except ProgrammingError as e:
        if DOES_NOT_EXIST_MESSAGE_TEMPLATE.format(table) in str(e):
            debug("Table {0} did not exist, so no dropping was necessary.".format(table))
        else:
            raise


def create_new_table(new_table_name, target_table_name, connection, temp=False):
    drop_table_if_exists(new_table_name, connection)
    prefix = 'temp ' if temp else ''
    connection.execute(
        'create {}table {} (like {})'.format(prefix, new_table_name, target_table_name)
    )


def create_temporary_table(new_table_name, target_table_name, connection):
    create_new_table(new_table_name, target_table_name, connection, temp=True)


def drop_table(table_name, connection):
    connection.execute(
        'drop table {}'.format(table_name)
    )


def get_rowcount(connection, table):
    new_row_result = connection.execute('select count(*) from {}'.format(table))
    for row in new_row_result:
        return int(row[0])


@contextmanager
def staging_table(destination_table_name, connection):
    staging_table_name = destination_table_name + '_staging'
    create_temporary_table(staging_table_name, destination_table_name, connection)
    yield staging_table_name
    drop_table(staging_table_name, connection)


@contextmanager
def checkout_connection():
    connection = engine.connect()
    try:
        with connection.begin():
            yield connection
    finally:
        connection.close()


@contextmanager
def checkout_raw_connection():
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()


# promote a staging table with up-to-date data into production by renaming it
# however, we move the existing one out of the way first in case something goes wrong
def deploy_table(table, staging_table, old_table, connection):
    info('Promoting staging table ({}) to production ({})'.format(staging_table, table))
    drop_table_if_exists(old_table, connection)

    # swap the staging table with the real one
    with connection.begin() as transaction:
        try:
            connection.execute("ALTER TABLE {0} rename to {1}".format(table, old_table))
        except ProgrammingError as e:
            if DOES_NOT_EXIST_MESSAGE_TEMPLATE.format(table) in str(e):
                info("Table {0} did not exist, so no renaming was necessary.".format(table))
                # roll back the transaction so the second alter can still commence
                transaction.rollback()
            else:
                raise

        connection.execute("ALTER TABLE {0} rename to {1}".format(staging_table, table))

    drop_table_if_exists(old_table, connection)


def load_from_s3(connection, bucket_name, key_name, table_name, delim="\t", create_statement=None):
    try:
        if create_statement:
            drop_table_if_exists(table_name, connection)
            connection.execute(create_statement)
        connection.execute("""
            COPY {table} FROM 's3://{bucket}/{key}'
            CREDENTIALS 'aws_access_key_id={access};aws_secret_access_key={secret}'
            DELIMITER '{delim}' TRUNCATECOLUMNS ACCEPTINVCHARS NULL AS '\000' IGNOREBLANKLINES
        """.format(
                delim=delim,
                table=table_name,
                bucket=bucket_name,
                key=key_name,
                access=AWS_ACCESS_KEY_ID,
                secret=AWS_SECRET_ACCESS_KEY,
            )
        )
    except ProgrammingError as e:
        #info("error loading: \n" + get_load_errs(connection))
        info("error loading: \n")
        raise


def get_load_errs(connection):
    # see: http://docs.aws.amazon.com/redshift/latest/dg/r_STL_LOAD_ERRORS.html
    sql = "select * from stl_load_errors order by starttime desc limit 2"
    fmt = ": %s\n\t".join(["userid", "slice", "tbl", "starttime", "session", "query", "filename",
                           "line_number", "colname", "type", "col_length", "position", "raw_line",
                           "raw_field_value", "err_code", "err_reason"]) + ": %s\n"
    ret = ""
    for row in connection.execute(sql):
        ret += fmt % tuple([str(field)[:80] for field in row])
    return ret


def optimize(connection, table):
    connection.execute("VACUUM {}".format(table))
    connection.execute("ANALYZE {}".format(table))

