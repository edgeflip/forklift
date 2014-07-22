from logging import debug
from sqlalchemy.exc import ProgrammingError
from contextlib import contextmanager
from forklift.db.base import engine
from forklift.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY


DOES_NOT_EXIST_MESSAGE_TEMPLATE = '"{0}" does not exist'

# no 'drop table if exists', so just swallow the error
def drop_table_if_exists(table, connection):
    try:
        with connection.begin():
            debug('Dropping table {}'.format(table))
            connection.execute("DROP TABLE {0}".format(table))
    except ProgrammingError as e:
        if DOES_NOT_EXIST_MESSAGE_TEMPLATE.format(table) in str(e):
            debug("Table {0} did not exist, so no dropping was performed.".format(table))
        else:
            raise


def create_temporary_table(temporary_table_name, target_table_name, connection):
    drop_table_if_exists(temporary_table_name, connection)
    connection.execute(
        'create temp table {} (like {})'.format(temporary_table_name, target_table_name)
    )


def drop_table(table_name, connection):
    connection.execute(
        'drop table {}'.format(table_name)
    )


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


def load_from_s3(connection, bucket_name, key_name, table_name, dest_bucket, delim="\t"):
    connection.execute("""
        COPY {table} FROM 's3://{bucket}/{key}'
        CREDENTIALS 'aws_access_key_id={access};aws_secret_access_key={secret}'
        DELIMITER '{delim}' TRUNCATECOLUMNS ACCEPTINVCHARS NULL AS '\\000' IGNOREBLANKLINES
    """.format(
            delim=delim,
            table=table_name,
            bucket=bucket_name,
            key=key_name,
            access=AWS_ACCESS_KEY,
            secret=AWS_SECRET_KEY,
        )
    )
