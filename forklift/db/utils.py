import csv
from logging import debug, info, warning
import psycopg2
import tempfile
from sqlalchemy.exc import ProgrammingError
from contextlib import contextmanager
from forklift.s3.utils import key_to_local_file, write_file_to_key
from forklift.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY
import time

BUCKET_NAME = 'warehouse-forklift'

DOES_NOT_EXIST_MESSAGE_TEMPLATE = '"{0}" does not exist'


def table_exists(table, connection):
    result = connection.execute("""
        select 1
        from information_schema.tables
        where table_name = '{}'
    """.format(table))
    return result.first() is not None


# no 'drop table if exists', so just swallow the error
def drop_table_if_exists(table, connection):
    if table_exists(table, connection):
        connection.execute("DROP TABLE {0}".format(table))


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


def get_rowcount(table_name, engine=None, connection=None):
    if engine:
        connection = engine.connect()
    new_row_result = connection.execute('select count(*) from {}'.format(table_name))
    for row in new_row_result:
        return int(row[0])
    if engine:
        connection.close()


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


# promote a staging table with up-to-date data into production by renaming it
# however, we move the existing one out of the way first in case something goes wrong
def deploy_table(table, staging_table, old_table, engine):
    with engine.connect() as connection:
        info('Promoting staging table (%s) to production (%s)', staging_table, table)
        drop_table_if_exists(old_table, connection)

        # swap the staging table with the real one
        if table_exists(table, connection):
            connection.execute("ALTER TABLE {0} rename to {1}".format(table, old_table))

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
    except ProgrammingError:
        info("error loading: \n %s", get_load_errs(connection))
        raise


def unload_to_s3(connection, table_name, bucket_name, key_name, delim="\t"):
    connection.execute("""
        UNLOAD ('select * from {table}') TO 's3://{bucket}/{key}'
        CREDENTIALS 'aws_access_key_id={access};aws_secret_access_key={secret}'
        DELIMITER '{delim}'
    """.format(
        delim=delim,
        table=table_name,
        bucket=bucket_name,
        key=key_name,
        access=AWS_ACCESS_KEY,
        secret=AWS_SECRET_KEY,
    ))


def get_load_errs(connection):
    # see: http://docs.aws.amazon.com/redshift/latest/dg/r_STL_LOAD_ERRORS.html
    sql = "select * from stl_load_errors order by starttime desc limit 2"
    fmt = ": %s\n\t".join(["userid", "slice", "tbl", "starttime", "session", "query", "filename",
                           "line_number", "colname", "type", "col_length", "position", "raw_line",
                           "raw_field_value", "err_code", "err_reason"]) + ": %s\n"
    ret = ""
    for row in connection.execute(sql):
        ret += fmt % tuple(str(field)[:80] for field in row)
    return ret


# VACUUM reclaims diskspace and more importantly re-sorts all rows
# http://docs.aws.amazon.com/redshift/latest/dg/r_VACUUM_command.html
# ANALYZE updates table statistics for use by the query planner
# http://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE.html
# Amazon recommends that you run them both after adding or deleting rows
# to help query speeds
# Needs to be run outside of a transaction
def optimize(table, logger, redshift_engine):
    conn = redshift_engine.raw_connection()
    old_iso_level = conn.isolation_level
    conn.set_isolation_level(0)
    curs = conn.cursor()
    logger.info('vacuuming {}'.format(table))
    curs.execute('vacuum {}'.format(table))
    logger.info('vacuum of {} complete'.format(table))
    logger.info('analyzing {}'.format(table))
    curs.execute('analyze {}'.format(table))
    logger.info('analysis of {} complete'.format(table))
    conn.set_isolation_level(old_iso_level)
    conn.close()


def mysql_redshift_column_map(datatype, max_chars):
    redshift_vals = {
        'int' : 'integer',
        'mediumint': 'bigint',
        'tinyint': 'integer',
        'bigint' : 'bigint',
        'decimal' : 'decimal',
        'real' : 'real',
        'double precision' : 'double precision',
        'boolean' : 'boolean',
        'char' : 'char(50)',  # ehhh.. this is usually ips
        'varchar' : 'varchar(1028)',
        'date' : 'date',
        'timestamp': 'timestamp',
        'datetime': 'timestamp',  # kinda magic
        'longtext': 'varchar',  # .. really magic
        }


    out = redshift_vals[datatype]
    return out

def postgres_column_formatter(datatype, char_length):
    if datatype is 'varchar':
        return "{}({})".format(datatype, char_length)
    else:
        return datatype


def postgres_columns_from_mysql(description):
    return ','.join(
        ' '.join(
            [ columnname, mysql_redshift_column_map(datatype, max_characters) ]
        ) for (columnname, datatype, max_characters) in description
    )

def postgres_columns_from_postgres(description):
    return ','.join(
        ' '.join(
            [ columnname, postgres_column_formatter(datatype, character_max) ]
        ) for (columnname, datatype, character_max) in description
    )


def write2csv(table, connection, file_obj, delim):
    debug('Creating CSV for {}'.format(table))
    l = 20000
    o = 0
    result = connection.execute("select * from {0} limit {1} offset {2}".format(table, l, o))
    writer = csv.writer(file_obj, delimiter=delim)
    rows = result.fetchall()
    writer.writerows(rows)
    while len(rows) > 0:
        o += l
        result = connection.execute("select * from {0} limit {1} offset {2}".format(table, l, o))
        rows = result.fetchall()
        writer.writerows(rows)


def copy_to_redshift(rds_source_engine, redshift_engine, staging_table, final_table, old_table, delim):
    start = time.time()
    file_obj = tempfile.NamedTemporaryFile()

    # get the schema of the table
    columns = None
    csvtime = None
    runcsv = None


    with rds_source_engine.connect() as connection:
        description = connection.execute("""
            select
                column_name,
                data_type,
                character_maximum_length
            from information_schema.columns
            where table_name = '{}'
        """.format(final_table))
        columns = postgres_columns_from_mysql(description)
        write2csv(final_table, connection, file_obj, delim)
        csvtime = time.time()
        runcsv = csvtime - start

    key_name = "rds_sync/{}.csv".format(final_table)

    write_file_to_key(BUCKET_NAME, key_name, file_obj)
    ups3time = time.time()
    runs3 = ups3time - csvtime

    # create the table with the columns query now generated
    info('Creating table {}'.format(staging_table))
    with redshift_engine.connect() as connection:
        drop_table_if_exists(staging_table, connection)
        connection.execute("CREATE TABLE {0} ({1})".format(staging_table, columns))

        # copy the file that we just uploaded to s3 to redshift
        try:
            load_from_s3(
                connection,
                BUCKET_NAME,
                key_name,
                staging_table,
                delim
            )
        except psycopg2.DatabaseError:
            # step through the csv we are about to copy over and change the encodings to work properly with redshift
            warning("Error copying, assuming encoding errors and rewriting CSV...")

            reader = csv.reader(file_obj, delimiter=delim)
            converted_file = tempfile.NamedTemporaryFile()
            writer = csv.writer(converted_file, delimiter=delim)
            keep_going = True
            while keep_going:
                try:
                    this = reader.next()
                    new = [i.decode('latin-1').encode('utf-8') for i in this]
                    writer.writerow(new)
                except StopIteration:
                    keep_going = False

            info("Rewrite complete")
            write_file_to_key(BUCKET_NAME, key_name, converted_file)
            load_from_s3(
                connection,
                BUCKET_NAME,
                key_name,
                staging_table,
                delim
            )

        copytime = time.time()
        runcopy = copytime - ups3time
    deploy_table(final_table, staging_table, old_table, redshift_engine)
    endtime = time.time()
    runswap = endtime - copytime
    runtotal = endtime - start
    info("Successfully copied %s from RDS to S3 to Redshift" % final_table)

    events = [
        ('write csv', runcsv),
        ('write to s3', runs3),
        ('copy from s3 to redshift', runcopy),
        ('swap redshift tables', runswap),
        ('complete entire process', runtotal)
    ]
    info('|'.join("{0:.2f} seconds to {1}".format(duration, event) for event, duration in events))


def copy_from_file(engine, table, file_obj, delim):
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.copy_expert(
        "copy {} from STDIN with DELIMITER '{}'".format(table, delim),
        file_obj
    )
    cursor.close()
    connection.commit()
    connection.close()

def cache_table(redshift_engine, cache_engine, staging_table, final_table, old_table, delim):
    s3_key_name = 'redshift_sync/{}'.format(final_table)
    with redshift_engine.connect() as connection:
        unload_to_s3(
            connection,
            final_table,
            BUCKET_NAME,
            s3_key_name,
            delim,
        )
        result = connection.execute("""
            SELECT
                column_name,
                data_type,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_name='{table}'
            ORDER BY ordinal_position
        """.format(table=final_table))
        columns = postgres_columns_from_postgres(result)

    file_obj = tempfile.NamedTemporaryFile()
    key_to_local_file(
        BUCKET_NAME,
        s3_key_name,
        file_obj
    )

    with cache_engine.connect() as connection:
        drop_table_if_exists(staging_table, connection)
        connection.execute("CREATE TABLE {0} ({1})".format(staging_table, columns))

    copy_from_file(
        cache_engine,
        staging_table,
        file_obj,
        delim
    )

    deploy_table(final_table, staging_table, old_table, cache_engine)
