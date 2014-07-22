from boto.s3.connection import S3Connection
from boto.s3.key import Key
import logging

from forklift.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY

logger = logging.getLogger(__name__)


def get_conn_s3(key=AWS_ACCESS_KEY, sec=AWS_SECRET_KEY):
    return S3Connection(key, sec)


def move_file(bucket_name, key_name, new_directory):
    buck = get_conn_s3().get_bucket(bucket_name)
    buck.copy_key(os.path.join(new_directory, key_name), bucket_name, key_name)
    buck.delete_key(key_name)

def delete_s3_bucket(conn_s3, bucket_name):
    buck = conn_s3.get_bucket(bucket_name)
    for key in buck.list():
        key.delete()
    conn_s3.delete_bucket(bucket_name)

def create_s3_bucket(conn_s3, bucket_name, overwrite=False):
    if conn_s3.lookup(bucket_name) is not None:
        if overwrite:
            logger.debug("deleting old S3 bucket " + bucket_name)
            delete_s3_bucket(conn_s3, bucket_name)
        else:
            logger.debug("keeping old S3 bucket " + bucket_name)
            return None
    logger.debug("creating S3 bucket " + bucket_name)
    return conn_s3.create_bucket(bucket_name)

