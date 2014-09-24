from boto.s3.connection import S3Connection
from boto.s3.key import Key
import logging
import os
import ssl
from forklift.nlp.utils import ReservoirSampler

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


def stream_batched_files_from(bucket_names, batch_size):
    feeds_in_batch = 0
    feed_batch = []
    for feed in stream_files_from(bucket_names):
        if feeds_in_batch < batch_size:
            feed_batch.append(feed)
            feeds_in_batch += 1
        else:
            feeds_in_batch = 0
            yield feed_batch
            feed_batch = []
    if feeds_in_batch > 0:
        yield feed_batch


def stream_files_from(bucket_names):
    conn_s3 = get_conn_s3()
    for b, bucket_name in enumerate(bucket_names):
        logger.debug(
            "reading bucket %d/%d (%s)" % (b, len(bucket_names), bucket_name)
        )
        keys = conn_s3.get_bucket(bucket_name).list()
        for key in keys:
            yield key
    conn_s3.close()


def write_string_to_key(bucket, key_name, string):
    key = Key(bucket)
    key.key = key_name
    key.set_contents_from_string(string + "\n")
    key.close()


def key_to_local_file(bucket_name, s3_key_name, file_obj):
    s3_conn = get_conn_s3()
    bucket = s3_conn.get_bucket(bucket_name)
    key = bucket.get_key(s3_key_name)
    key.get_contents_to_file(file_obj)


def write_filename_to_key(bucket_name, filename):
    s3conn = get_conn_s3()
    red = s3conn.get_bucket(bucket_name)

    k = red.new_key(filename)
    k.set_contents_from_filename('%s.csv' % filename)
    logger.info("Uploaded %s to s3" % filename)


class S3ReservoirSampler(ReservoirSampler):
    def generator(self, bucket_name):
        conn_s3 = get_conn_s3()
        m = self.prev_value.name if self.prev_value else ''
        return conn_s3.get_bucket(bucket_name).list(
            marker=m
        )

    def sampling_bucket(self, bucket_name):
        max_retries = 10
        retries = 0
        while max_retries > retries:
            try:
                return self.sampling(self.generator(bucket_name))
            except ssl.SSLError:
                print "SSLError, reconnecting"
                retries += 1
