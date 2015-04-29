import argparse
import gzip
from itertools import imap
import logging
import multiprocessing
import os
import ssl

import forklift.s3.utils as s3

S3_IN_BUCKET_NAMES = ["user_feeds_%d" % i for i in range(5)] + \
    ["feed_crawler_%d" % i for i in range(5)]
OUT_PATH = '/home/tristan/compressiontest'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


def s3_key_iter_bucket(bucket_name, retries=5):
    conn_s3 = s3.get_conn_s3()
    buck_list = conn_s3.get_bucket(bucket_name).list()
    key_name_prev = ''
    while True:
        try:
            for key in buck_list:
                yield key
                key_name_prev = key.name
        except ssl.SSLError:
            logger.debug(
                "pid " + str(os.getpid()) + " SSLError exception, reconnecting"
            )
            for r in range(retries):
                conn_s3 = s3.get_conn_s3()
                buck_list = conn_s3.get_bucket(bucket_name).list(
                    marker=key_name_prev
                )
                try:
                    yield key
                    key_name_prev = key.name
                except ssl.SSLError:
                    logger.debug("pid %d retry %d failure" % (os.getpid(), r))
                    continue
                logger.debug("pid %d retry %d success" % (os.getpid(), r))
                break


def s3_key_iter(bucket_names=S3_IN_BUCKET_NAMES):
    for bucket_index, bucket_name in enumerate(bucket_names):
        logger.debug("reading bucket %d/%d (%s)" % (
            bucket_index,
            len(bucket_names),
            bucket_name
        ))
        for key in s3_key_iter_bucket(bucket_name):
            yield key, bucket_name


conn_s3_global = None


def set_global_conns():
    global conn_s3_global
    conn_s3_global = s3.get_conn_s3()


def handle_feed_s3(args, retries=1):
    key, bucket_name = args[0]
    pid = os.getpid()
    logger.debug("pid " + str(pid) + ", key " + key.name)

    f = gzip.open(
        '{}/{}/{}.json.gz'.format(OUT_PATH, bucket_name, key.name), 'wb'
    )
    key.get_contents_to_file(f)
    f.close()


def process_feeds(worker_count, max_feeds):
    conn_s3 = s3.get_conn_s3()

    logger.info(
        "process %d farming out to %d childs" % (os.getpid(), worker_count)
    )
    pool = multiprocessing.Pool(
        processes=worker_count,
        initializer=set_global_conns
    )

    feed_arg_iter = imap(None, s3_key_iter())
    for i, out_file_names in enumerate(
        pool.imap_unordered(handle_feed_s3, feed_arg_iter)
    ):
        if (max_feeds is not None) and (i >= max_feeds):
            logging.debug("bailing")
            break
    pool.terminate()
    conn_s3.close()
    return i

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Eat up the FB sync json and compress it onto a hard drive'
    )
    parser.add_argument(
        '--workers',
        type=int,
        help='number of workers to multiprocess',
        default=2
    )
    parser.add_argument(
        '--maxfeeds',
        type=int,
        help='bail after x feeds are done',
        default=24
    )
    parser.add_argument(
        '--logfile',
        type=str,
        help='for debugging',
        default=None
    )
    args = parser.parse_args()

    hand_s = logging.StreamHandler()
    hand_s.setFormatter(logging.Formatter('%(asctime)s %(message)s'))

    if args.logfile is None:
        hand_s.setLevel(logging.DEBUG)
    else:
        hand_s.setLevel(logging.INFO)
        hand_f = logging.FileHandler(args.logfile)
        hand_f.setFormatter(logging.Formatter(
            '%(levelname)s %(asctime)s %(process)d %(thread)d %(message)s'
        ))
        hand_f.setLevel(logging.DEBUG)
        logger.addHandler(hand_f)
    logger.addHandler(hand_s)

    for bucket_name in S3_IN_BUCKET_NAMES:
        directory = "{}/{}".format(OUT_PATH, bucket_name)
        if not os.path.exists(directory):
            os.makedirs(directory)

    process_feeds(args.workers, args.maxfeeds)
