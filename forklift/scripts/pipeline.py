from boto.dynamodb.layer2 import Layer2
from boto.dynamodb.condition import IN, BETWEEN
from boto.s3.key import Key
from forklift.loaders.fbsync import FeedChunk, POSTS, LINKS, LIKES, TOP_WORDS, add_new_data
from forklift.db.base import redshift_engine
from forklift.utils import batcher
from forklift.s3.utils import get_conn_s3
from forklift.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY
from itertools import imap, repeat
import forklift.nlp.tfidf as tfidf
import argparse
import os
import time
import logging
import multiprocessing
import uuid

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False

BATCH_SIZE = 50
S3_OUT_BUCKET_NAME = "warehouse-forklift"
HOURS_BACK = 24
COMMON_OUTPUT_PREFIX = "transfer_batches"
POSTS_FOLDER = 'posts'
LINKS_FOLDER = 'links'
LIKES_FOLDER = 'likes'
TOP_WORDS_FOLDER = 'top_words'


def stream_files_between(start, end):
    querier = Layer2(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    table = querier.get_table('prod.fb_sync_maps')
    logger.info("Scanning table {} for files between {} and {}".format(
        table.name,
        start,
        end
    ))
    return (
        (item['bucket'], item['fbid_primary'], item['fbid_secondary'])
        for item in querier.scan(
            table,
            attributes_to_get=[
                'updated',
                'status',
                'fbid_primary',
                'fbid_secondary',
                'bucket'
            ],
            scan_filter={
                'updated': BETWEEN(start, end),
                'status': IN([
                    'back_fill',
                    'page_likes',
                    'comment_crawl',
                    'complete'
                ]),
            }
        )
    )


conn_s3_global = None
vectorizer = None


def worker_setup(vocab, idf):
    global conn_s3_global
    conn_s3_global = get_conn_s3()

    global vectorizer
    vectorizer = tfidf.bootstrap_trained_vectorizer(vocab, idf)


def key_name(version, prefix):
    return "/".join((COMMON_OUTPUT_PREFIX, str(version), prefix, str(uuid.uuid4())))


def handle_feed_s3(args):
    keys, out_bucket_name = args

    logger.info("Received feed chunk of length {}".format(str(len(keys))))
    # name should have format primary_secondary; e.g., "1000000031200_100070833"
    feed_chunk = FeedChunk(vectorizer)
    for bucket_name, primary, secondary in keys:
        key = Key(
            bucket=conn_s3_global.get_bucket(bucket_name),
            name='{}_{}'.format(primary, secondary)
        )
        feed_chunk.add_feed_from_key(key)

    key_names = {
        POSTS: key_name(version, POSTS_FOLDER),
        LINKS: key_name(version, LINKS_FOLDER),
        LIKES: key_name(version, LIKES_FOLDER),
        TOP_WORDS: key_name(version, TOP_WORDS_FOLDER),
    }

    logger.info("Writing chunk to s3: {}".format(key_names))
    feed_chunk.write_s3(
        conn_s3_global,
        out_bucket_name,
        key_names
    )

    return (
        feed_chunk.counts[POSTS],
        feed_chunk.counts[LINKS],
        feed_chunk.counts[LIKES]
    )


def process_feeds(
    worker_count,
    out_bucket_name,
    version,
    start,
    end,
    pretrained_vectorizer_bucket,
    pretrained_vectorizer_prefix,
    vectorizer_training_bucket,
    training_set_size,
):

    vocab, idf = tfidf.load_or_train_vectorizer_components(
        get_conn_s3(),
        pretrained_vectorizer_bucket,
        pretrained_vectorizer_prefix,
        vectorizer_training_bucket,
        training_set_size
    )
    logger.info(
        "process %d farming out to %d childs" %
        (os.getpid(), worker_count)
    )
    pool = multiprocessing.Pool(
        processes=worker_count,
        initializer=worker_setup,
        initargs=[vocab, idf],
        maxtasksperchild=5,
    )

    recent_feeds_batched = batcher(
        stream_files_between(start, end),
        BATCH_SIZE
    )
    feed_arg_iter = imap(
        None,
        recent_feeds_batched,
        repeat(out_bucket_name)
    )
    post_line_count_tot = 0
    link_line_count_tot = 0
    like_line_count_tot = 0

    for i, counts_tup in enumerate(
        pool.imap_unordered(handle_feed_s3, feed_arg_iter)
    ):
        if counts_tup is None:
            continue
        else:
            post_lines, link_lines, like_lines = counts_tup
            post_line_count_tot += post_lines
            link_line_count_tot += link_lines
            like_line_count_tot += like_lines

    pool.terminate()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Eat up the FB sync data and put it into a tsv'
    )
    parser.add_argument(
        '--out_bucket',
        type=str,
        help='base dir for output files',
        nargs='?',
        default=S3_OUT_BUCKET_NAME
    )
    parser.add_argument(
        '--pretrained_vectorizer_bucket',
        type=str,
        help='s3 bucket housing the pre-trained vectorizer files',
        nargs='?',
        default=tfidf.VECTORIZER_DEFAULT_BUCKET
    )
    parser.add_argument(
        '--pretrained_vectorizer_prefix',
        type=str,
        help='s3 path prefix(sans bucket), housing pretrained vectorizer files',
        nargs='?',
        default=tfidf.VECTORIZER_DEFAULT_PREFIX
    )
    parser.add_argument(
        '--workers',
        type=int,
        help='number of workers to multiprocess',
        default=1
    )
    parser.add_argument(
        '--vectorizer_training_bucket',
        type=str,
        help='s3 bucket housing the raw feed files for training the vectorizer',
        nargs='?',
        default=tfidf.VECTORIZER_TRAINING_BUCKET
    )
    parser.add_argument(
        '--training_set_size',
        type=int,
        help='number of feeds to including in training set',
        default=tfidf.TRAINING_SET_SIZE
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--hours_back',
        type=int,
        help='# of hours back to look for FBSync data',
    )
    group.add_argument(
        '--start_timestamp',
        type=int,
        help='beginning of FBSync timestamp range',
    )
    parser.add_argument(
        '--end_timestamp',
        type=int,
        help='end of FBSync timestamp range',
    )
    args = parser.parse_args()

    version = int(time.time())

    end = args.end_timestamp or version
    if args.start_timestamp:
        start = args.start_timestamp
    else:
        hours_back = args.hours_back or HOURS_BACK
        start = end - 3600*hours_back

    process_feeds(
        args.workers,
        args.out_bucket,
        str(version),
        start,
        end,
        args.pretrained_vectorizer_bucket,
        args.pretrained_vectorizer_prefix,
        args.vectorizer_training_bucket,
        args.training_set_size,
    )

    exit(0)
    logger.info("Done loading to s3, time for Redshift!")
    connection = redshift_engine.connect()
    add_new_data(
        args.out_bucket,
        COMMON_OUTPUT_PREFIX,
	str(version),
        POSTS_FOLDER,
        LINKS_FOLDER,
        LIKES_FOLDER,
        TOP_WORDS_FOLDER,
        connection
    )
