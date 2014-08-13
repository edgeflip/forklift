from boto.dynamodb.layer2 import Layer2
from boto.dynamodb.condition import GE, IN
from boto.s3.key import Key
from forklift.loaders.fbsync import FeedChunk, POSTS, LINKS, LIKES, TOP_WORDS
from forklift.utils import batcher
from forklift.s3.utils import get_conn_s3
from forklift.nlp.tfidf import bootstrap_trained_vectorizer, load_or_train_vectorizer_components, TRAINING_SET_SIZE
from forklift.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY
from itertools import imap, repeat
import argparse
import os
import time
import logging
import multiprocessing
import uuid

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False

BATCH_SIZE = 100
S3_OUT_BUCKET_NAME = "redshift_transfer_tristan"
VECTORIZER_TRAINING_BUCKET = "feed_crawler_0"
VECTORIZER_DEFAULT_BUCKET = "redshift_transfer_tristan"
VECTORIZER_DEFAULT_PREFIX = "vectorizer"


def stream_files_since(timestamp):
    querier = Layer2(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    table = querier.get_table('staging.fb_sync_maps')
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
                'updated': GE(timestamp),
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
    vectorizer = bootstrap_trained_vectorizer(vocab, idf)


def key_name(version, prefix):
    return "/".join((version, prefix, str(uuid.uuid4())))


def handle_feed_s3(args):
    keys, out_bucket_name = args

    # name should have format primary_secondary; e.g., "100000008531200_1000760833"
    feed_chunk = FeedChunk(vectorizer)
    for bucket_name, primary, secondary in keys:
        key = Key(
            bucket=conn_s3_global.get_bucket(bucket_name),
            name='{}_{}'.format(primary, secondary)
        )
        feed_chunk.add_feed_from_key(key)

    key_names = {
        POSTS: key_name(version, "posts"),
        LINKS: key_name(version, "links"),
        LIKES: key_name(version, "likes"),
        TOP_WORDS: key_name(version, "top_words"),
    }

    feed_chunk.write_s3(
        conn_s3_global,
        out_bucket_name,
        key_names
    )

    return (feed_chunk.counts[POSTS], feed_chunk.counts[LINKS], feed_chunk.counts[LIKES])


def process_feeds(
    worker_count,
    out_bucket_name,
    version,
    pretrained_vectorizer_bucket,
    pretrained_vectorizer_prefix,
    vectorizer_training_bucket,
    training_set_size,
):

    vocab, idf = load_or_train_vectorizer_components(
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
        initargs=[vocab, idf]
    )

    recent_feeds_batched = batcher(
        stream_files_since(int(time.time() - (86400*30))),
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

    parser = argparse.ArgumentParser(description='Eat up the FB sync data and put it into a tsv')
    parser.add_argument('--out_bucket', type=str, help='base dir for output files', nargs='?', default=S3_OUT_BUCKET_NAME)
    parser.add_argument('--pretrained_vectorizer_bucket', type=str, help='s3 bucket housing the pre-trained vectorizer files', nargs='?', default=VECTORIZER_DEFAULT_BUCKET)
    parser.add_argument('--pretrained_vectorizer_prefix', type=str, help='s3 path prefix, minus the bucket, housing the pre-trained vectorizer files', nargs='?', default=VECTORIZER_DEFAULT_PREFIX)
    parser.add_argument('--workers', type=int, help='number of workers to multiprocess', default=1)
    parser.add_argument('--logfile', type=str, help='for debugging', default=None)
    parser.add_argument('--vectorizer_training_bucket', type=str, help='s3 bucket housing the raw feed files for training the vectorizer', nargs='?', default=VECTORIZER_TRAINING_BUCKET)
    parser.add_argument('--training_set_size', type=int, help='number of feeds to including in training set', default=TRAINING_SET_SIZE)
    args = parser.parse_args()

    hand_s = logging.StreamHandler()
    hand_s.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    if args.logfile is None:
        hand_s.setLevel(logging.DEBUG)
    else:
        hand_s.setLevel(logging.INFO)
        hand_f = logging.FileHandler(args.logfile)
        hand_f.setFormatter(logging.Formatter('%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'))
        hand_f.setLevel(logging.DEBUG)
        logger.addHandler(hand_f)
    logger.addHandler(hand_s)

    version = str(int(time.time()))

    process_feeds(
        args.workers,
        args.out_bucket,
        version,
        args.pretrained_vectorizer_bucket,
        args.pretrained_vectorizer_prefix,
        args.vectorizer_training_bucket,
        args.training_set_size,
    )
