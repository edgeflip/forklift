#!/usr/bin/python
import os
import logging
import argparse
import multiprocessing
from itertools import imap, repeat
import os.path
import time
import datetime
import uuid
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import spdiags
import nltk

from forklift.s3.utils import get_conn_s3, create_s3_bucket, stream_batched_files_from, stream_files_from, write_string_to_key
from forklift.loaders.fbsync import FeedFromS3, FeedChunk, POSTS, LINKS, LIKES, TOP_WORDS


S3_OUT_BUCKET_NAME = "redshift_transfer_tristan"
S3_IN_BUCKET_NAMES = [ "user_feeds_%d" % i for i in range(5) ] + ["feed_crawler_%d" % i for i in range(5) ]
FEEDS_PER_FILE = 100    # optimal filesize is between 1MB and 1GB
                        # this is a rough guesstimate that should get us there
TRAINING_SET_SIZE = 10000

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False

# scikit-learn stuff

def tokenizer(message):
    return nltk.regexp_tokenize(message, r'(?u)\b\w\w+\b')

def create_vectorizer(**extra_kwargs):
    return TfidfVectorizer(
        input='content',
        min_df=0.001,
        max_df=0.4,
        tokenizer=tokenizer,
        **extra_kwargs
    )

def training_feeds(in_bucket_names, training_set_size):
    for i, key in enumerate(stream_files_from(in_bucket_names)):
        if i > training_set_size:
            return
        try:
            feed = FeedFromS3(key)
            yield feed.post_corpus
        except KeyError:
            logger.info("Skipping data-less key %s", key)

def train(in_bucket_names, training_set_size):
    vectorizer = create_vectorizer()
    logger.debug("Fitting training set.")
    vectorizer.fit(training_feeds(in_bucket_names, training_set_size))
    logger.debug("Done fitting.")
    return (vectorizer.vocabulary_, vectorizer.idf_)

# Each worker gets its own S3 connection, manage that with a global variable.  There's prob
# a better way to do this.
# see: http://stackoverflow.com/questions/10117073/how-to-use-initializer-to-set-up-my-multiprocess-pool
conn_s3_global = None
vectorizer = None
def worker_setup(vocab, idf):
    global conn_s3_global
    conn_s3_global = get_conn_s3()

    global vectorizer
    vectorizer = create_vectorizer(vocabulary=vocab)
    n_features = idf.shape[0]
    vectorizer._tfidf._idf_diag = spdiags(idf, diags=0, m=n_features, n=n_features)


def key_name(prefix):
    return prefix + "/" + str(uuid.uuid4())

def handle_feed_s3(args):
    keys, out_bucket_name = args

    pid = os.getpid()

    # name should have format primary_secondary; e.g., "100000008531200_1000760833"
    feed_chunk = FeedChunk(vectorizer)
    for key in keys:
        feed_chunk.add_feed_from_key(key)

    key_names = {
        POSTS: key_name("posts"),
        LINKS: key_name("links"),
        LIKES: key_name("likes"),
        TOP_WORDS: key_name("top_words"),
    }

    feed_chunk.write_s3(
        conn_s3_global,
        out_bucket_name,
        key_names
    )

    return (feed_chunk.counts[POSTS], feed_chunk.counts[LINKS], feed_chunk.counts[LIKES])


def process_feeds(worker_count, overwrite, out_bucket_name, in_bucket_names, training_set_size):

    vectorizer_parameters = train(in_bucket_names, training_set_size)

    conn_s3 = get_conn_s3()
    create_s3_bucket(conn_s3, out_bucket_name, overwrite)
    conn_s3.close()

    logger.info("process %d farming out to %d childs" % (os.getpid(), worker_count))
    pool = multiprocessing.Pool(processes=worker_count, initializer=worker_setup, initargs=list(vectorizer_parameters))

    feed_arg_iter = imap(None, stream_batched_files_from(in_bucket_names, FEEDS_PER_FILE), repeat(out_bucket_name))
    post_line_count_tot = 0
    link_line_count_tot = 0
    like_line_count_tot = 0

    time_start = time.time()
    for i, counts_tup in enumerate(pool.imap_unordered(handle_feed_s3, feed_arg_iter)):
        if i % 1000 == 0:
            time_delt = datetime.timedelta(seconds=int(time.time()-time_start))
            logger.info("\t%s %d feed chunks of size %s, %d posts, %d links, %d likes" % (str(time_delt), i, FEEDS_PER_FILE, post_line_count_tot, link_line_count_tot, like_line_count_tot))
        if counts_tup is None:
            continue
        else:
            post_lines, link_lines, like_lines = counts_tup
            post_line_count_tot += post_lines
            link_line_count_tot += link_lines
            like_line_count_tot += like_lines

    pool.terminate()


###################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Eat up the FB sync data and put it into a tsv')
    parser.add_argument('--out_bucket', type=str, help='base dir for output files', nargs='?', default=S3_OUT_BUCKET_NAME)
    parser.add_argument('--in_buckets', type=str, help='buckets that hold input files', nargs='*', default=S3_IN_BUCKET_NAMES)
    parser.add_argument('--workers', type=int, help='number of workers to multiprocess', default=1)
    parser.add_argument('--training_set_size', type=int, help='number of feeds to including in training set', default=TRAINING_SET_SIZE)
    parser.add_argument('--overwrite', action='store_true', help='overwrite previous runs')
    parser.add_argument('--logfile', type=str, help='for debugging', default=None)
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

    process_feeds(args.workers, args.overwrite, args.out_bucket, args.in_buckets, args.training_set_size)


#zzz todo: do something more intelligent with \n and \t in text

#zzz todo: audit (non-)use of delim for different handlers

