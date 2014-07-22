#!/usr/bin/python
import os
import os
import logging
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from urlparse import urlparse
import argparse
import multiprocessing
from itertools import imap, repeat
import os.path
import time
import datetime
import uuid
import itertools
import httplib
import socket

from forklift.s3.utils import get_conn_s3, create_s3_bucket
from forklift.loaders.fbsync import FeedPostFromJson, FeedFromS3


S3_OUT_BUCKET_NAME = "redshift_transfer_tristan"
S3_IN_BUCKET_NAMES = [ "user_feeds_%d" % i for i in range(5) ] + ["feed_crawler_%d" % i for i in range(5) ]
FEEDS_PER_FILE = 100    # optimal filesize is between 1MB and 1GB
                        # this is a rough guesstimate that should get us there

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


# S3 stuff

def s3_key_iter(bucket_names):
    conn_s3 = get_conn_s3()
    for b, bucket_name in enumerate(bucket_names):
        logger.debug("reading bucket %d/%d (%s)" % (b, len(bucket_names), bucket_name))
        feeds = conn_s3.get_bucket(bucket_name).list()
        feeds_in_batch = 0
        feed_batch = []
        for feed in feeds:
            if feeds_in_batch < FEEDS_PER_FILE:
                feed_batch.append(feed)
                feeds_in_batch += 1
            else:
                feeds_in_batch = 0
                yield feed_batch
                feed_batch = []
        if feeds_in_batch > 0:
            yield feed_batch
    conn_s3.close()


# data structs for transforming json to db rows


class FeedChunk(object):
    def __init__(self, keys):
        self.num_posts = 0
        self.num_links = 0
        self.num_likes = 0
        self.post_string = ""
        self.link_string = ""
        self.like_string = ""
        for key in keys:
            for attempt in range(3):
                try:
                    feed = FeedFromS3(key)
                    post_lines = feed.get_post_lines()
                    link_lines = feed.get_link_lines()
                    like_lines = feed.get_like_lines()
                    self.post_string += "\n".join(post_lines) + "\n"
                    self.link_string += "\n".join(link_lines) + "\n"
                    self.like_string += "\n".join(like_lines) + "\n"
                    self.num_posts += len(post_lines)
                    self.num_links += len(link_lines)
                    self.num_likes += len(like_lines)
                    break
                except (httplib.IncompleteRead, socket.error):
                    pass
                except KeyError:
                    break


    def write_posts(self, bucket, key_name, delim):
        self.write_stuff(bucket, key_name, self.post_string)

    def write_links(self, bucket, key_name, delim):
        self.write_stuff(bucket, key_name, self.link_string)

    def write_likes(self, bucket, key_name, delim):
        self.write_stuff(bucket, key_name, self.like_string)

    def write_stuff(self, bucket, key_name, string):
        key = Key(bucket)
        key.key = key_name
        key.set_contents_from_string(string + "\n")
        key.close()

    def write_s3(self, conn_s3, bucket_name, key_name_posts, key_name_links, key_name_likes, delim="\t"):
        bucket = conn_s3.get_bucket(bucket_name)
        self.write_posts(bucket, key_name_posts, delim)
        self.write_links(bucket, key_name_links, delim)
        self.write_likes(bucket, key_name_likes, delim)


# Each worker gets its own S3 connection, manage that with a global variable.  There's prob
# a better way to do this.
# see: http://stackoverflow.com/questions/10117073/how-to-use-initializer-to-set-up-my-multiprocess-pool
conn_s3_global = None
def set_global_conns():
    global conn_s3_global
    conn_s3_global = get_conn_s3()


def key_name(prefix):
    return prefix + "/" + str(uuid.uuid4())

def handle_feed_s3(args):
    keys, out_bucket_name = args

    pid = os.getpid()

    # name should have format primary_secondary; e.g., "100000008531200_1000760833"
    feed_chunk = FeedChunk(keys)
    key_names = key_name(prefix) for prefix in "posts", "links", "likes"

    feed_chunk.write_s3(
        conn_s3_global,
        out_bucket_name,
        *key_names
    )

    return (feed_chunk.num_posts, feed_chunk.num_links, feed_chunk.num_likes)


def process_feeds(worker_count, overwrite, out_bucket_name, in_bucket_names):

    conn_s3 = get_conn_s3()
    create_s3_bucket(conn_s3, out_bucket_name, overwrite)
    conn_s3.close()

    logger.info("process %d farming out to %d childs" % (os.getpid(), worker_count))
    pool = multiprocessing.Pool(processes=worker_count, initializer=set_global_conns)

    feed_arg_iter = imap(None, s3_key_iter(in_bucket_names), repeat(out_bucket_name))
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

    #zzz todo: deal with unloaded partial batches of feeds still stuck in S3


    pool.terminate()
    return i



###################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Eat up the FB sync data and put it into a tsv')
    parser.add_argument('--out_bucket', type=str, help='base dir for output files', nargs='?', default=S3_OUT_BUCKET_NAME)
    parser.add_argument('--in_buckets', type=str, help='buckets that hold input files', nargs='*', default=S3_IN_BUCKET_NAMES)
    parser.add_argument('--workers', type=int, help='number of workers to multiprocess', default=1)
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

    process_feeds(args.workers, args.overwrite, args.out_bucket, args.in_buckets)


#zzz todo: do something more intelligent with \n and \t in text

#zzz todo: audit (non-)use of delim for different handlers

