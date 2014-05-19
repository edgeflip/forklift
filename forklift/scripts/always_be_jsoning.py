#!/usr/bin/python
import sys
import os
import json
import logging
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from urlparse import urlparse
import tempfile
import argparse
import multiprocessing
from itertools import imap, repeat
import os.path
import time
import datetime
import uuid
import itertools

sys.path.append(os.path.abspath(os.curdir))
from forklift.settings import S3_OUT_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_KEY
#from forklift.tasks import post_upload, post_user_upload, move_s3_file


S3_IN_BUCKET_NAMES = [ "user_feeds_%d" % i for i in range(5) ]
S3_DONE_DIR = "loaded"
DB_TEXT_LEN = 4096
FEEDS_PER_FILE = 100    # optimal filesize is between 1MB and 1GB
                        # this is a rough guesstimate that should get us there


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


# S3 stuff

def get_conn_s3(key=AWS_ACCESS_KEY, sec=AWS_SECRET_KEY):
    return S3Connection(key, sec)

def s3_key_iter(bucket_names=S3_IN_BUCKET_NAMES):
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


# data structs for transforming json to db rows


class FeedFromS3(object):
    """Holds an entire feed from a single user crawl"""

    def __init__(self, key):
        prim_id, fbid = key.name.split("_")

        with tempfile.TemporaryFile() as fp:
            key.get_contents_to_file(fp)
            fp.seek(0)
            feed_json = json.load(fp)
            try:
                feed_json_list = feed_json['data']
            except KeyError:
                logger.debug("no data in feed %s" % key.name)
                logger.debug(str(feed_json))
                raise
        logger.debug("\tread feed json with %d posts from %s" % (len(feed_json_list), key.name))

        self.user_id = fbid
        self.posts = []
        for post_json in feed_json_list:
            try:
                self.posts.append(FeedPostFromJson(post_json))
            except Exception:
                logger.debug("error parsing: " + str(post_json))
                # logger.debug("full feed: " + str(feed_json_list))
                raise

    def get_post_lines(self, delim="\t"):
        post_lines = []
        for p in self.posts:
            post_fields = [self.user_id, p.post_id, p.post_ts, p.post_type, p.post_app, p.post_from,
                           p.post_link, p.post_link_domain,
                           p.post_story, p.post_description, p.post_caption, p.post_message]
            line = delim.join(f.replace(delim, " ").replace("\n", " ").encode('utf8', 'ignore') for f in post_fields)
            post_lines.append(line)
        return post_lines

    def get_link_lines(self, delim="\t"):
        link_lines = []
        for p in self.posts:
            for user_id in p.to_ids.union(p.like_ids, p.comment_ids):
                has_to = "1" if user_id in p.to_ids else ""
                has_like = "1" if user_id in p.like_ids else ""
                has_comm = "1" if user_id in p.comment_ids else ""
                link_fields = [p.post_id, user_id, has_to, has_like, has_comm]
                link_lines.append(delim.join(f.encode('utf8', 'ignore') for f in link_fields))
        return link_lines


class FeedChunk(object):
    feeds = None

    def __init__(self, keys):
        self.feeds = []
        for key in keys:
            try:
                self.feeds.append(FeedFromS3(key))
            except KeyError:
                pass


    def write_posts(self, bucket, key_name, delim):
        post_lines = list(itertools.chain.from_iterable(feed.get_post_lines() for feed in self.feeds))
        key_posts = Key(bucket)
        key_posts.key = key_name
        key_posts.set_contents_from_string("\n".join(post_lines))
        return len(post_lines)


    def write_links(self, bucket, key_name, delim):
        link_lines = list(itertools.chain.from_iterable(feed.get_link_lines() for feed in self.feeds))
        key_links = Key(bucket)
        key_links.key = key_name
        key_links.set_contents_from_string("\n".join(link_lines))
        return len(link_lines)


    def write_s3(self, conn_s3, bucket_name, key_name_posts, key_name_links, delim="\t"):
        bucket = conn_s3.get_bucket(bucket_name)
        len_post_lines = self.write_posts(bucket, key_name_posts, delim)
        len_link_lines = self.write_links(bucket, key_name_links, delim)

        return len_post_lines, len_link_lines


# Despite what the docs say, datetime.strptime() format doesn't like %z
# see: http://stackoverflow.com/questions/526406/python-time-to-age-part-2-timezones/526450#526450
def parse_ts(time_string):
    tz_offset_hours = int(time_string[-5:]) / 100  # we're ignoring the possibility of minutes here
    tz_delt = datetime.timedelta(hours=tz_offset_hours)
    return datetime.datetime.strptime(time_string[:-5], "%Y-%m-%dT%H:%M:%S") - tz_delt

class FeedPostFromJson(object):
    """Each post contributes a single post line, and multiple user-post lines to the db"""

    def __init__(self, post_json):
        self.post_id = str(post_json['id'])
        # self.post_ts = post_json['updated_time']
        self.post_ts = parse_ts(post_json['updated_time']).strftime("%Y-%m-%d %H:%M:%S")
        self.post_type = post_json['type']
        self.post_app = post_json['application']['id'] if 'application' in post_json else ""

        self.post_from = post_json['from']['id'] if 'from' in post_json else ""
        self.post_link = post_json.get('link', "")
        self.post_link_domain = urlparse(self.post_link).hostname if (self.post_link) else ""

        #todo: fix this terrible, terrible thing that limits the length of strings
        self.post_story = post_json.get('story', "")
        self.post_description = post_json.get('description', "")
        self.post_caption = post_json.get('caption', "")
        self.post_message = post_json.get('message', "")

        self.to_ids = set()
        self.like_ids = set()
        self.comment_ids = set()
        if 'to' in post_json:
            self.to_ids.update([user['id'] for user in post_json['to']['data']])
        if 'likes' in post_json:
            self.like_ids.update([user['id'] for user in post_json['likes']['data']])
        if 'comments' in post_json:
            self.comment_ids.update([user['id'] for user in post_json['comments']['data']])


# Each worker gets its own Redshift connection, manage that with a global variable.  There's prob
# a better way to do this.
# see: http://stackoverflow.com/questions/10117073/how-to-use-initializer-to-set-up-my-multiprocess-pool
conn_s3_global = None
def set_global_conns():
    global conn_s3_global
    conn_s3_global = get_conn_s3()


def handle_feed_s3(args):
    keys = args[0]  #zzz todo: there's got to be a better way to handle this

    pid = os.getpid()
    logger.debug("pid " + str(pid) + ", " + " have conn")

    # name should have format primary_secondary; e.g., "100000008531200_1000760833"
    feed_chunk = FeedChunk(keys)

    logger.debug("done with the chunk")
    key_name_posts = "posts/" + str(uuid.uuid4())
    key_name_links = "links/" + str(uuid.uuid4())
    post_count, link_count = feed_chunk.write_s3(conn_s3_global, S3_OUT_BUCKET_NAME, key_name_posts, key_name_links)

    #(post_upload.si((key_name_posts,) | move_s3_file.s(S3_OUT_BUCKET_NAME, key_name_posts, S3_DONE_DIR)).apply_async()
    #(post_user_upload.si((key_name_links,) | move_s3_file.s(S3_OUT_BUCKET_NAME, key_name_links, S3_DONE_DIR)).apply_async()

    return (post_count, link_count)


def process_feeds(worker_count, overwrite):

    conn_s3 = get_conn_s3()
    #create_s3_bucket(conn_s3, S3_OUT_BUCKET_NAME, overwrite)
    conn_s3.close()

    logger.info("process %d farming out to %d childs" % (os.getpid(), worker_count))
    pool = multiprocessing.Pool(processes=worker_count, initializer=set_global_conns)

    feed_arg_iter = imap(None, s3_key_iter())
    post_line_count_tot = 0
    link_line_count_tot = 0

    time_start = time.time()
    for i, counts_tup in enumerate(pool.imap_unordered(handle_feed_s3, feed_arg_iter)):

        if i % 1000 == 0:
            time_delt = datetime.timedelta(seconds=int(time.time()-time_start))
            logger.info("\t%s %d feeds, %d posts, %d links" % (str(time_delt), i, post_line_count_tot, link_line_count_tot))
        if counts_tup is None:
            continue
        else:
            post_lines, link_lines = counts_tup
            post_line_count_tot += post_lines
            link_line_count_tot += link_lines

    #zzz todo: deal with unloaded partial batches of feeds still stuck in S3


    pool.terminate()
    return i



###################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Eat up the FB sync data and put it into a tsv')
    # parser.add_argument('out_dir', type=str, help='base dir for output files')
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

    process_feeds(args.workers, args.overwrite)


#zzz todo: do something more intelligent with \n and \t in text

#zzz todo: audit (non-)use of delim for different handlers

