import argparse
from boto.dynamodb.layer2 import Layer2
from boto.dynamodb.condition import IN, BETWEEN
from celery import chord
import logging
import time
import uuid

from forklift import tasks
from forklift.db.base import redshift_engine
from forklift.loaders.dynamo import DynamoLoader
from forklift.nlp import tfidf
from forklift.s3.utils import get_conn_s3
from forklift.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY
from forklift.utils import batcher

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


S3_OUT_BUCKET_NAME = "warehouse-forklift"
BATCH_SIZE = 100
HOURS_BACK = 24


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

    logger.info("Syncing dynamo")
    dynamo_loader = DynamoLoader(logger, redshift_engine.connect())
    dynamo_loader.sync()

    logger.info("Ensuring that the default vectorizer is trained")

    tfidf.ensure_trained_default_vectorizer(
        get_conn_s3(),
        args.vectorizer_training_bucket,
        args.training_set_size
    )

    logger.info("Trained vectorizer confirmed, building processing run")

    version = str(version)
    group = []
    for chunk in batcher(
        stream_files_between(start, end),
        BATCH_SIZE
    ):
        group.append(tasks.fbsync_process.si(
            chunk,
            version,
            args.out_bucket,
            str(uuid.uuid4()),
        ))
        time.sleep(30) # throttle this a bit so dynamo maybe won't complain

    logger.info("Successfully built run of {} chunks".format(len(group)))
    logger.info("Pushing run onto fbsync queue")
    chord(group)(tasks.fbsync_load.s(args.out_bucket, version))
    logger.info("Successfully pushed run onto fbsync queue")
