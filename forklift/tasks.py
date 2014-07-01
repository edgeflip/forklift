import celery

import forklift.loaders.fact.hourly as loaders
from forklift.db.utils import checkout_connection, load_from_s3
from forklift.s3.utils import move_file
from forklift.settings import S3_OUT_BUCKET_NAME
import logging

app = celery.Celery('forklift')
app.config_from_object('forklift.settings')
app.autodiscover_tasks(['forklift.tasks'])

logger = logging.getLogger()

@app.task
def fbid_load_hour(hour):
    with checkout_connection() as connection:
        loaders.FbidHourlyFactLoader().load_hour(hour, connection, logger)


@app.task
def friend_fbid_load_hour(hour):
    with checkout_connection() as connection:
        loaders.FriendFbidHourlyFactLoader().load_hour(hour, connection, logger)


@app.task
def visit_load_hour(hour):
    with checkout_connection() as connection:
        loaders.VisitHourlyFactLoader().load_hour(hour, connection, logger)


@app.task
def misc_load_hour(hour):
    with checkout_connection() as connection:
        loaders.MiscHourlyFactLoader().load_hour(hour, connection, logger)


@app.task
def ip_load_hour(hour):
    with checkout_connection() as connection:
        loaders.IpHourlyFactLoader().load_hour(hour, connection, logger)

@app.task
def post_import(filename):
    with checkout_connection() as connection:
        load_from_s3(connection, S3_OUT_BUCKET_NAME, filename, 'posts')

@app.task
def post_user_import(filename):
    with checkout_connection() as connection:
        load_from_s3(connection, S3_OUT_BUCKET_NAME, filename, 'user_posts')


@app.task
def move_s3_file(bucket_name, key_name, new_directory):
    move_file(bucket_name, key_name, new_directory)
