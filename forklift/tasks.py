import celery
from celery.utils.log import get_task_logger

import forklift.loaders.fact.hourly as loaders
from forklift.db.utils import checkout_connection

app = celery.Celery('forklift')
app.config_from_object('forklift.settings')
app.autodiscover_tasks(['forklift.tasks'])

logger = get_task_logger('grackle')

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
