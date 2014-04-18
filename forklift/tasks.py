import celery
import logging
import logging.config
import settings
from celery.utils.log import get_task_logger

from forklift.loaders.fbid_fact_loader import FbidFactLoader
from forklift.loaders.visit_fact_loader import VisitFactLoader
from forklift.loaders.ip_fact_loader import IpFactLoader
from forklift.loaders.friend_fbid_fact_loader import FriendFbidFactLoader
from forklift.loaders.misc_fact_loader import MiscFactLoader
from forklift.db.utils import checkout_connection

app = celery.Celery('forklift')
app.config_from_object('settings')
app.autodiscover_tasks(['tasks']) 

logger = get_task_logger('grackle')

@app.task
def fbid_load_hour(hour):
    with checkout_connection() as connection:
        FbidFactLoader().load_hour(hour, connection, logger)


@app.task
def friend_fbid_load_hour(hour):
    with checkout_connection() as connection:
        FriendFbidFactLoader().load_hour(hour, connection, logger)


@app.task
def visit_load_hour(hour):
    with checkout_connection() as connection:
        VisitFactLoader().load_hour(hour, connection, logger)


@app.task
def misc_load_hour(hour):
    with checkout_connection() as connection:
        MiscFactLoader().load_hour(hour, connection, logger)


@app.task
def ip_load_hour(hour):
    with checkout_connection() as connection:
        IpFactLoader().load_hour(hour, connection, logger)
