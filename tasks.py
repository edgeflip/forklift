import celery
from celery.log import get_task_logger
import settings

from loaders.fbid_fact_loader import FbidFactLoader
from loaders.visit_fact_loader import VisitFactLoader
from loaders.ip_fact_loader import IpFactLoader
from loaders.friend_fbid_fact_loader import FriendFbidFactLoader
from loaders.misc_fact_loader import MiscFactLoader

from db.utils import checkout_connection

app = celery.Celery('forklift')
app.config_from_object('settings')
app.autodiscover_tasks(['tasks']) 

logger = get_task_logger(__name__)

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
