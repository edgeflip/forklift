import celery

from loaders.fbid_fact_loader import FbidFactLoader
from loaders.visit_fact_loader import VisitFactLoader
from loaders.ip_fact_loader import IpFactLoader
from loaders.friend_fbid_fact_loader import FriendFbidFactLoader
from loaders.misc_fact_loader import MiscFactLoader

from db.base import engine

class SqlAlchemyTask(celery.Task):
    """Abstract Celery Task to make sure a connection is 
    available and is closed on task completion"""
    abstract = True

    def run(self, *args):
        connection = engine.connect()
        try:
            with connection.begin_nested():
                self.do_work(connection, *args)                
        finally:
            connection.close()


class HourlyLoaderTask(SqlAlchemyTask):
    abstract = True

    def do_work(self, connection, hour):
        self.loader.load_hour(hour, connection)


class FbidHourlyLoaderTask(HourlyLoaderTask):
    def __init__(self):
        self.loader = FbidFactLoader()


class VisitFactLoaderTask(HourlyLoaderTask):
    def __init__(self):
        self.loader = VisitFactLoader()


class IpFactLoaderTask(HourlyLoaderTask):
    def __init__(self):
        self.loader = IpFactLoader()


class FriendFbidFactLoaderTask(HourlyLoaderTask):
    def __init__(self):
        self.loader = FriendFbidFactLoader()


class MiscFactLoaderTask(HourlyLoaderTask):
    def __init__(self):
        self.loader = MiscFactLoader()

