"""Fabric tasks for managing servers"""
import os
import signal
from os.path import join

from fabric import api as fab

from . import BASEDIR


# Break convention for simplicity here:
l = fab.local

CELERY_QUEUES = (
    'hourly_fbid_facts',
    'hourly_visit_facts',
    'hourly_friend_fbid_facts',
    'hourly_misc_facts',
)


# TODO: Make runserver and celery tasks more like dynamo, s.t. can also do this:
#@fab.task(name='all')
#def start_all():
#    """Start all edgeflip servers
#
#    Namely:
#
#        background tasks (Celery)
#
#    """
#    fab.execute(start_celery)



@fab.task(name='celery')
def start_celery(workers='4',
                 loglevel='info',
                 queues=','.join(CELERY_QUEUES)):
    """Start Celery with the specified number of workers and log level

    Directs Celery at the default set of queues, or those specified, e.g.:

        celery:queues="hourly_misc_facts\,hourly_fbid_facts"

    """
    pass
