"""Fabric tasks for managing servers"""
from fabric import api as fab
import os

@fab.task(name='celery')
def start_celery():
    fab.local("ENV={} celery -A forklift.tasks worker".format(os.environ['ENV']))
