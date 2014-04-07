"""Fabric tasks for managing servers"""
from fabric import api as fab

@fab.task(name='celery')
def start_celery(env='development'):
    fab.local("ENV={} celery -A tasks worker".format(env))
