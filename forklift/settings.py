import os
import pymlconf
from kombu import Queue
import logging
import logging.config
from celery.signals import setup_logging

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
GLOBAL_CONFIG_DIR = '/etc/forklift'
# Load configuration from conf.d directories #
# default configuration in repo:
config = pymlconf.ConfigManager(dirs=[os.path.join(PROJECT_ROOT, 'conf.d')], filename_as_namespace=False)
if os.path.isdir(GLOBAL_CONFIG_DIR):
    config.load_dirs([os.path.join(GLOBAL_CONFIG_DIR, 'conf.d')], filename_as_namespace=False)

locals().update((key.upper(), value) for key, value in config.items())

redshift_config = DATABASES['redshift']
REDSHIFT_URL = "postgresql://{user}:{pass}@{host}:{port}/{db}".format(**redshift_config)

rds_source_config = DATABASES['rds_source']
RDS_SOURCE_URL = "mysql://{user}:{pass}@{host}:{port}/{db}".format(**rds_source_config)

rds_cache_config = DATABASES['rds_cache']
RDS_CACHE_URL = "postgresql://{user}:{pass}@{host}:{port}/{db}".format(**rds_cache_config)

# Celery
BROKER_URL = 'amqp://{user}:{pass}@{host}:5672/{vhost}'.format(**RABBITMQ)
CELERY_IMPORTS = ('forklift.tasks', )
CELERY_RESULT_BACKEND = 'redis://{host}:6379'.format(**REDIS_RESULT_BACKEND)
CELERYD_PREFETCH_MULTIPLIER = 1
CELERY_ACCEPT_CONTENT = ['pickle', 'json'] # json so we can inspect workers
CELERYD_MAX_TASKS_PER_CHILD = 5

QUEUE_ARGS = {'x-ha-policy': 'all'}
CELERY_QUEUES = (
    Queue('fbid_hourly', routing_key='hourly.fbid', queue_arguments=QUEUE_ARGS),
    Queue('friend_fbid_hourly', routing_key='hourly.friend_fbid', queue_arguments=QUEUE_ARGS),
    Queue('ip_hourly', routing_key='hourly.ip', queue_arguments=QUEUE_ARGS),
    Queue('visit_hourly', routing_key='hourly.visit', queue_arguments=QUEUE_ARGS),
    Queue('misc_hourly', routing_key='hourly.misc', queue_arguments=QUEUE_ARGS),
    Queue('fbsync', routing_key='fbsync', queue_arguments=QUEUE_ARGS),
    Queue('extract', routing_key='extract', queue_arguments=QUEUE_ARGS),
    Queue('transform', routing_key='transform', queue_arguments=QUEUE_ARGS),
    Queue('check_load', routing_key='check_load', queue_arguments=QUEUE_ARGS),
    Queue('load', routing_key='load', queue_arguments=QUEUE_ARGS),
    Queue('merge', routing_key='merge', queue_arguments=QUEUE_ARGS),
    Queue('clean_up_incremental_tables', routing_key='clean_up_incremental_tables', queue_arguments=QUEUE_ARGS),
    Queue('compute_aggregates', routing_key='compute_aggregates', queue_arguments=QUEUE_ARGS),
    Queue('compute_edges', routing_key='compute_edges', queue_arguments=QUEUE_ARGS),
    Queue('compute_post_aggregates', routing_key='compute_post_aggregates', queue_arguments=QUEUE_ARGS),
    Queue('compute_user_post_aggregates', routing_key='compute_user_post_aggregates', queue_arguments=QUEUE_ARGS),
    Queue('compute_user_timeline_aggregates', routing_key='compute_user_timeline_aggregates', queue_arguments=QUEUE_ARGS),
    Queue('compute_poster_aggregates', routing_key='compute_poster_aggregates', queue_arguments=QUEUE_ARGS),
    Queue('compute_user_aggregates', routing_key='compute_user_aggregates', queue_arguments=QUEUE_ARGS),
)

CELERY_ROUTES = {
    'forklift.tasks.fbid_load_hour': {'queue': 'fbid_hourly', 'routing_key': 'hourly.fbid'},
    'forklift.tasks.friend_fbid_load_hour': {'queue': 'friend_fbid_hourly', 'routing_key': 'hourly.friend_fbid'},
    'forklift.tasks.ip_load_hour': {'queue': 'ip_hourly', 'routing_key': 'hourly.ip'},
    'forklift.tasks.visit_load_hour': {'queue': 'visit_hourly', 'routing_key': 'hourly.visit'},
    'forklift.tasks.misc_load_hour': {'queue': 'misc_hourly', 'routing_key': 'hourly.misc'},
    'forklift.tasks.fbsync_process': {'queue': 'fbsync', 'routing_key': 'fbsync'},
    'forklift.tasks.fbsync_load': {'queue': 'fbsync', 'routing_key': 'fbsync'},
    'forklift.tasks.extract_url': {'queue': 'extract', 'routing_key': 'extract'},
    'forklift.tasks.transform_page': {'queue': 'transform', 'routing_key': 'transform'},
    'forklift.tasks.check_load': {'queue': 'check_load', 'routing_key': 'check_load'},
    'forklift.tasks.load_run': {'queue': 'load', 'routing_key': 'load'},
    'forklift.tasks.merge_run': {'queue': 'merge', 'routing_key': 'merge'},
    'forklift.tasks.clean_up_incremental_tables': {'queue': 'clean_up_incremental_tables', 'routing_key': 'clean_up_incremental_tables'},
    'forklift.tasks.compute_aggregates': {'queue': 'compute_aggregates', 'routing_key': 'compute_aggregates'},
    'forklift.tasks.compute_edges': {'queue': 'compute_edges', 'routing_key': 'compute_edges'},
    'forklift.tasks.compute_post_aggregates': {'queue': 'compute_post_aggregates', 'routing_key': 'compute_post_aggregates'},
    'forklift.tasks.compute_user_post_aggregates': {'queue': 'compute_user_post_aggregates', 'routing_key': 'compute_user_post_aggregates'},
    'forklift.tasks.compute_user_timeline_aggregates': {'queue': 'compute_user_timeline_aggregates', 'routing_key': 'compute_user_timeline_aggregates'},
    'forklift.tasks.compute_poster_aggregates': {'queue': 'compute_poster_aggregates', 'routing_key': 'compute_poster_aggregates'},
    'forklift.tasks.compute_user_aggregates': {'queue': 'compute_user_aggregates', 'routing_key': 'compute_user_aggregates'},
}

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'console': {
            'format': '[%(asctime)s][%(levelname)s] %(name)s %(filename)s:%(funcName)s:%(lineno)d | %(message)s',
            'datefmt': '%H:%M:%S',
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'console',
        },
        'syslog': {
            'level': 'INFO',
            'class': 'logging.handlers.SysLogHandler',
            'formatter': 'console',
            'address': '/dev/log',
        },
    },
    'root': {
        'handlers': ['console', 'syslog'],
        'level': 'DEBUG',
    },
    'loggers': {
        'grackle': {
            'level': 'DEBUG',
        },
        '__main__': {
            'handlers': ['console'],
        },
        'forklift.tasks': {
            'handlers': ['console', 'syslog'],
        },
    }
}

if 'ENV' in os.environ and os.environ['ENV'] in ('staging', 'production'):
    LOGGING['root']['level'] = 'INFO'
    LOGGING['handlers']['sentry'] = {
        'level': 'INFO',
        'class': 'raven.handlers.logging.SentryHandler',
        'formatter': 'console',
    }
    LOGGING['loggers']['grackle'].setdefault('handlers', []).append('sentry')
logging.config.dictConfig(LOGGING)


@setup_logging.connect
def configure_logging(sender=None, **kwargs):
    logging.config.dictConfig(LOGGING)


IP_SLUG = 'ip'
FBID_SLUG = 'fbid'
VISIT_SLUG = 'visit'
FRIEND_SLUG = 'friend_fbid'
MISC_SLUG = 'misc'
