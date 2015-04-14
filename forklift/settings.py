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

CELERY_ROUTES = {
    'forklift.tasks.extract_url': {'queue': 'fbsync_extract', 'routing_key': 'fbsync.extract'},
    'forklift.tasks.transform_page': {'queue': 'fbsync_transform', 'routing_key': 'fbsync.transform'},
    'forklift.tasks.check_load': {'queue': 'fbsync_check_load', 'routing_key': 'fbsync.check_load'},
    'forklift.tasks.load_run': {'queue': 'fbsync_load', 'routing_key': 'fbsync.load'},
    'forklift.tasks.merge_run': {'queue': 'fbsync_merge', 'routing_key': 'fbsync.merge'},
    'forklift.tasks.clean_up_incremental_tables': {'queue': 'fbsync_clean_up_incremental_tables', 'routing_key': 'fbsync.clean_up_incremental_tables'},
    'forklift.tasks.compute_aggregates': {'queue': 'fbsync_aggregates', 'routing_key': 'fbsync.aggregates'},
    'forklift.tasks.compute_edges': {'queue': 'fbsync_edges', 'routing_key': 'fbsync.aggregates.edge'},
    'forklift.tasks.compute_post_aggregates': {'queue': 'fbsync_post_aggregates', 'routing_key': 'fbsync.aggregates.post'},
    'forklift.tasks.compute_user_post_aggregates': {'queue': 'fbsync_user_post_aggregates', 'routing_key': 'fbsync.aggregates.user_post'},
    'forklift.tasks.compute_user_timeline_aggregates': {'queue': 'fbsync_user_timeline_aggregates', 'routing_key': 'fbsync.aggregates.user_timeline'},
    'forklift.tasks.compute_poster_aggregates': {'queue': 'fbsync_poster_aggregates', 'routing_key': 'fbsync.aggregates.poster'},
    'forklift.tasks.compute_user_aggregates': {'queue': 'fbsync_user_aggregates', 'routing_key': 'fbsync.aggregates.user'},
    'forklift.tasks.cache_tables': {'queue': 'fbsync_cache_tables', 'routing_key': 'fbsync.cache_tables'},
    'forklift.tasks.batch_push': {'queue': 'fbsync_batch_push', 'routing_key': 'fbsync.batch_push'},
}

CELERY_QUEUES = []
for route, config in CELERY_ROUTES.iteritems():
    CELERY_QUEUES.append(Queue(
        config['queue'],
        routing_key=config['routing_key'],
        queue_arguments=QUEUE_ARGS
    ))


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
