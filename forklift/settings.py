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
SQLALCHEMY_URL = "postgresql://{user}:{pass}@{host}:{port}/{db}".format(**redshift_config)


# Celery
BROKER_URL = 'amqp://{user}:{pass}@{host}:5672/{vhost}'.format(**RABBITMQ)
CELERY_IMPORTS = ('forklift.tasks', )
CELERY_RESULT_BACKEND = ''
CELERYD_PREFETCH_MULTIPLIER = 1
CELERY_ACCEPT_CONTENT = ['pickle']

QUEUE_ARGS = {'x-ha-policy': 'all'}
CELERY_QUEUES = (
    Queue('fbid_hourly', routing_key='hourly.fbid', queue_arguments=QUEUE_ARGS),
    Queue('friend_fbid_hourly', routing_key='hourly.friend_fbid', queue_arguments=QUEUE_ARGS),
    Queue('ip_hourly', routing_key='hourly.ip', queue_arguments=QUEUE_ARGS),
    Queue('visit_hourly', routing_key='hourly.visit', queue_arguments=QUEUE_ARGS),
    Queue('misc_hourly', routing_key='hourly.misc', queue_arguments=QUEUE_ARGS),
)

CELERY_ROUTES = {
    'forklift.tasks.fbid_load_hour': {'queue': 'fbid_hourly', 'routing_key': 'hourly.fbid'},
    'forklift.tasks.friend_fbid_load_hour': {'queue': 'friend_fbid_hourly', 'routing_key': 'hourly.friend_fbid'},
    'forklift.tasks.ip_load_hour': {'queue': 'ip_hourly', 'routing_key': 'hourly.ip'},
    'forklift.tasks.visit_load_hour': {'queue': 'visit_hourly', 'routing_key': 'hourly.visit'},
    'forklift.tasks.misc_load_hour': {'queue': 'misc_hourly', 'routing_key': 'hourly.misc'},
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
    }
}

if os.environ['ENV'] in ('staging', 'production'):
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

S3_OUT_BUCKET_NAME = "user_feeds_parsed"
