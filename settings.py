import os
import pymlconf

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(PROJECT_ROOT)
# Load configuration from conf.d directories #
# default configuration in repo:
config = pymlconf.ConfigManager(dirs=[os.path.join(PROJECT_ROOT, 'conf.d')],
                                filename_as_namespace=False)

locals().update((key.upper(), value) for key, value in config.items())

redshift_config = DATABASES['redshift']
SQLALCHEMY_URL = "postgresql://{user}:{pass}@{host}:{port}/{db}".format(**redshift_config)


# Celery
BROKER_URL = 'amqp://{user}:{pass}@{host}:5672/{vhost}'.format(**RABBITMQ)
CELERY_IMPORTS = ('tasks', )
CELERY_RESULT_BACKEND = ''
