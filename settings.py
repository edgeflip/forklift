import os
#import pymlconf

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(PROJECT_ROOT)
# Load configuration from conf.d directories #
# default configuration in repo:
#config = pymlconf.ConfigManager(dirs=[os.path.join(PROJECT_ROOT, 'conf.d')],
                                #filename_as_namespace=False)
# TODO: Configuration shouldn't live with releases; instead: /etc/edgeflip/ ?
#env_conf_dir = os.path.expanduser(os.getenv('EDGEFLIP_CONF_DIR',
                                            #'/var/www/edgeflip'))
#if os.getenv('EDGEFLIP_CONF_DIR') or os.path.isdir('/var/www/edgeflip'):
    # environmental or personal configuration overwrites:
    #config.load_dirs([os.path.join(env_conf_dir, 'conf.d')],
                     #filename_as_namespace=False)
#locals().update((key.upper(), value) for key, value in config.items())
redshift_config = {
    'db': 'forklift',
    'user': 'redshift',
    'pass': 'root',
    'host': '127.0.0.1',
    'port': '5432',
}
print redshift_config

#redshift_config = config.DATABASES['redshift']
SQLALCHEMY_URL = "postgresql://{user}:{pass}@{host}:{port}/{db}".format(**redshift_config)
