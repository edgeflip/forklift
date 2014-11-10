"""Fabric tasks for managing servers"""
from fabric import api as fab
from . import workon
import os

@fab.task(name='celery')
def start_celery():
    config_filename = 'faraday.yaml'
    environment = os.environ['ENV'] if 'ENV' in os.environ else 'development'
    global_config_file = '/etc/forklift/{}'.format(config_filename)
    faraday_conf = global_config_file if os.path.isfile(global_config_file) else config_filename
    fab.local("ENV={} FARADAY_SETTINGS_FILE={} celery -A forklift.tasks worker".format(environment, faraday_conf))


@fab.task
def dynamo(command='up', *flags, **kws):
    """Manage the dynamo development database server

    This task accepts three commands: "up" [default], "status" and "down".
    For example:

        dynamo
        dynamo:up
        dynamo:down

    The first two examples are identical -- they start a mock dynamo database
    server, unless one is already running. This server is globally accessible
    to the system, but by default stores its database file and PID record
    under the working directory's repository checkout. The third example
    stops a server managed by this checkout.

    Optional parameters match those of faraday's "local" command.

    """
    running = False
    with fab.settings(fab.hide('stderr'), warn_only=True):
        result = fab.local('faraday local status', capture=True)
        running = result.succeeded
    commands = ('up', 'down', 'status')
    if command not in commands:
        fab.abort("Unexpected command, select from: {}".format(', '.join(commands)))
    if(
        (command == 'up' and running) or
        (command == 'down' and not running)
    ):
        return

    # translate into faraday commands
    if command == 'up':
        command = 'start'
    elif command == 'down':
        command = 'stop'
    fab.local("faraday local {}".format(command))
