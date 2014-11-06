"""Fabric tasks for managing servers"""
from fabric import api as fab
from . import workon
import os

@fab.task(name='celery')
def start_celery():
    fab.local("ENV={} celery -A forklift.tasks worker".format(os.environ['ENV']))


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
    environment = os.environ['ENV'] if 'ENV' in os.environ else 'development'
    with workon(environment):
        with fab.settings(fab.hide('running'), warn_only=True):
            status = fab.local('faraday local status', capture=True)
            if 'local server found running' in status:
                running = True
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
