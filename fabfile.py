"""Fabric tasks for the forklift project

Explore the project's Fabric interface, e.g.:

    $ fab -l
    $ fab -d build
    $ fab -d build.virtualenv

Requires provisioning of Fabric >= 1.6.

"""
import subprocess
from os.path import join

from fab import build, serve, test, virtualenv_path
import fabric.api as fab

fab.env.roledefs = {
    'production': ['warehouse-production'],
}


def _subprocess(*args, **kws):
    # Use Popen to avoid KeyboardInterrupt messiness
    process = subprocess.Popen(*args, **kws)
    while process.returncode is None:
        try:
            process.poll()
        except KeyboardInterrupt:
            # Pass ctrl+c to the shell
            pass

@fab.task(name='shell')
def start_shell(env='development'):
    """Open a development shell"""
    _subprocess(join(virtualenv_path('forklift'), 'python'), env={'ENV': env})
