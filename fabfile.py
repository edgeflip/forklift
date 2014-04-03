"""Fabric tasks for the forklift project

Explore the project's Fabric interface, e.g.:

    $ fab -l
    $ fab -d build
    $ fab -d build.virtualenv

Requires provisioning of Fabric >= 1.6.

"""
import subprocess

from fabric import api as fab

from fab import BASEDIR, build, serve, test


def _subprocess(*args, **kws):
    # Use Popen to avoid KeyboardInterrupt messiness
    process = subprocess.Popen(*args, **kws)
    while process.returncode is None:
        try:
            process.poll()
        except KeyboardInterrupt:
            # Pass ctrl+c to the shell
            pass
