"""Fabric tasks to test the project"""
import os.path
import shutil
import tempfile

from fabric import api as fab

from . import serve, true, workon


DEFAULT_FLAGS = (
    '--with-blockage',
    '--with-progressive',
    '--cover-branches',
    '--cover-erase',
    '--cover-html',
    '--cover-package=loaders',
    '--exclude=^fab$',
    '--logging-level=ERROR',
    '--logging-clear-handlers',
    '--verbosity=4',
)


DEFAULT_KEY_ARGS = (
    # ('key', 'value'),
)


@fab.task(name='all', default=True)
def test(path='', env=None, *args, **kws):
    """Run all, or a subset, of project tests

    To limit test discovery to a particular file or import path, specify the
    "path" argument, e.g.:

        test:tests/test_tasks.py

    Flags and both novel and overriding keyword arguments may be passed to
    nose, e.g.:

        test:.,pdb,config=my.cfg

    This task sets some flags by default. To clear these, pass the flag or
    keyword argument "clear-defaults":

        test:.,clear-defaults
        test:clear-defaults=[true|yes|y|1]

    """
    # Determine test arguments #
    flags = list(args)

    # Include default flags?
    try:
        flags.remove('clear-defaults')
    except ValueError:
        clear_default_args0 = False
    else:
        clear_default_args0 = True
    clear_default_args1 = true(kws.pop('clear-defaults', None))
    if not clear_default_args0 and not clear_default_args1:
        flags.extend(DEFAULT_FLAGS)

    key_args = dict(DEFAULT_KEY_ARGS)
    key_args.update(kws)

    # Test #

    with workon(env):
        fab.local(' '.join(['nosetests'] + flags))


__test__ = False # In case nose gets greedy
