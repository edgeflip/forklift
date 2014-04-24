"""Fabric tasks to prepare and build the project and its environment"""
import itertools
import os.path
from os.path import join

from fabric import api as fab

from . import BASEDIR, true, workon


ENV_NAME = 'forklift'

# Break convention for simplicity here:
l = fab.local


# Tasks #

@fab.task(name='all', default=True)
def build_all(deps='1', env=None):
    """Execute all tasks to build the project and its environment

    By default, OS-level dependencies are installed (via task "dependencies").
    This step may be excluded, on platforms which don't require this or which
    don't provide APT, by supplying argument "deps":

        build:deps=[false|0|no|n]

    Otherwise, the following tasks are executed:

        virtualenv
        update-distribute
        requirements
        db

    Customize the virtualenv name by specifying argument "env":

        build:env=MY-ENV

    """
    # dependencies
    if true(deps):
        fab.execute(install_deps)

    # virtualenv
    fab.execute(make_virtualenv, name=env)

    # update-distribute
    # (Needed as long as Ubuntu provides such an old version):
    fab.execute(update_distribute)

    # requirements
    fab.execute(install_reqs)

    # db
    fab.execute(setup_db)


@fab.task(name='dependencies')
def install_deps():
    """Install OS-level (e.g. C library) dependencies

    Requires that the OS provides APT.

    This task respects the roles under which it is invoked, (and defaults to
    "dev"). "Base" dependencies are always installed, as well as role-specific
    dependencies found in the dependencies/ directory. For example:

        fab -R staging dependencies

    will install dependencies specified by base.dependencies and
    staging.dependencies, given that they are both discovered in the
    dependencies/ directory.

    """
    # Check for apt-get:
    if not which_binary('apt-get'):
        # warn & return rather than abort so as not to raise SystemExit:
        fab.warn("No path to APT, cannot install OS dependencies")
        return

    # Install APT packages specified in dependencies dir:
    roles = fab.env.roles or ['dev'] # Default to just dev
    deps_paths = (join(BASEDIR, 'dependencies', '{}.dependencies'.format(role))
                  for role in itertools.chain(['base'], roles))
    deps = itertools.chain.from_iterable(open(deps_path).readlines()
                                         for deps_path in deps_paths
                                         if os.path.exists(deps_path))
    l('sudo apt-get install -y {}'.format(
        ' '.join(dep.strip() for dep in deps)))


@fab.task(name='virtualenv')
def make_virtualenv(name=None):
    """Create the project's virtual Python environment

    Requires that OS-level dependencies have been installed.

    By default, the virtualenv is named 'forklift'.
    Supply an argument ("name") to change this value:

        virtualenv:MY-ENV

    Returns the environment name (when invoked by another task) and sets this
    value in the Fabric environment.

    """
    name = name or ENV_NAME
    l('''
      source /etc/bash_completion.d/virtualenvwrapper
      mkvirtualenv {}
    '''.format(name), shell='/bin/bash')
    fab.env.virtualenv = name
    return name


@fab.task(name='update-distribute')
def update_distribute(env=None):
    """Update an installation of distribute"""
    with workon(env):
        l('pip install -U distribute')


@fab.task(name='requirements')
def install_reqs(env=None):
    """Install Python package requirements

    Requires that a virtual environment has been created, and is either
    already activated, or specified, e.g.:

        requirements:MY-ENV

    This task respects the roles under which it is invoked, (and defaults to
    "dev"). "Base" requirements are always installed, as well as role-specific
    requirements found in the requirements/ directory. For example:

        fab -R staging requirements

    will install requirements specified by base.requirements and
    staging.requirements, given that they are both discovered in the
    requirements/ directory.

    """
    roles = fab.env.roles or ['dev'] # Default to just dev
    reqs_paths = (join('requirements', '{}.requirements'.format(role))
                  for role in itertools.chain(['base'], roles))
    with workon(env):
        with fab.lcd(BASEDIR):
            l('pip install {}'.format(
                ' '.join('-r ' + path for path in reqs_paths
                         if os.path.exists(join(BASEDIR, path)))
            ))



@fab.task(name='db')
def setup_db(env=None, force='0', testdata='1'):
    """Initialize a redshift (postgresql) database

    Requires that a virtual environment has been created, and is either
    already activated, or specified, e.g.:

        db:MY-ENV

    To force initialization during development, by tearing down any existing
    database, specify "force":

        db:force=[1|true|yes|y]

    In development, a test data fixture is loaded into the database by default; disable
    this by specifying "testdata":

        db:testdata=[0|false|no|n]

    """
    roles = fab.env.roles or ['dev']
    sql_path = join(BASEDIR, 'forklift', 'sql')
    sql_context = {'DATABASE': 'forklift', 'USER': 'redshift', 'PASSWORD': 'root'}

    # Database teardown
    if 'dev' in roles:
        if true(force):
            teardown_commands = open(join(sql_path, 'teardown.sql')).read().split(';')
            for command in teardown_commands:
                l('sudo -u postgres psql --command="{}"'.format(
                    command.strip().format(**sql_context),
                ))

        # Database initialization
        role_exists = l(
            'sudo -u postgres psql -tAc "select 1 from pg_roles where rolname=\'{USER}\'"'.format(**sql_context),
            capture=True,
        )
        if not role_exists:
            l('sudo -u postgres psql -c "create role {USER} with nosuperuser createdb nocreaterole login password \'{PASSWORD}\';"'.format(**sql_context))

        database_exists = l(
            'sudo -u postgres psql -tAc  "select 1 from pg_database where datname=\'{DATABASE}\'"'.format(**sql_context),
            capture=True
        )
        if not database_exists:
            l('sudo -u postgres psql -c "create database {DATABASE} with owner={USER} template=template0 encoding=\'utf-8\'"'.format(**sql_context))


# Helpers #

def which_binary(name):
    """Check for path to binary at `name`, with "which"."""
    with fab.settings(warn_only=True): # Handle abortion manually
        return l('which {}'.format(name), capture=True)
