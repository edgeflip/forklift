forklift
========

Data Warehousing ETL

Forklift's aim is to manage data import, processing, and export in our data warehouse. We currently use it to compute aggregates for the targeted sharing app within Redshift.

Some useful terms here, if you haven't encountered them yet, are the standard data warehousing concepts of the 'Dimension' and 'Fact'. A dimension is essentially just a field in a table which we'd like to break down (group by clause in a SQL query) or filter by (where clause). Facts refer to numeric data that we'd like to retrieve and analyze (select clause).

Our targeted sharing app produces a buttload of events and related rows (visits, visitors especially). We sync these to Redshift (under a different repo ATM) and here refer to these as 'raw' tables. The aggregate computations convert this raw event data into fact tables, which look like this:

campaign id | hour | fact1 | fact2
:--: | :--: | :--: | :--:
5 | 2014-02-01 06 PM | 246 | 123

The campaign id and hours are our dimensions, and the rest of the columns are facts. Since we rely heavily on distinct events by some dimension (visit id, ip address, etc), most fact tables have an additional dimension to ensure we can still roll up different time periods and not double-count rows. These tables end up being pretty big, as you can expect, but still function as a way to let us collapse 25 or so events down to one easily-queryable row.


Installation
---
Sandbox is fabricated in the same vein as the Edgeflip repository, so beware the copypasta. If you already have an edgeflip sandbox, 1-3 will be done and are skippable, but are included here for completeness.

1. You'll need an SSH key with access to the edgeflip github account in ~/.ssh -- contact us if you need to get one.
2. Install minimal developer tools, git and Fabric:

    $ `sudo apt-get install git python-pip`
    $ `sudo pip install Fabric>=1.6`

3. [Add the RabbitMQ ppa](http://rabbitmq.com/install-debian.html#apt) (such that the `fab build` step will automatically install the appropriate version).

4. Check out the repo: `git clone https://github.com/edgeflip/forklift.git`
5. From within the checkout hierarchy, build the application:

    $ `ENV=development fab build`

    (To see all tasks and options, run `fab -l`.)
6. Run migrations:
    $ `ENV=development alembic upgrade head`
7. Test:
    $ `ENV=development fab test`



####RabbitMQ
---
This is the same as 'edgeflip' as well, so skip this if you've done this for that repo.

1. Start by installing RabbitMQ: `sudo apt-get install rabbitmq-server`. (If this is your first setup, `fab build` should have done this for you.)
2. Create a user: `sudo rabbitmqctl add_user edgeflip edgeflip`
3. Create a vhost: `sudo rabbitmqctl add_vhost edgehost`
4. Set permissions for this user on that new vhost: `sudo rabbitmqctl set_permissions -p edgehost edgeflip ".*" ".*" ".*"`

Using
---
#### Run the celery workers from your virtualenv
    $ ENV=development fab serve.celery

#### To make them do something, queue up some jorbs:
* The following script queues up processing jobs for all of the existing queues. The default is the current and previous hours: `ENV=development python forklift/scripts/queue_hourly_fact_loaders.py`
* Perform repulls with a number of days back: `ENV=development python forklift/scripts/queue_hourly_fact_loaders.py --days-back=25`
* Or a start date: `ENV=development python forklift/scripts/queue_hourly_fact_loaders.py --start-date=2014-02-01`
* Or a specific date range: `ENV=development python forklift/scripts/queue_hourly_fact_loaders.py --start-date=2014-02-01 --end-date=2014-02-28`

#### Adding facts or other stuff
1. Update the [warehouse definition](forklift/warehouse/definition.py)
2. Have alembic create a migration for you:
    $ `ENV=development alembic revision -m "Added some shit" --autogenerate`
3. It will spit out a filename. Open it up and make any necessary changes; usually there aren't any, but if you have miscellaneous stuff in your development db it might try and delete it, so you should take those lines out of both the up and down migrations.
4. Apply the migration:
    $ `ENV=development alembic upgrade head`


Updating Production
---

1. Currently we only have one box that runs everything. `ecssh i-1d280f4e`
2. `screen -r warehouse`
3. Kill it with Ctrl+c
4. `git pull`
5. *other stuff, maybe. see below*
6. `ENV=production fab -R production serve.celery`
7. Detach (Ctrl-a d)

### Activities which may go in step 5:

#### Updating libs (this may never happen):
```
deactivate
ENV=production fab -R production build
workon forklift
```

#### Updating credentials:
    $ sudo s3cmd get --recursive --force -c /root/.s3cfg-creds s3://ef-techops/creds/apps/production/edgeflip-forklift/ /etc/forklift/conf.d/

#### Running migrations:
    $ ENV=production alembic upgrade head`
