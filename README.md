forklift
========

Data Warehousing ETL

More to come...

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
