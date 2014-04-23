from importlib import import_module
from datetime import datetime, timedelta
import os, sys

sys.path.append(os.path.abspath(os.curdir))
from forklift.settings import CELERY_ROUTES as routes

def truncate(dt):
    return dt.replace(minute=0,second=0,microsecond=0)


this_hour = truncate(datetime.today())
last_hour = truncate(datetime.today() - timedelta(hours=1))
tasks = [route for route in routes if routes[route]['routing_key'].startswith('hourly.')]

for task in tasks:
    path, method = task.rsplit('.', 1)
    mod = import_module(path)
    met = getattr(mod, method)
    met.apply_async(args=(this_hour,))
    met.apply_async(args=(last_hour,))

