from importlib import import_module
from datetime import datetime, timedelta, date, time
import os, sys
import argparse

sys.path.append(os.path.abspath(os.curdir))
from forklift.settings import CELERY_ROUTES as routes

def formatted_date(datestr):
    return datetime.strptime(datestr, '%Y-%m-%d').date()

def function_from_task(task):
    path, function = task.rsplit('.', 1)
    mod = import_module(path)
    return getattr(mod, function)

def hour_range(start, end):
    curr = start
    while curr < end:
        yield curr
        curr += timedelta(hours=1)

def queue_hour(hour):
    truncated_hour = truncate(hour)
    for method in methods:
        method.apply_async(args=(truncated_hour,))

def truncate(dt):
    return dt.replace(minute=0,second=0,microsecond=0)

parser = argparse.ArgumentParser(description="Queue hourly fact loaders")
parser.add_argument('--start-date', type=formatted_date, default=None)
parser.add_argument('--end-date', type=formatted_date, default=None)
parser.add_argument('--days-back', type=int, default=None)
args = parser.parse_args()

tasks = [route for route in routes if routes[route]['routing_key'].startswith('hourly.')]
methods = [function_from_task(task) for task in tasks]

if args.start_date and args.end_date:
    for hour in hour_range(
        datetime.combine(args.start_date, time()),
        datetime.combine(args.end_date, time())
    ):
        queue_hour(hour)
elif args.start_date:
    for hour in hour_range(
        datetime.combine(args.start_date, time()),
        datetime.today()
    ):
        queue_hour(hour)
elif args.end_date:
    pass
    # error, pick a start date dude
elif args.days_back:
    for hour in hour_range(
        datetime.today() - timedelta(days=args.days_back),
        datetime.today()
    ):
        queue_hour(hour)
else:
    for hour in hour_range(
        datetime.today() - timedelta(hours=1),
        datetime.today()
    ):
        queue_hour(hour)

