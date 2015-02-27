import datetime
import logging
import requests
from requests.adapters import HTTPAdapter
import sys

LOG = logging.getLogger(__name__)
FB_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def urlload(url, **kwargs):
    logger = kwargs.pop('logger', LOG)
    """Load data from the given Facebook URL."""
    timeout = kwargs.pop('timeout', 10)
    try:
        s = requests.Session()
        s.mount('https://graph.facebook.com', HTTPAdapter(max_retries=1))
        return s.get(url, params=kwargs, timeout=timeout).json()
    except IOError as exc:
        exc_type, exc_value, trace = sys.exc_info()
        logger.warning(
            "Error opening URL %s %r", url, getattr(exc, 'reason', ''),
            exc_info=True
        )
        try:
            original_msg = exc.read()
        except Exception:
            pass
        else:
            if original_msg:
                logger.warning("Returned error message was: %s", original_msg)
        raise exc_type, exc_value, trace


# Despite what the docs say, datetime.strptime() format doesn't like %z
# see: http://stackoverflow.com/questions/526406/python-time-to-age-part-2-timezones/526450#526450
def parse_ts(time_string):
    if not time_string:
        return None
    tz_offset_hours = int(time_string[-5:]) / 100  # we're ignoring the possibility of minutes here
    tz_delt = datetime.timedelta(hours=tz_offset_hours)
    return datetime.datetime.strptime(time_string[:-5], "%Y-%m-%dT%H:%M:%S") - tz_delt


def convert_ts(time_string):
    if not time_string:
        return None
    return parse_ts(time_string).strftime(FB_DATE_FORMAT)

def parse_date(date_string):
    if not date_string:
        return ''
    return datetime.datetime.strptime(date_string, "%m/%d/%Y").date()
