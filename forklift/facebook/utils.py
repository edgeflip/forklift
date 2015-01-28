import datetime
import logging
import requests
import sys

LOG = logging.getLogger(__name__)


def urlload(url, **kwargs):
    """Load data from the given Facebook URL."""
    timeout = kwargs.pop('timeout') or 30
    try:
        return requests.get(url, params=kwargs, timeout=timeout).json()
    except IOError as exc:
        exc_type, exc_value, trace = sys.exc_info()
        LOG.warning(
            "Error opening URL %s %r", url, getattr(exc, 'reason', ''),
            exc_info=True
        )
        try:
            original_msg = exc.read()
        except Exception:
            pass
        else:
            if original_msg:
                LOG.warning("Returned error message was: %s", original_msg)
        raise exc_type, exc_value, trace


# Despite what the docs say, datetime.strptime() format doesn't like %z
# see: http://stackoverflow.com/questions/526406/python-time-to-age-part-2-timezones/526450#526450
def parse_ts(time_string):
    tz_offset_hours = int(time_string[-5:]) / 100  # we're ignoring the possibility of minutes here
    tz_delt = datetime.timedelta(hours=tz_offset_hours)
    return datetime.datetime.strptime(time_string[:-5], "%Y-%m-%dT%H:%M:%S") - tz_delt
