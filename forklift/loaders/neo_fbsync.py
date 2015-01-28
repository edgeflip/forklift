import sys
import requests
import logging
from collections import defaultdict
from forklift import facebook
from urlparse import urlparse

LOG = logging.getLogger(__name__)
DEFAULT_DELIMITER = ","


def urlload(url, **kwargs):
    """Load data from the given Facebook URL."""
    timeout = kwargs.pop('timeout') or 40
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


def extract_user_endpoint(endpoint, user_id, token):
    return urlload(get_user_endpoint(user_id, endpoint), access_token=token)


def get_user_endpoint(user_id, endpoint):
    return 'https://graph.facebook.com/v2.2/{}/{}'.format(user_id, endpoint)


def transform_stream_page(input_data, asid):
    output_lines = defaultdict()
    posts = (FeedPostFromJson(post) for post in input_data)
    delim = DEFAULT_DELIMITER
    for post in posts:
        output_lines['posts'].append(assemble_post_line(post, asid, delim))

    return output_lines


def transform_field(self, field, delim):
    if isinstance(field, basestring):
        return field.replace(delim, " ").replace("\n", " ").replace("\x00", "").encode('utf8', 'ignore')
    else:
        return str(field)


def assemble_post_line(post, asid, delim):
    post_fields = (
        asid,
        post.post_id,
        post.post_ts,
        post.post_type,
        post.post_app,
        post.post_from,
        post.post_link,
        post.post_link_domain,
        post.post_story,
        post.post_description,
        post.post_caption,
        post.post_message,
        len(post.like_ids) if hasattr(post, 'like_ids') else 0,
        len(post.comments) if hasattr(post, 'comments') else 0,
        len(post.to_ids) if hasattr(post, 'to_ids') else 0,
        len(post.commenters) if hasattr(post, 'commenters') else 0,
    )
    return tuple(transform_field(field, delim) for field in post_fields)

class FeedPostFromJson(object):
    date_format = "%Y-%m-%d %H:%M:%S"

    def __init__(self, post_json):
        self.post_id = str(post_json['id'])
        self.post_ts = facebook.utils.parse_ts(post_json['updated_time']).strftime(self.date_format)
        self.post_type = post_json['type']
        self.post_app = post_json['application']['id'] if 'application' in post_json else ""
        self.post_from = post_json['from']['id'] if 'from' in post_json else ""
        self.post_link = post_json.get('link', "")
        try:
            self.post_link_domain = urlparse(self.post_link).hostname if (self.post_link) else ""
        except ValueError: # handling invalid Ipv6 address errors
            self.post_link_domain = ""

        self.post_story = post_json.get('story', "")
        self.post_description = post_json.get('description', "")
        self.post_caption = post_json.get('caption', "")
        self.post_message = post_json.get('message', "")

        if 'to' in post_json and 'data' in post_json['to']:
            self.to_ids = set()
            self.to_ids.update([user['id'] for user in post_json['to']['data']])
        if 'likes' in post_json and 'data' in post_json['likes']:
            self.like_ids = set()
            self.like_ids.update([user['id'] for user in post_json['likes']['data']])
        if 'comments' in post_json and 'data' in post_json['comments']:
            self.comments = [{
                'comment_id': comment['id'],
                'commenter_id': comment['from']['id'],
                'message': comment['message'],
                'comment_ts': facebook.utils.parse_ts(comment['created_time']).strftime(self.date_format),
            } for comment in post_json['comments']['data']]
            self.commenters = defaultdict(list)
            for comment in self.comments:
                self.commenters[comment['commenter_id']].append(comment['message'])
        if 'place' in post_json:
            place = post_json['place']
            location = place.get('location', {})
            self.locale = {
                'locale_id': place.get('id'),
                'locale_name': place.get('name'),
                'locale_city': location.get('city'),
                'locale_state': location.get('state'),
                'locale_zip': location.get('zip'),
                'locale_country': location.get('country'),
                'locale_address': location.get('street'),
            }

def permissions(*args):
    return extract_user_endpoint('permissions', *args)


def user(*args):
    return extract_user_endpoint('', *args)


def activities(*args):
    return extract_user_endpoint('activities', *args)


def interests(*args):
    return extract_user_endpoint('interests', *args)


def likes(*args):
    return extract_user_endpoint('likes', *args)


def taggable_friends(*args):
    return extract_user_endpoint('taggable_friends', *args)


def statuses(*args):
    return extract_user_endpoint('statuses', *args)


def links(*args):
    return extract_user_endpoint('links', *args)


def photos(*args):
    return extract_user_endpoint('photos', *args)


def uploaded_photos(*args):
    return extract_user_endpoint('photos/uploaded', *args)


def videos(*args):
    return extract_user_endpoint('videos', *args)


def uploaded_videos(*args):
    return extract_user_endpoint('videos/uploaded', *args)
