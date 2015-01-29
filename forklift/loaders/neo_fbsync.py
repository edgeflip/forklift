import sys
import requests
import logging
from collections import defaultdict
import forklift.facebook.utils as facebook
from urlparse import urlparse

LOG = logging.getLogger(__name__)
DEFAULT_DELIMITER = ","
FB_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def urlload(url, **kwargs):
    """Load data from the given Facebook URL."""
    timeout = kwargs.pop('timeout', 40)
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


def transform_stream(input_data, entity_id, data_type):
    output_lines = defaultdict(list)
    posts = (FeedPostFromJson(post, data_type) for post in input_data['data'])
    delim = DEFAULT_DELIMITER

    for post in posts:
        output_lines['posts'].append(assemble_post_line(post, entity_id, delim))
        output_lines['post_likes'].extend(assemble_post_like_lines(post, entity_id, delim))
        output_lines['post_tags'].extend(assemble_post_tag_lines(post, entity_id, delim))
        output_lines['comments'].extend(assemble_comment_lines(post, entity_id, delim))
        output_lines['locales'].extend(assemble_locale_lines(post, entity_id, delim))

    return output_lines


def transform_taggable_friends(input_data, asid):
    output_lines = defaultdict()

    for input_row in input_data['data']:
        friend_fields = (
            asid,
            input_row['name']
        )
        output_lines['taggable_friends'].append(
            tuple(transform_field(field, DEFAULT_DELIMITER) for field in friend_fields)
        )

    return output_lines


def transform_permissions(input_data, asid):
    output_lines = []

    for input_row in input_data['data']:
        permission_fields = (
            asid,
            input_row['permission'],
            input_row['status'],
        )
        output_lines.append(
            tuple(transform_field(field, DEFAULT_DELIMITER) for field in permission_fields)
        )

    return { 'permissions': output_lines }


def transform_public_profile(input_data, asid):
    output_lines = []

    profile_fields = (
        asid,
        input_data.get('email', ''),
        input_data.get('age_range', ''),
        input_data.get('birthday', ''),
        input_data.get('first_name', ''),
        input_data.get('last_name', ''),
        input_data.get('gender', ''),
        input_data.get('timezone', ''),
        input_data.get('locale', ''),
        input_data.get('location', {}).get('id', ''),
        input_data.get('location', {}).get('name', ''),
        facebook.parse_ts(input_data['updated_time']).strftime(FB_DATE_FORMAT),
    )

    output_lines.append(
        tuple(transform_field(field, DEFAULT_DELIMITER) for field in profile_fields)
    )

    return { 'users': output_lines }


def transform_pages(input_data, asid):
    output_lines = []

    for input_row in input_data['data']:
        page_fields = (
            asid,
            input_row['id'],
            input_row['name'],
            input_row['category'],
        )
        output_lines.append(
            tuple(transform_field(field, DEFAULT_DELIMITER) for field in page_fields)
        )

    return output_lines


def transform_activities(input_data, asid):
    return {'user_activities': transform_pages(input_data, asid)}


def transform_page_likes(input_data, asid):
    return {'user_likes': transform_pages(input_data, asid)}


def transform_interests(input_data, asid):
    return {'user_interests': transform_pages(input_data, asid)}


def transform_field(field, delim):
    if isinstance(field, basestring):
        return field.replace(delim, " ").replace("\n", " ").replace("\x00", "").encode('utf8', 'ignore')
    else:
        return str(field)


def assemble_post_like_lines(post, post_id, delim):
    output_lines = []
    if hasattr(post, 'like_ids'):
        for user_id in post.like_ids:
            like_fields = (
                post.post_id,
                user_id,
                post.post_from,
            )
            output_lines.append(
                transform_field(f, delim) for f in like_fields
            )
    return output_lines

def assemble_post_tag_lines(post, post_id, delim):
    output_lines = []
    if hasattr(post, 'tagged_ids'):
        for user_id in post.tagged_ids:
            tag_fields = (
                post.post_id,
                user_id,
                post.post_from,
            )
            output_lines.append(
                transform_field(f, delim) for f in tag_fields
            )
    return output_lines

def assemble_user_post_lines(post, asid, delim):
    link_lines = []
    user_ids = None
    if hasattr(post, 'tagged_ids'):
        user_ids = post.tagged_ids
    if hasattr(post, 'like_ids'):
        if user_ids:
            user_ids = user_ids.union(post.like_ids)
        else:
            user_ids = post.like_ids
    if hasattr(post, 'commenters'):
        commenter_ids = post.commenters.keys()
        if user_ids:
            user_ids = user_ids.union(commenter_ids)
        else:
            user_ids = commenter_ids
    if user_ids:
        for user_id in user_ids:
            has_to = "1" if hasattr(post, 'tagged_ids') and user_id in post.tagged_ids else ""
            has_like = "1" if hasattr(post, 'like_ids') and user_id in post.like_ids else ""
            if hasattr(post, 'commenters') and user_id in commenter_ids:
                has_comm = "1"
                num_comments = str(len(post.commenters[user_id]))
            else:
                has_comm = ""
                num_comments = "0"

            link_fields = (
                post.post_id,
                user_id,
                asid,
                has_to,
                has_like,
                has_comm,
                num_comments,
            )
            link_lines.append(
                transform_field(f, delim) for f in link_fields
            )
    return link_lines


def assemble_comment_lines(post, asid, delim):
    comment_lines = []
    if not hasattr(post, 'comments'):
        return comment_lines

    for comment in post.comments:
        comment_fields = (
            post.post_id,
            asid,
            comment['comment_id'],
            comment['commenter_id'],
            comment['message'],
            comment['comment_ts'],
            comment['like_count'],
            comment['user_likes'],
        )
        comment_lines.append(
            transform_field(f, delim) for f in comment_fields
        )

    return comment_lines


def assemble_locale_lines(post, asid, delim):
    locale_lines = []
    if not hasattr(post, 'locale'):
        return locale_lines
    for tagged_asid in list(getattr(post, 'tagged_ids', [])) + [asid]:
        locale_fields = (
            post.locale['locale_id'],
            post.locale['locale_name'],
            post.locale['locale_city'],
            post.locale['locale_state'],
            post.locale['locale_zip'],
            post.locale['locale_country'],
            post.locale['locale_address'],
            tagged_asid
        )
        locale_lines.append(
            transform_field(f, delim) for f in locale_fields
        )

    return locale_lines


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
        len(getattr(post, 'like_ids', [])),
        len(getattr(post, 'comments', [])),
        len(getattr(post, 'tagged_ids', [])),
        len(getattr(post, 'commenters', [])),
    )
    return tuple(transform_field(field, delim) for field in post_fields)

class FeedPostFromJson(object):

    def __init__(self, post_json, data_type):
        self.post_id = str(post_json['id'])
        if 'updated_time' in post_json:
            self.post_ts = facebook.parse_ts(post_json['updated_time']).strftime(FB_DATE_FORMAT)
        elif 'created_time' in post_json:
            self.post_ts = facebook.parse_ts(post_json['created_time']).strftime(FB_DATE_FORMAT)
        self.post_type = data_type
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

        if 'tags' in post_json and 'data' in post_json['tags']:
            self.tagged_ids = set()
            self.tagged_ids.update([user['id'] for user in post_json['tags']['data'] if 'id' in user])
        if 'likes' in post_json and 'data' in post_json['likes']:
            self.like_ids = set()
            self.like_ids.update([user['id'] for user in post_json['likes']['data']])
        if 'comments' in post_json and 'data' in post_json['comments']:
            self.comments = [{
                'comment_id': comment.get('id'),
                'commenter_id': comment.get('from', {}).get('id'),
                'message': comment.get('message'),
                'comment_ts': facebook.parse_ts(comment.get('created_time')).strftime(FB_DATE_FORMAT),
                'like_count': comment.get('like_count'),
                'user_likes': comment.get('user_likes'),
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
