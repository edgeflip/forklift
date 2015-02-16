import logging
from collections import defaultdict, namedtuple
import forklift.facebook.utils as facebook
from forklift.utils import get_or_create_efid
from urlparse import urlparse

LOG = logging.getLogger(__name__)
DEFAULT_DELIMITER = ","
FB_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
POSTS_TABLE = 'v2_posts'
EDGES_TABLE = 'v2_edges'
POST_LIKES_TABLE = 'v2_post_likes'
POST_COMMENTS_TABLE = 'v2_post_comments'
POST_TAGS_TABLE = 'v2_post_tags'
POST_AGGREGATES_TABLE = 'v2_post_aggregates'
USER_POST_AGGREGATES_TABLE = 'v2_user_post_aggregates'
USER_TIMELINE_AGGREGATES_TABLE = 'v2_user_timeline_aggregates'
POSTER_AGGREGATES_TABLE = 'v2_poster_aggregates'
USERS_TABLE = 'v2_users'
USER_AGGREGATES_TABLE = 'v2_user_aggregates'
USER_INTERESTS_TABLE = 'v2_user_interests'
USER_ACTIVITIES_TABLE = 'v2_user_activities'
USER_LIKES_TABLE = 'v2_user_likes'
USER_PERMISSIONS_TABLE = 'v2_user_permissions'
USER_FRIENDS_TABLE = 'v2_user_friends'
USER_LOCALES_TABLE = 'v2_user_locales'
USER_LANGUAGES_TABLE = 'v2_user_languages'


def get_user_endpoint(user_id, endpoint):
    return 'https://graph.facebook.com/v2.2/{}/{}'.format(user_id, endpoint)


def transform_stream(input_data, efid, appid, data_type, post_id, post_from):
    output_lines = defaultdict(list)
    posts = (FeedPostFromJson(post, data_type, appid) for post in input_data['data'])
    delim = DEFAULT_DELIMITER

    for post in posts:
        output_lines[POSTS_TABLE].append(assemble_post_line(post, efid, delim))
        if hasattr(post, 'like_ids'):
            output_lines[POST_LIKES_TABLE].extend(assemble_post_like_lines(post.post_id, post.likes, post.post_from, efid, delim))
        if hasattr(post, 'tagged_ids'):
            output_lines[POST_TAGS_TABLE].extend(assemble_post_tag_lines(post.post_id, post.tagged_ids, post.post_from, efid, delim))
        if hasattr(post, 'comments'):
            output_lines[POST_COMMENTS_TABLE].extend(assemble_comment_lines(post.post_id, post.comments, efid, appid, delim))

        output_lines[USER_LOCALES_TABLE].extend(assemble_locale_lines(post, efid, delim))

    return output_lines


def transform_taggable_friends(input_data, efid, appid, crawl_type, post_id, post_from):
    output_lines = defaultdict()

    for input_row in input_data['data']:
        friend_fields = (
            efid,
            input_row['name']
        )
        output_lines[USER_FRIENDS_TABLE].append(
            tuple(transform_field(field, DEFAULT_DELIMITER) for field in friend_fields)
        )

    return output_lines


def transform_permissions(input_data, efid, appid, crawl_type, post_id, post_from):
    output_lines = []

    for input_row in input_data['data']:
        permission_fields = (
            efid,
            input_row['permission'],
            input_row['status'],
        )
        output_lines.append(
            tuple(transform_field(field, DEFAULT_DELIMITER) for field in permission_fields)
        )

    return { USER_PERMISSIONS_TABLE: output_lines }


def transform_public_profile(input_data, efid, appid, crawl_type, post_id, post_from):
    profile_fields = (
        efid,
        input_data.get('email', ''),
        input_data.get('age_range', ''),
        facebook.parse_date(input_data.get('birthday', '')),
        input_data.get('first_name', ''),
        input_data.get('last_name', ''),
        input_data.get('gender', ''),
        input_data.get('timezone', ''),
        input_data.get('locale', ''),
        input_data.get('location', {}).get('id', ''),
        input_data.get('location', {}).get('name', ''),
        facebook.parse_ts(input_data['updated_time']).strftime(FB_DATE_FORMAT),
    )

    output_line = tuple(transform_field(field, DEFAULT_DELIMITER) for field in profile_fields)
    language_lines = (
        tuple(transform_field(field, DEFAULT_DELIMITER) for field in (efid, language['name']))
        for language in input_data.get('languages', [])
    )

    return {
        USERS_TABLE: [output_line],
        USER_LANGUAGES_TABLE: language_lines,
    }


def transform_activities(input_data, efid, appid, crawl_type, post_id, post_from):
    return {USER_ACTIVITIES_TABLE: assemble_page_lines(input_data, efid)}


def transform_page_likes(input_data, efid, appid, crawl_type, post_id, post_from):
    return {USER_LIKES_TABLE: assemble_page_lines(input_data, efid)}


def transform_interests(input_data, efid, appid, crawl_type, post_id, post_from):
    return {USER_INTERESTS_TABLE: assemble_page_lines(input_data, efid)}


def transform_post_comments(input_data, efid, appid, crawl_type, post_id, post_from):
    comments = (CommentFromJson(comment, appid) for comment in input_data)
    return {
        POST_COMMENTS_TABLE: assemble_comment_lines(post_id, comments, efid, appid, DEFAULT_DELIMITER)
    }


def transform_post_likes(input_data, efid, appid, crawl_type, post_id, post_from):
    likes = (get_or_create_efid(row['id'], appid) for row in input_data['data'])
    return {
        POST_LIKES_TABLE: assemble_post_like_lines(post_id, likes, get_or_create_efid(post_from, appid), efid, DEFAULT_DELIMITER)
    }


def transform_post_tags(input_data, efid, appid, crawl_type, post_id, post_from):
    tagged_ids = (get_or_create_efid(row['id'], appid) for row in input_data)
    return {
        POST_TAGS_TABLE: assemble_post_tag_lines(post_id, tagged_ids, get_or_create_efid(post_from, appid), efid, DEFAULT_DELIMITER)
    }


def transform_field(field, delim):
    if isinstance(field, basestring):
        return field.replace(delim, " ").replace("\n", " ").replace("\x00", "").encode('utf8', 'ignore')
    else:
        return str(field)


FBEndpoint = namedtuple('FBEndpoint', ['endpoint', 'transformer', 'entity_type'])
USER_ENTITY_TYPE = 'user'
POST_ENTITY_TYPE = 'post'

PRIMARY_KEYS = {
    POSTS_TABLE: ('post_id',),
    POST_LIKES_TABLE: ('post_id', 'liker_efid'),
    POST_COMMENTS_TABLE: ('comment_id',),
    POST_TAGS_TABLE: ('post_id', 'tagged_efid'),
    USER_INTERESTS_TABLE: ('efid', 'page_id'),
    USER_LIKES_TABLE: ('efid', 'page_id'),
    USER_ACTIVITIES_TABLE: ('efid', 'page_id'),
    USER_LANGUAGES_TABLE: ('efid',),
    USERS_TABLE: ('efid',),
    USER_PERMISSIONS_TABLE: ('efid', 'permission'),
    USER_FRIENDS_TABLE: ('efid',),
    USER_LOCALES_TABLE: ('post_id', 'tagged_efid'),
}


ENDPOINTS = {
    'statuses': FBEndpoint(
        endpoint='statuses',
        transformer=transform_stream,
        entity_type=USER_ENTITY_TYPE,
    ),
    'links': FBEndpoint(
        endpoint='links',
        transformer=transform_stream,
        entity_type=USER_ENTITY_TYPE,
    ),
    'photos': FBEndpoint(
        endpoint='photos',
        transformer=transform_stream,
        entity_type=USER_ENTITY_TYPE,
    ),
    'uploaded_photos': FBEndpoint(
        endpoint='photos/uploaded',
        transformer=transform_stream,
        entity_type=USER_ENTITY_TYPE,
    ),
    'videos': FBEndpoint(
        endpoint='videos',
        transformer=transform_stream,
        entity_type=USER_ENTITY_TYPE,
    ),
    'uploaded_videos': FBEndpoint(
        endpoint='videos/uploaded',
        transformer=transform_stream,
        entity_type=USER_ENTITY_TYPE,
    ),
    'permissions': FBEndpoint(
        endpoint='permissions',
        transformer=transform_permissions,
        entity_type=USER_ENTITY_TYPE,
    ),
    'public_profile': FBEndpoint(
        endpoint='',
        transformer=transform_public_profile,
        entity_type=USER_ENTITY_TYPE,
    ),
    'activities': FBEndpoint(
        endpoint='activities',
        transformer=transform_activities,
        entity_type=USER_ENTITY_TYPE,
    ),
    'interests': FBEndpoint(
        endpoint='interests',
        transformer=transform_interests,
        entity_type=USER_ENTITY_TYPE,
    ),
    'page_likes': FBEndpoint(
        endpoint='likes',
        transformer=transform_page_likes,
        entity_type=USER_ENTITY_TYPE,
    ),
    'taggable_friends': FBEndpoint(
        endpoint='taggable_friends',
        transformer=transform_taggable_friends,
        entity_type=USER_ENTITY_TYPE,
    ),
    'post_likes': FBEndpoint(
        endpoint='likes',
        transformer=transform_post_likes,
        entity_type=POST_ENTITY_TYPE,
    ),
    'post_comments': FBEndpoint(
        endpoint='comments',
        transformer=transform_post_comments,
        entity_type=POST_ENTITY_TYPE,
    ),
    'post_tags': FBEndpoint(
        endpoint='tags',
        transformer=transform_post_tags,
        entity_type=POST_ENTITY_TYPE,
    ),
}

def assemble_page_lines(input_data, efid):
    output_lines = []

    for input_row in input_data['data']:
        page_fields = (
            efid,
            input_row['id'],
            input_row['name'],
            input_row['category'],
        )
        output_lines.append(
            tuple(transform_field(field, DEFAULT_DELIMITER) for field in page_fields)
        )

    return output_lines


def assemble_post_like_lines(post_id, like_ids, post_from, efid, delim):
    return [
        [transform_field(f, delim) for f in (post_id, user_id, post_from)]
        for user_id in like_ids
    ]

def assemble_post_tag_lines(post_id, tagged_ids, post_from, efid, delim):
    return [
        [transform_field(f, delim) for f in (post_id, user_id, post_from)]
        for user_id in tagged_ids
    ]


def assemble_comment_lines(post_id, comments, efid, appid, delim):
    comment_lines = []

    for comment_json in comments:
        comment = CommentFromJson(comment_json, appid)
        comment_fields = (
            post_id,
            efid,
            comment.comment_id,
            comment.commenter_id,
            comment.message,
            comment.comment_ts,
            comment.like_count,
            comment.user_likes,
        )
        comment_lines.append(
            [transform_field(f, delim) for f in comment_fields]
        )

    return comment_lines


def assemble_locale_lines(post, efid, delim):
    locale_lines = []
    if not hasattr(post, 'locale'):
        return locale_lines

    for tagged_asid in list(getattr(post, 'tagged_ids', [])) + [efid]:
        locale_fields = (
            post.locale.get('locale_id', ''),
            post.locale.get('locale_name', ''),
            post.locale.get('locale_city', ''),
            post.locale.get('locale_state', ''),
            post.locale.get('locale_zip', ''),
            post.locale.get('locale_country', ''),
            post.locale.get('locale_address', ''),
            post.post_id,
            tagged_asid
        )
        locale_lines.append(
            [transform_field(f, delim) for f in locale_fields]
        )

    return locale_lines


def assemble_post_line(post, efid, delim):
    post_fields = (
        efid,
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
    )
    return list(transform_field(field, delim) for field in post_fields)


def datediff_expression():
    return "datediff('year', birthday, getdate())"



class CommentFromJson(object):
    def __init__(self, comment_json, appid):
        self.comment_id = get_or_create_efid(comment_json.get('id'), appid)
        self.commenter_id = get_or_create_efid(comment_json.get('from', {}).get('id'), appid)
        self.message = comment_json.get('message')
        self.comment_ts = facebook.parse_ts(comment_json.get('created_time')).strftime(FB_DATE_FORMAT)
        self.like_count = comment_json.get('like_count')
        self.user_likes = comment_json.get('user_likes')


class FeedPostFromJson(object):

    def __init__(self, post_json, data_type, appid):
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
            self.comments = post_json['comments']['data']
            self.commenters = defaultdict(list)
            for comment in self.comments:
                self.commenters[comment.get('commenter_id')].append(comment.get('message'))
        if 'place' in post_json:
            place = post_json['place']
            location = place.get('location', {})
            if isinstance(location, dict):
                self.locale = {
                    'locale_id': place.get('id'),
                    'locale_name': place.get('name'),
                    'locale_city': location.get('city'),
                    'locale_state': location.get('state'),
                    'locale_zip': location.get('zip'),
                    'locale_country': location.get('country'),
                    'locale_address': location.get('street'),
                }
            else:
                self.locale = {
                    'locale_name': location,
                }
