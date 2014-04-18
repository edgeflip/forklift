from .classes import Fact, Dimension
from forklift.settings import IP_SLUG, FBID_SLUG, VISIT_SLUG, FRIEND_SLUG, MISC_SLUG
from sqlalchemy import Integer, BigInteger, DateTime, String


class HourlyAggregateTable(object):
    slug = None

    common_dimensions = (
        Dimension(
            slug='campaign_id',
            pretty_name='Campaign ID',
            column_name='campaign_id',
            source_table='events',
            datatype=Integer,
        ),
        Dimension(
            slug='hour',
            pretty_name='Hour',
            column_name='hour',
            datatype=DateTime
        )
    )
    extra_dimensions = ()


    @classmethod
    def tablename(cls):
        return '{}_facts_hourly'.format(cls.slug)


    @classmethod
    def columns(cls):
        return cls.dimensions() + cls.facts


    @classmethod
    def dimensions(cls):
        return cls.common_dimensions + cls.extra_dimensions


class FbidFactsHourly(HourlyAggregateTable):
    slug = FBID_SLUG
    facts = (
        Fact(
            slug='fbids_authorized',
            pretty_name='Unique Authorized Users',
            expression="count(distinct case when events.type='authorized' then visitors.fbid else null end)",
        ),
        Fact(
            slug='fbids_generated_friends',
            pretty_name='Users generated Friend Suggestions',
            expression="count(distinct case when events.type='generated' then visitors.fbid else null end)",
        ),
        Fact(
            slug='fbids_shown_friends',
            pretty_name='Users shown Friend Suggestions',
            expression="count(distinct case when events.type='shown' then visitors.fbid else null end)",
        ),
        Fact(
            slug='fbids_face_pages',
            pretty_name='Users shown Faces Page',
            expression="count(distinct case when events.type='faces_page_rendered' then visitors.fbid else null end)",
        ),
        Fact(
            slug='fbids_shared',
            pretty_name='Users who shared',
            expression="count(distinct case when events.type='shared' then visitors.fbid else null end)",
        ),
    )

    extra_dimensions = (
        Dimension(
            slug=FBID_SLUG,
            pretty_name='FBID',
            column_name='fbid',
            source_table='visitors',
            datatype=BigInteger
        ),
    )


class FriendFbidFactsHourly(HourlyAggregateTable):
    slug = FRIEND_SLUG
    facts = (
        Fact(
            slug='friends_shared_with',
            pretty_name='Unique Friends shared with',
            expression="count(distinct case when events.type='shared' then events.friend_fbid else null end)",
        ),
    )

    extra_dimensions = (
        Dimension(
            slug=FRIEND_SLUG,
            pretty_name='Friend FBID',
            column_name='friend_fbid',
            source_table='events',
            datatype=BigInteger,
        ),
    )


class VisitFactsHourly(HourlyAggregateTable):
    slug = VISIT_SLUG
    facts = (
        Fact(
            slug='visit_ids',
            pretty_name='Visit Ids',
            expression="count(distinct case when events.type='heartbeat' then events.visit_id else null end)",
        ),
        Fact(
            slug='visits_declined_auth',
            pretty_name='Declined Authorizations',
            expression="count(distinct case when (events.type='auth_fail' or events.type='oauth_declined') then events.visit_id else 0 end)",
        ),
        Fact(
            slug='visits_shown_friend_sugg',
            pretty_name='Visits shown Friend Suggestions',
            expression="count(distinct case when events.type='shown' then events.visit_id else null end)",
        ),
        Fact(
            slug='authorized_visits',
            pretty_name='Authorized Visits',
            expression="count(distinct case when events.type='authorized' then events.visit_id else null end)",
        ),
        Fact(
            slug='visits_with_shares',
            pretty_name='Visits with shares',
            expression="count(distinct case when events.type='shared' then events.visit_id else null end)",
        ),
    )

    extra_dimensions = (
        Dimension(
            slug=VISIT_SLUG,
            pretty_name='Visit',
            column_name='visit_id',
            source_table='events',
            datatype=BigInteger,
        ),
    )


class MiscFactsHourly(HourlyAggregateTable):
    slug = MISC_SLUG
    facts = (
        Fact(
            slug='visitors',
            pretty_name='Visitors',
            expression="sum(case when events.type='heartbeat' then 1 else 0 end)",
        ),
        Fact(
            slug='no_friends',
            pretty_name='No Friends',
            expression="sum(case when events.type='no_friends_error' then 1 else 0 end)",
        ),
        Fact(
            slug='declined_auth',
            pretty_name='Declined Authorizations',
            expression="sum(case when (events.type='auth_fail' or events.type='oauth_declined') then 1 else 0 end)",
        ),
        Fact(
            slug='authorizations',
            pretty_name='Authorizations',
            expression="sum(case when events.type='authorized' then 1 else 0 end)",
        ),
        Fact(
            slug='shares_failed',
            pretty_name='Shares Failed',
            expression="sum(case when events.type='share_fail' then 1 else 0 end)",
        ),
        Fact(
            slug='clickbacks',
            pretty_name='Clickbacks',
            expression="sum(case when events.type='clickback' then 1 else 0 end)",
        ),
    )


class IpFactsHourly(HourlyAggregateTable):
    slug = IP_SLUG
    facts = (
        Fact(
            slug='ip_session_start',
            pretty_name='IP Session Start',
            expression="count(distinct case when events.type='session_start' then ip else null end)",
        ),
        Fact(
            slug='ip_cookies',
            pretty_name='IP Cookies',
            expression="count(distinct case when events.type='cookies_enabled' then ip else null end)",
        ),
        Fact(
            slug='ip_visitors',
            pretty_name='IP Visitors',
            expression="count(distinct case when events.type='heartbeat' then ip else null end)",
        ),
        Fact(
            slug='ips_declined_auth',
            pretty_name='IPs Declined Authorization',
            expression="count(distinct case when (events.type='auth_fail' or events.type='oauth_declined' ) then ip else null end)",
        ),
        Fact(
            slug='ips_authorized',
            pretty_name='IPs Authorized',
            expression="count(distinct case when events.type='authorized' then ip else null end)",
        ),
    )

    extra_dimensions = (
        Dimension(
            slug=IP_SLUG,
            pretty_name='IP Address',
            column_name='ip',
            source_table='visits',
            datatype=String,
        ),
    )

