from forklift.db.utils import (
    cache_table,
    copy_to_redshift,
    deploy_table,
    drop_table_if_exists,
)
import itertools
import logging
logger = logging.getLogger(__name__)


def staging_table_name(table):
    return '{}_staging'.format(table)


def old_table_name(table):
    return '{}_old'.format(table)


VISITS_AGG_TABLE = 'visithourly'
FBID_AGG_TABLE = 'fbidhourly'
FRIEND_AGG_TABLE = 'friendhourly'
EVENT_AGG_TABLE = 'eventhourly'
CAMPAIGN_HOURLY_TABLE = 'campaignhourly'
CAMPAIGN_ROLLUP_TABLE = 'campaignrollups'
CLIENT_ROLLUP_TABLE = 'clientrollups'


METRICS = {
    FBID_AGG_TABLE: [
        ('uniq_users_authorized', "COUNT(DISTINCT CASE WHEN type='authorized' THEN visitors.fbid ELSE NULL END)"),
        ('users_generated_faces', "COUNT(DISTINCT CASE WHEN type='generated' THEN visitors.fbid ELSE NULL END)"),
        ('users_facepage_rendered', "COUNT(DISTINCT CASE WHEN type='faces_page_rendered' THEN visitors.fbid ELSE NULL END)"),
        ('users_shown_faces', "COUNT(DISTINCT CASE WHEN type='shown' THEN visitors.fbid ELSE NULL END)"),
        ('users_who_shared', "COUNT(DISTINCT CASE WHEN type='shared' THEN visitors.fbid ELSE NULL END)"),
    ],
    VISITS_AGG_TABLE: [
        ('initial_redirects', "COUNT(DISTINCT CASE WHEN type='initial_redirect' or type='incoming_redirect' THEN visit_id ELSE NULL END)"),
        ('visits', "COUNT(DISTINCT CASE WHEN type='incoming_redirect' THEN visit_id ELSE NULL END)"),
        ('authorized_visits', "COUNT(DISTINCT CASE WHEN type='authorized' THEN visit_id ELSE NULL END)"),
        ('failed_visits', "COUNT(DISTINCT CASE WHEN type='initial_redirect' or type='incoming_redirect' THEN visit_id ELSE NULL END) - COUNT(DISTINCT CASE WHEN type='authorized' THEN visit_id ELSE NULL END)"),
        ('visits_generated_faces', "COUNT(DISTINCT CASE WHEN type='generated' THEN visit_id ELSE NULL END)"),
        ('visits_facepage_rendered', "COUNT(DISTINCT CASE WHEN type='faces_page_rendered' THEN visit_id ELSE NULL END)"),
        ('visits_shown_faces', "COUNT(DISTINCT CASE WHEN type='shown' THEN visit_id ELSE NULL END)"),
        ('visits_with_share_clicks', "COUNT(DISTINCT CASE WHEN type='share_click' THEN visit_id ELSE NULL END)"),
        ('visits_with_shares', "COUNT(DISTINCT CASE WHEN type='shared' THEN visit_id ELSE NULL END)"),
    ],
    FRIEND_AGG_TABLE: [
        ('distinct_faces_shown', "COUNT(DISTINCT CASE WHEN type='shown' THEN friend_fbid ELSE NULL END)"),
        ('audience', "COUNT(DISTINCT CASE WHEN type='shared' THEN friend_fbid ELSE NULL END)"),
    ],
    EVENT_AGG_TABLE: [
        ('clicks', "SUM(CASE WHEN events.type='button_click' THEN 1 ELSE 0 END)"),
        ('auth_fails', "SUM(CASE WHEN (events.type='auth_fail' or events.type='oauth_declined') THEN 1 ELSE 0 END)"),
        ('total_faces_shown', "SUM(CASE WHEN events.type='shown' THEN 1 ELSE 0 END)"),
        ('total_shares', "SUM(CASE WHEN events.type='shared' THEN 1 ELSE 0 END)"),
        ('clickbacks', "SUM(CASE WHEN events.type='clickback' THEN 1 ELSE 0 END)"),
    ],
}
ALL_METRICS = list(itertools.chain.from_iterable(METRICS.values()))
METRICS[CAMPAIGN_HOURLY_TABLE] = [(name, "coalesce(max({}), 0)".format(name)) for (name, _) in ALL_METRICS]

FROM_CLAUSE = """from events
inner join visits using (visit_id)
inner join visitors using (visitor_id)
inner join campaigns using (campaign_id)
inner join clients using (client_id)
inner join campaign_properties using (campaign_id)
inner join campaigns root_campaign on (root_campaign.campaign_id = campaign_properties.root_campaign_id)
left join edgeflip_fbids on (edgeflip_fbids.fbid = visitors.fbid)
WHERE edgeflip_fbids.fbid is null
AND campaigns.delete_dt is null
"""

AGGREGATE_QUERIES = (
    (FBID_AGG_TABLE, """
SELECT
    campaign_id,
    date_trunc('hour', first_instance) as hour,
    {metric_expressions}
    FROM
    (
        select
            root_campaign.campaign_id,
            visitors.fbid,
            events.type,
            min(events.event_datetime) as first_instance
            {from_clause}
        GROUP BY root_campaign.campaign_id, visitors.fbid, events.type
    ) mins
    join visitors using (fbid)
    GROUP BY 1, 2
"""),
    (VISITS_AGG_TABLE, """
SELECT
    root_campaign.campaign_id,
    date_trunc('hour', visits.created) as hour,
    {metric_expressions}
    {from_clause}
    GROUP BY root_campaign.campaign_id, hour
"""),
    (FRIEND_AGG_TABLE, """
SELECT
    campaign_id,
    date_trunc('hour', first_instance) as hour,
    {metric_expressions}
    FROM
    (
        select
            root_campaign.campaign_id,
            events.friend_fbid,
            events.type,
            min(events.event_datetime) as first_instance
            {from_clause}
        GROUP BY root_campaign.campaign_id, events.friend_fbid, events.type
    ) mins
    GROUP BY 1, 2
"""),
    (EVENT_AGG_TABLE, """
SELECT
    root_campaign.campaign_id,
    date_trunc('hour', events.event_datetime) as hour,
    {metric_expressions}
    {from_clause}
    GROUP BY root_campaign.campaign_id, hour
"""),
    (CAMPAIGN_HOURLY_TABLE, """
SELECT
    root_campaign.campaign_id,
    timebox.hour,
    {metric_expressions}
    FROM
        (
            select date_trunc('hour', event_datetime) as hour, campaign_id from events
            union
            select date_trunc('hour', visits.created) as hour, campaign_id from visits join events using (visit_id)
        ) timebox
        inner join campaign_properties using (campaign_id)
        inner join campaigns root_campaign on (root_campaign.campaign_id = campaign_properties.root_campaign_id)
        left join visithourly v on (v.campaign_id = root_campaign.campaign_id and v.hour = timebox.hour)
        left join fbidhourly fb on (fb.campaign_id = root_campaign.campaign_id and fb.hour = timebox.hour)
        left join friendhourly fr on (fr.campaign_id = root_campaign.campaign_id and fr.hour = timebox.hour)
        left join eventhourly e on (e.campaign_id = root_campaign.campaign_id and e.hour = timebox.hour)
    GROUP BY 1, 2
"""),
    (CAMPAIGN_ROLLUP_TABLE, """
SELECT
    root_campaign.campaign_id,
    {metric_expressions}
    {from_clause}
    GROUP BY root_campaign.campaign_id
"""),
    (CLIENT_ROLLUP_TABLE, """
SELECT
    clients.client_id,
    {metric_expressions}
    {from_clause}
    GROUP BY clients.client_id
""")
)


RAW_TABLES = {
    'visits': 'visit_id',
    'visitors': 'visitor_id',
    'campaigns': 'campaign_id',
    'events': 'event_id',
    'clients': 'client_id',
    'campaign_properties': 'campaign_property_id',
    'user_clients': 'user_client_id',
}

CACHED_RAW_TABLES = {'clients', 'campaigns'}

def refresh_aggregate_table(engine, table_name, query):
    with engine.connect() as connection:
        staging_table = staging_table_name(table_name)
        drop_table_if_exists(staging_table, connection)
        metrics = METRICS.get(table_name, ALL_METRICS)
        bound_query = query.format(
            from_clause=FROM_CLAUSE,
            metric_expressions=",\n\t".join("{} as {}".format(expr, name) for (name, expr) in metrics),
            fbid_aggs=FBID_AGG_TABLE,
            visit_aggs=VISITS_AGG_TABLE,
            friend_aggs=FRIEND_AGG_TABLE,
            event_aggs=EVENT_AGG_TABLE,
        )
        full_statement = 'CREATE TABLE {} AS {}'.format(staging_table, bound_query)
        logger.debug('Calculating aggregates for {}'.format(table_name))
        with connection.begin():
            connection.execute(full_statement)
    logger.debug('Deploying {} aggregates to Redshift'.format(table_name))
    deploy_table(
        table_name,
        staging_table,
        old_table_name(table_name),
        engine
    )
    logger.debug('Done deploying {} aggregates to Redshift'.format(table_name))


def process(
    rds_source_engine,
    redshift_engine,
    cache_engine,
    delim='|',
    raw_tables=None,
    aggregate_queries=None,
    cached_raw_tables=None,
):
    raw_tables = raw_tables if raw_tables is not None else RAW_TABLES
    aggregate_queries = aggregate_queries if aggregate_queries is not None else AGGREGATE_QUERIES
    cached_raw_tables = cached_raw_tables if cached_raw_tables is not None else CACHED_RAW_TABLES

    for table, table_id in raw_tables.iteritems():
        copy_to_redshift(
            rds_source_engine,
            redshift_engine,
            staging_table_name(table),
            table,
            old_table_name(table),
            delim
        )

    for (aggregate_table, aggregate_query) in aggregate_queries:
        with redshift_engine.connect() as redshift_connection:
            refresh_aggregate_table(
                redshift_connection,
                aggregate_table,
                aggregate_query
            )
        cache_table(
            redshift_engine,
            cache_engine,
            staging_table_name(aggregate_table),
            aggregate_table,
            old_table_name(aggregate_table),
            delim
        )

    # The reporting dashboard also needs to refer to a subset of the raw tables,
    # so stick them in the reporting cache
    for cached_table in cached_raw_tables:
        cache_table(
            redshift_engine,
            cache_engine,
            staging_table_name(cached_table),
            cached_table,
            old_table_name(cached_table),
            delim
        )
