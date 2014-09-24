from forklift.db.utils import (
    cache_table,
    copy_to_redshift,
    deploy_table,
    drop_table_if_exists,
)
import logging
logger = logging.getLogger(__name__)

OUR_IP_STRING = ','.join("'{}'".format(ip) for ip in ('38.88.227.194',))


def staging_table_name(table):
    return '{}_staging'.format(table)


def old_table_name(table):
    return '{}_staging'.format(table)


def metric_expressions():
    return """
        COUNT(DISTINCT CASE WHEN t.type='initial_redirect' or t.type='incoming_redirect' THEN t.visit_id ELSE NULL END) AS initial_redirects,
        COUNT(DISTINCT CASE WHEN t.type='incoming_redirect' THEN t.visit_id ELSE NULL END) AS visits,
        SUM(CASE WHEN t.type='button_click' THEN 1 ELSE 0 END) AS clicks,
        COUNT(DISTINCT CASE WHEN t.type='authorized' THEN t.visit_id ELSE NULL END) AS authorized_visits,
        COUNT(DISTINCT fbid) AS uniq_users_authorized,
        COUNT(DISTINCT CASE WHEN t.type='initial_redirect' or t.type='incoming_redirect' THEN t.visit_id ELSE NULL END) - COUNT(DISTINCT CASE WHEN t.type='authorized' THEN t.visit_id ELSE NULL END) as failed_visits,
        SUM(CASE WHEN (t.type='auth_fail' or t.type='oauth_declined') THEN 1 ELSE 0 END) AS auth_fails,
        COUNT(DISTINCT CASE WHEN t.type='generated' THEN visit_id ELSE NULL END) AS visits_generated_faces,
        COUNT(DISTINCT CASE WHEN t.type='generated' THEN fbid ELSE NULL END) AS users_generated_faces,
        COUNT(DISTINCT CASE WHEN t.type='faces_page_rendered' THEN visit_id ELSE NULL END) AS visits_facepage_rendered,
        COUNT(DISTINCT CASE WHEN t.type='faces_page_rendered' THEN fbid ELSE NULL END) AS users_facepage_rendered,
        COUNT(DISTINCT CASE WHEN t.type='shown' THEN visit_id ELSE NULL END) AS visits_shown_faces,
        COUNT(DISTINCT CASE WHEN t.type='shown' THEN fbid ELSE NULL END) AS users_shown_faces,
        SUM(CASE WHEN t.type='shown' THEN 1 ELSE 0 END) AS total_faces_shown,
        COUNT(DISTINCT CASE WHEN t.type='shown' THEN t.friend_fbid ELSE NULL END) AS distinct_faces_shown,
        COUNT(DISTINCT CASE WHEN t.type='share_click' THEN t.visit_id ELSE NULL END) as visits_with_share_clicks,
        COUNT(DISTINCT CASE WHEN t.type='shared' THEN visit_id ELSE NULL END) AS visits_with_shares,
        COUNT(DISTINCT CASE WHEN t.type='shared' THEN fbid ELSE NULL END) AS users_who_shared,
        COUNT(DISTINCT CASE WHEN t.type='shared' THEN t.friend_fbid ELSE NULL END) AS audience,
        SUM(CASE WHEN t.type='shared' THEN 1 ELSE 0 END) AS total_shares,
        SUM(CASE WHEN t.type='clickback' THEN 1 ELSE 0 END) AS clickbacks
    """

AGGREGATES = {
    'campaignhourly': """
        CREATE TABLE {} AS
        SELECT
            root_campaign.campaign_id,
            date_trunc('hour', t.updated) as hour,
            {}
            from events t
            inner join visits using (visit_id)
            inner join visitors v using (visitor_id)
            inner join campaigns using (campaign_id)
            inner join clients cl using (client_id)
            inner join campaign_properties using (campaign_id)
            inner join campaigns root_campaign on (root_campaign.campaign_id = campaign_properties.root_campaign_id)
            WHERE visits.ip not in ({})
            AND campaigns.delete_dt is null
            GROUP BY root_campaign.campaign_id, hour
    """,
    'campaignrollups': """
        CREATE TABLE {} AS
        SELECT
            root_campaign.campaign_id,
            {}
            from events t
            inner join visits using (visit_id)
            inner join visitors v using (visitor_id)
            inner join campaigns using (campaign_id)
            inner join clients cl using (client_id)
            inner join campaign_properties using (campaign_id)
            inner join campaigns root_campaign on (root_campaign.campaign_id = campaign_properties.root_campaign_id)
            WHERE visits.ip not in ({})
            AND campaigns.delete_dt is null
            GROUP BY root_campaign.campaign_id
    """,
    'clientrollups': """
        CREATE TABLE {} AS
        SELECT
            client_id,
            {}
            from events t
            inner join visits using (visit_id)
            inner join visitors v using (visitor_id)
            inner join campaigns using (campaign_id)
            inner join clients cl using (client_id)
            WHERE visits.ip not in ({})
            AND campaigns.delete_dt is null
            GROUP BY client_id
        """
}


def refresh_aggregate_table(connection, table_name, query):
    staging_table = staging_table_name(table_name)
    drop_table_if_exists(staging_table, connection)
    bound_query = query.format(
        staging_table,
        metric_expressions(),
        OUR_IP_STRING
    )
    logger.debug('Calculating aggregates for {}'.format(table_name))
    connection.execute(bound_query)
    logger.debug('Deploying {} aggregates to Redshift'.format(table_name))
    deploy_table(
        table_name,
        staging_table,
        old_table_name(table_name),
        connection
    )
    logger.debug('Done deploying {} aggregates to Redshift'.format(table_name))


def extract(redshift_engine):
    """ main for syncing with RDS """

    for table, table_id in [
        ('visits', 'visit_id'),
        ('visitors', 'visitor_id'),
        ('campaigns', 'campaign_id'),
        ('events', 'event_id'),
        ('clients', 'client_id'),
        ('campaign_properties', 'campaign_property_id'),
        ('user_clients', 'user_client_id'),
    ]:
        logger.debug('Uploading {} ..'.format(table))
        copy_to_redshift(table)
        logger.debug('Done.')  # poor man's timer

    with redshift_engine.connect() as redshift_connection:
        for (aggregate_table, aggregate_query) in AGGREGATES:
            refresh_aggregate_table(
                redshift_connection,
                aggregate_table,
                aggregate_query
            )
            cache_table(
                staging_table_name(aggregate_table),
                aggregate_table,
                old_table_name(aggregate_table)
            )
