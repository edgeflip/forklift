"""Initial tables

Revision ID: e8e9af3683
Revises: None
Create Date: 2014-04-18 15:52:40.048224

"""

# revision identifiers, used by Alembic.
revision = 'e8e9af3683'
down_revision = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('ip_facts_hourly',
    sa.Column('campaign_id', sa.Integer(), nullable=False),
    sa.Column('hour', sa.DateTime(), nullable=False),
    sa.Column('ip', sa.String(), nullable=False),
    sa.Column('ip_session_start', sa.Integer(), nullable=True),
    sa.Column('ip_cookies', sa.Integer(), nullable=True),
    sa.Column('ip_visitors', sa.Integer(), nullable=True),
    sa.Column('ips_declined_auth', sa.Integer(), nullable=True),
    sa.Column('ips_authorized', sa.Integer(), nullable=True),
    )
    op.create_table('friend_fbid_facts_hourly',
    sa.Column('campaign_id', sa.Integer(), nullable=False),
    sa.Column('hour', sa.DateTime(), nullable=False),
    sa.Column('friend_fbid', sa.BigInteger(), nullable=False),
    sa.Column('friends_shared_with', sa.Integer(), nullable=True),
    )
    op.create_table('fact_metadata',
    sa.Column('fact_id', sa.Integer(), nullable=False),
    sa.Column('slug', sa.String(), nullable=True),
    sa.Column('pretty_name', sa.String(), nullable=True),
    sa.Column('expression', sa.String(), nullable=True),
    sa.Column('dimension_id', sa.Integer(), nullable=True),
    )
    op.create_table('visit_facts_hourly',
    sa.Column('campaign_id', sa.Integer(), nullable=False),
    sa.Column('hour', sa.DateTime(), nullable=False),
    sa.Column('visit_id', sa.BigInteger(), nullable=False),
    sa.Column('visit_ids', sa.Integer(), nullable=True),
    sa.Column('visits_declined_auth', sa.Integer(), nullable=True),
    sa.Column('visits_shown_friend_sugg', sa.Integer(), nullable=True),
    sa.Column('authorized_visits', sa.Integer(), nullable=True),
    sa.Column('visits_with_shares', sa.Integer(), nullable=True),
    )
    op.create_table('fbid_facts_hourly',
    sa.Column('campaign_id', sa.Integer(), nullable=False),
    sa.Column('hour', sa.DateTime(), nullable=False),
    sa.Column('fbid', sa.BigInteger(), nullable=False),
    sa.Column('fbids_authorized', sa.Integer(), nullable=True),
    sa.Column('fbids_generated_friends', sa.Integer(), nullable=True),
    sa.Column('fbids_shown_friends', sa.Integer(), nullable=True),
    sa.Column('fbids_face_pages', sa.Integer(), nullable=True),
    sa.Column('fbids_shared', sa.Integer(), nullable=True),
    )
    op.create_table('misc_facts_hourly',
    sa.Column('campaign_id', sa.Integer(), nullable=False),
    sa.Column('hour', sa.DateTime(), nullable=False),
    sa.Column('visitors', sa.Integer(), nullable=True),
    sa.Column('no_friends', sa.Integer(), nullable=True),
    sa.Column('declined_auth', sa.Integer(), nullable=True),
    sa.Column('authorizations', sa.Integer(), nullable=True),
    sa.Column('shares_failed', sa.Integer(), nullable=True),
    sa.Column('clickbacks', sa.Integer(), nullable=True),
    )
    op.create_table('dimension_metadata',
    sa.Column('dimension_id', sa.Integer(), nullable=False),
    sa.Column('slug', sa.String(), nullable=True),
    sa.Column('pretty_name', sa.String(), nullable=True),
    sa.Column('column', sa.String(), nullable=True),
    sa.Column('source_table', sa.String(), nullable=True),
    )


def downgrade():
    op.drop_table('dimension_metadata')
    op.drop_table('misc_facts_hourly')
    op.drop_table('fbid_facts_hourly')
    op.drop_table('visit_facts_hourly')
    op.drop_table('fact_metadata')
    op.drop_table('friend_fbid_facts_hourly')
    op.drop_table('ip_facts_hourly')
