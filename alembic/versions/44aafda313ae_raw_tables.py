"""Raw tables

Revision ID: 44aafda313ae
Revises: e8e9af3683
Create Date: 2014-05-02 13:18:47.947086

"""

# revision identifiers, used by Alembic.
revision = '44aafda313ae'
down_revision = 'e8e9af3683'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('events',
    sa.Column('event_id', sa.Integer(), nullable=False, primary_key=True),
    sa.Column('visit_id', sa.Integer(), nullable=True),
    sa.Column('type', sa.String(), nullable=True),
    sa.Column('campaign_id', sa.Integer(), nullable=True),
    sa.Column('client_content', sa.Integer(), nullable=True),
    sa.Column('content', sa.String(), nullable=True),
    sa.Column('friend_fbid', sa.BigInteger(), nullable=True),
    sa.Column('activity_id', sa.BigInteger(), nullable=True),
    sa.Column('event_datetime', sa.DateTime(), nullable=True),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.Column('created', sa.DateTime(), nullable=True),
    )

    op.create_table('visits',
    sa.Column('visit_id', sa.Integer(), nullable=False, primary_key=True),
    sa.Column('visitor_id', sa.Integer(), nullable=True),
    sa.Column('session_id', sa.String(), nullable=True),
    sa.Column('appid', sa.BigInteger(), nullable=True),
    sa.Column('ip', sa.String(), nullable=True),
    sa.Column('user_agent', sa.String(), nullable=True),
    sa.Column('referer', sa.String(), nullable=True),
    sa.Column('source', sa.String(), nullable=True),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.Column('created', sa.DateTime(), nullable=True),
    )

    op.create_table('visitors',
    sa.Column('visitor_id', sa.Integer(), nullable=False, primary_key=True),
    sa.Column('uuid', sa.String(), nullable=True),
    sa.Column('fbid', sa.BigInteger(), nullable=True),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.Column('created', sa.DateTime(), nullable=True),
    )

def downgrade():
    op.drop_table('visitors')
    op.drop_table('visits')
    op.drop_table('events')
