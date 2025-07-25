"""Initial metrics table

Revision ID: 001
Revises: 
Create Date: 2024-07-10 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('metrics',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('actual_value', sa.Float(), nullable=True),
    sa.Column('check_id', sa.String(), nullable=True),
    sa.Column('condition', sa.String(), nullable=True),
    sa.Column('dataset', sa.String(), nullable=True),
    sa.Column('datasource', sa.String(), nullable=True),
    sa.Column('fail', sa.Boolean(), nullable=True),
    sa.Column('name', sa.String(), nullable=True),
    sa.Column('run_id', sa.String(), nullable=True),
    sa.Column('run_time', sa.DateTime(), nullable=True),
    sa.Column('sql', sa.String(), nullable=True),
    sa.Column('success', sa.Boolean(), nullable=True),
    sa.Column('threshold', sa.String(), nullable=True),
    sa.Column('threshold_list', sa.ARRAY(sa.Float()), nullable=True),
    sa.Column('type', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('metrics')
    # ### end Alembic commands ###