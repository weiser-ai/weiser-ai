"""Add tenant_id column to metrics table

Revision ID: 8dfda426f97e
Revises: 001
Create Date: 2025-07-11 18:46:41.807389+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8dfda426f97e'
down_revision = '001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add tenant_id column with default value of 1
    op.add_column('metrics', sa.Column('tenant_id', sa.Integer(), nullable=True, default=1))
    # Update existing rows to have tenant_id = 1
    op.execute("UPDATE metrics SET tenant_id = 1 WHERE tenant_id IS NULL")
    # Make the column non-nullable
    op.alter_column('metrics', 'tenant_id', nullable=False)


def downgrade() -> None:
    # Remove tenant_id column
    op.drop_column('metrics', 'tenant_id')