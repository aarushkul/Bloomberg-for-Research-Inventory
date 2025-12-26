"""add_sponsor_tracking_fields

Revision ID: f8279fe345a4
Revises: 85c123f218e1
Create Date: 2025-12-23 23:29:13.548601

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f8279fe345a4'
down_revision: Union[str, Sequence[str], None] = '85c123f218e1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add sponsor tracking fields to trial_insights table
    op.add_column('trial_insights', sa.Column('sponsor_name', sa.String(length=500), nullable=True))
    op.add_column('trial_insights', sa.Column('is_pfizer_trial', sa.Boolean(), nullable=False, server_default='false'))


def downgrade() -> None:
    """Downgrade schema."""
    # Remove sponsor tracking fields from trial_insights table
    op.drop_column('trial_insights', 'is_pfizer_trial')
    op.drop_column('trial_insights', 'sponsor_name')
