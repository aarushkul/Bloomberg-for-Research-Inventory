"""add_contact_enrichment_fields

Revision ID: 85c123f218e1
Revises: 7b3d6c2d8e6a
Create Date: 2025-12-23 22:58:02.189537

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '85c123f218e1'
down_revision: Union[str, Sequence[str], None] = '7b3d6c2d8e6a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add contact enrichment fields to trial_insights table
    op.add_column('trial_insights', sa.Column('contact_email', sa.String(length=255), nullable=True))
    op.add_column('trial_insights', sa.Column('contact_phone', sa.String(length=100), nullable=True))
    op.add_column('trial_insights', sa.Column('contact_institution', sa.String(length=500), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    # Remove contact enrichment fields from trial_insights table
    op.drop_column('trial_insights', 'contact_institution')
    op.drop_column('trial_insights', 'contact_phone')
    op.drop_column('trial_insights', 'contact_email')
