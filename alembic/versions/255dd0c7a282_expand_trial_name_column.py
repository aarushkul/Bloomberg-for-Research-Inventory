"""expand trial name column

Revision ID: 255dd0c7a282
Revises: 5f3e5e3f0c2b
Create Date: 2025-12-16 22:25:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '255dd0c7a282'
down_revision: Union[str, Sequence[str], None] = '5f3e5e3f0c2b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('trials', 'name', type_=sa.Text(), existing_type=sa.String(length=255), existing_nullable=False)


def downgrade() -> None:
    op.alter_column('trials', 'name', type_=sa.String(length=255), existing_type=sa.Text(), existing_nullable=False)
