"""add dedup and change tracking

Revision ID: 7b3d6c2d8e6a
Revises: 255dd0c7a282
Create Date: 2025-12-18 10:15:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7b3d6c2d8e6a"
down_revision: Union[str, Sequence[str], None] = "255dd0c7a282"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("trials", sa.Column("nct_id", sa.String(length=64), nullable=True))
    op.create_unique_constraint("uq_trials_nct_id", "trials", ["nct_id"])

    op.add_column("trial_insights", sa.Column("dedup_key", sa.String(length=64), nullable=True))
    op.add_column(
        "trial_insights",
        sa.Column("last_seen_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.add_column(
        "trial_insights",
        sa.Column("is_new", sa.Boolean(), server_default=sa.text("true"), nullable=False),
    )
    op.add_column(
        "trial_insights",
        sa.Column("is_changed", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.add_column("trial_insights", sa.Column("change_summary", sa.Text(), nullable=True))
    op.create_unique_constraint(
        "uq_trial_insights_trial_dedup",
        "trial_insights",
        ["trial_id", "dedup_key"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_trial_insights_trial_dedup", "trial_insights", type_="unique")
    op.drop_column("trial_insights", "change_summary")
    op.drop_column("trial_insights", "is_changed")
    op.drop_column("trial_insights", "is_new")
    op.drop_column("trial_insights", "last_seen_at")
    op.drop_column("trial_insights", "dedup_key")

    op.drop_constraint("uq_trials_nct_id", "trials", type_="unique")
    op.drop_column("trials", "nct_id")
