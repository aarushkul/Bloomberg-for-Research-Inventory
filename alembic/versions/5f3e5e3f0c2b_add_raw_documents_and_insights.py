"""add raw documents and insights

Revision ID: 5f3e5e3f0c2b
Revises: 9cbc68b70ca2
Create Date: 2025-12-15 04:25:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5f3e5e3f0c2b'
down_revision: Union[str, Sequence[str], None] = '9cbc68b70ca2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'raw_documents',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('source', sa.String(length=50), nullable=False),
        sa.Column('external_id', sa.String(length=255), nullable=False),
        sa.Column('status', sa.String(length=50), nullable=False, server_default=sa.text("'new'")),
        sa.Column('payload', sa.JSON(), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('source', 'external_id', name='uq_raw_document_source_external_id')
    )
    op.create_table(
        'trial_insights',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('trial_id', sa.Integer(), nullable=True),
        sa.Column('raw_document_id', sa.Integer(), nullable=True),
        sa.Column('lab_name', sa.String(length=255), nullable=False),
        sa.Column('need_level', sa.String(length=50), nullable=False),
        sa.Column('product_category', sa.String(length=255), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['raw_document_id'], ['raw_documents.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['trial_id'], ['trials.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    op.drop_table('trial_insights')
    op.drop_table('raw_documents')
