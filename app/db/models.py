"""ORM models for the Bloomberg research backend."""

from sqlalchemy import (
    Boolean,
    JSON,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import relationship

from .base import Base


class TimestampedBase(Base):
    """Base class that gives each model an id and timestamps."""

    __abstract__ = True

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class Trial(TimestampedBase):
    """Clinical trial we analyze for downstream demand."""

    __tablename__ = "trials"

    nct_id = Column(String(64), nullable=True, unique=True)
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=True)

    demand_signals = relationship("DemandSignal", back_populates="trial", cascade="all, delete-orphan")
    insights = relationship("TrialInsight", back_populates="trial", cascade="all, delete-orphan")


class Supplier(TimestampedBase):
    """Company that provides reagents."""

    __tablename__ = "suppliers"

    name = Column(String(255), nullable=False, unique=True)

    reagents = relationship("Reagent", back_populates="supplier", cascade="all, delete-orphan")


class Reagent(TimestampedBase):
    """Reagent stocked by a supplier and used in trials."""

    __tablename__ = "reagents"

    name = Column(String(255), nullable=False)
    supplier_id = Column(Integer, ForeignKey("suppliers.id", ondelete="CASCADE"), nullable=False)

    supplier = relationship("Supplier", back_populates="reagents")
    inventory_snapshots = relationship(
        "InventorySnapshot",
        back_populates="reagent",
        cascade="all, delete-orphan",
    )
    demand_signals = relationship("DemandSignal", back_populates="reagent", cascade="all, delete-orphan")


class InventorySnapshot(TimestampedBase):
    """Inventory measurement for a reagent at a point in time."""

    __tablename__ = "inventory_snapshots"

    reagent_id = Column(Integer, ForeignKey("reagents.id", ondelete="CASCADE"), nullable=False)
    quantity_on_hand = Column(Integer, nullable=False)
    recorded_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    reagent = relationship("Reagent", back_populates="inventory_snapshots")


class DemandSignal(TimestampedBase):
    """Forecast of reagent demand produced from trial analytics."""

    __tablename__ = "demand_signals"

    trial_id = Column(Integer, ForeignKey("trials.id", ondelete="CASCADE"), nullable=False)
    reagent_id = Column(Integer, ForeignKey("reagents.id", ondelete="CASCADE"), nullable=False)
    expected_demand = Column(Numeric(scale=2), nullable=False)
    signal_strength = Column(String(50), nullable=False)
    detected_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    trial = relationship("Trial", back_populates="demand_signals")
    reagent = relationship("Reagent", back_populates="demand_signals")


class RawDocument(TimestampedBase):
    """Raw payloads pulled from external sources before translation."""

    __tablename__ = "raw_documents"
    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_raw_document_source_external_id"),
    )

    source = Column(String(50), nullable=False)
    external_id = Column(String(255), nullable=False)
    status = Column(String(50), nullable=False, default="new")
    payload = Column(JSON, nullable=False)
    error_message = Column(Text, nullable=True)
    processed_at = Column(DateTime(timezone=True), nullable=True)

    insights = relationship("TrialInsight", back_populates="raw_document", cascade="all, delete-orphan")


class TrialInsight(TimestampedBase):
    """Structured insight derived from a trial document."""

    __tablename__ = "trial_insights"
    __table_args__ = (
        UniqueConstraint("trial_id", "dedup_key", name="uq_trial_insights_trial_dedup"),
    )

    trial_id = Column(Integer, ForeignKey("trials.id", ondelete="CASCADE"), nullable=True)
    raw_document_id = Column(Integer, ForeignKey("raw_documents.id", ondelete="SET NULL"), nullable=True)
    lab_name = Column(String(255), nullable=False)
    need_level = Column(String(50), nullable=False)
    product_category = Column(String(255), nullable=False)
    notes = Column(Text, nullable=True)
    dedup_key = Column(String(64), nullable=True)
    last_seen_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    is_new = Column(Boolean, nullable=False, server_default="true")
    is_changed = Column(Boolean, nullable=False, server_default="false")
    change_summary = Column(Text, nullable=True)

    trial = relationship("Trial", back_populates="insights")
    raw_document = relationship("RawDocument", back_populates="insights")


__all__ = [
    "TimestampedBase",
    "Trial",
    "Supplier",
    "Reagent",
    "InventorySnapshot",
    "DemandSignal",
    "RawDocument",
    "TrialInsight",
]
