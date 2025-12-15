"""Declarative base shared across all ORM models."""

from sqlalchemy.orm import declarative_base

Base = declarative_base()

__all__ = ["Base"]
