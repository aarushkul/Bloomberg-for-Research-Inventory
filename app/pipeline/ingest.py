"""CLI helper to load raw clinical docs into the database."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from sqlalchemy import select

from app.db.models import RawDocument
from app.db.session import get_session


def load_payloads(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text())
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    raise ValueError("Expected JSON object or list of objects")


def upsert_documents(source: str, id_field: str, payloads: Iterable[Dict[str, Any]]) -> int:
    inserted = 0
    with get_session() as session:
        for payload in payloads:
            external_id = _extract_id(payload, id_field)
            if not external_id:
                raise ValueError(f"Could not find field '{id_field}' in payload: {payload.keys()}")

            existing = session.execute(
                select(RawDocument).where(
                    RawDocument.source == source,
                    RawDocument.external_id == external_id,
                )
            ).scalar_one_or_none()

            if existing:
                existing.payload = payload
                existing.status = "new"
                existing.error_message = None
            else:
                session.add(
                    RawDocument(
                        source=source,
                        external_id=external_id,
                        payload=payload,
                        status="new",
                    )
                )
            inserted += 1
    return inserted


def _extract_id(payload: Dict[str, Any], id_field: str) -> str:
    value = payload.get(id_field)
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw clinical trial/grant JSON into the DB queue.")
    parser.add_argument("path", type=Path, help="Path to a JSON file (object or list of objects).")
    parser.add_argument("--source", default="clinicaltrials", help="Origin tag, e.g. clinicaltrials or nih.")
    parser.add_argument(
        "--id-field",
        default="nct_id",
        help="Field inside each JSON record that uniquely identifies it (default: nct_id).",
    )
    args = parser.parse_args()

    payloads = load_payloads(args.path)
    count = upsert_documents(args.source, args.id_field, payloads)
    print(f"Queued {count} document(s) from {args.source}.")


if __name__ == "__main__":
    main()
