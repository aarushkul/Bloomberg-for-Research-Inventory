"""Process queued raw documents into structured trials and insights."""

from __future__ import annotations

import argparse
import datetime as dt
from typing import Dict, List

from sqlalchemy import select

from app.db.models import RawDocument, Trial, TrialInsight
from app.db.session import get_session

KEYWORD_MAP: Dict[str, Dict[str, str]] = {
    "flow cytometry": {
        "product_category": "Flow Cytometry Antibodies",
        "notes": "Requires fluorochrome-conjugated panels",
    },
    "il-6": {
        "product_category": "ELISA Kits",
        "notes": "Cytokine quantitation",
    },
    "hek293": {
        "product_category": "Cell Culture Media & Supplements",
        "notes": "Supports HEK293 expansion",
    },
    "crispr": {
        "product_category": "Genome Editing Reagents",
        "notes": "Guide RNA and nuclease kits",
    },
}


def process_pending(batch_size: int) -> None:
    with get_session() as session:
        docs = (
            session.execute(
                select(RawDocument)
                .where(RawDocument.status == "new")
                .order_by(RawDocument.created_at)
                .limit(batch_size)
            )
            .scalars()
            .all()
        )

        if not docs:
            print("No pending documents. You're all caught up.")
            return

        for doc in docs:
            doc.status = "processing"
            session.flush()
            try:
                trial = Trial(
                    name=_extract_trial_name(doc.payload, doc.external_id),
                    description=_extract_description(doc.payload),
                )
                session.add(trial)
                session.flush()

                insights = _translate_payload(doc.payload, default_lab=_extract_lab_name(doc.payload))
                for insight in insights:
                    session.add(
                        TrialInsight(
                            trial=trial,
                            raw_document=doc,
                            lab_name=insight["lab_name"],
                            need_level=insight["need_level"],
                            product_category=insight["product_category"],
                            notes=insight.get("notes"),
                        )
                    )

                doc.status = "done"
                doc.error_message = None
                doc.processed_at = dt.datetime.now(dt.timezone.utc)
            except Exception as exc:  # pylint: disable=broad-except
                doc.status = "error"
                doc.error_message = str(exc)
                print(f"Failed to process {doc.source}:{doc.external_id} -> {exc}")
        print(f"Processed {len(docs)} document(s).")


def _extract_trial_name(payload: Dict, fallback_id: str) -> str:
    for key in (
        "official_title",
        "brief_title",
        "study_title",
        "title",
    ):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return f"Trial {fallback_id}" if fallback_id else "Untitled Trial"


def _extract_description(payload: Dict) -> str:
    for key in ("detailed_description", "description", "summary"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_lab_name(payload: Dict) -> str:
    for key in ("lab", "lead_sponsor", "sponsor", "agency" ):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            name = value.get("name") or value.get("agency")
            if isinstance(name, str) and name.strip():
                return name.strip()
    return "Unknown Lab"


def _translate_payload(payload: Dict, default_lab: str) -> List[Dict[str, str]]:
    text_segments = [
        payload.get("detailed_description", ""),
        payload.get("description", ""),
        payload.get("brief_summary", ""),
        payload.get("methods", ""),
    ]
    text_blob = " ".join(segment.lower() for segment in text_segments if isinstance(segment, str))

    matches = []
    for keyword, mapping in KEYWORD_MAP.items():
        if keyword in text_blob:
            matches.append(
                {
                    "lab_name": default_lab,
                    "need_level": payload.get("need_level", "High"),
                    "product_category": mapping["product_category"],
                    "notes": mapping["notes"],
                }
            )

    if not matches:
        matches.append(
            {
                "lab_name": default_lab,
                "need_level": payload.get("need_level", "Medium"),
                "product_category": payload.get("default_category", "General Lab Supplies"),
                "notes": "No keyword match; generic demand created.",
            }
        )
    return matches


def main() -> None:
    parser = argparse.ArgumentParser(description="Translate queued raw docs into structured insights.")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of docs to process per run.")
    args = parser.parse_args()
    process_pending(args.batch_size)


if __name__ == "__main__":
    main()
