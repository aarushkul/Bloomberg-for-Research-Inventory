"""Process queued raw documents into structured trials and insights."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from decimal import Decimal
from typing import Any, Dict, List, Tuple

from sqlalchemy import Select, select

from app.db.models import (
    DemandSignal,
    InventorySnapshot,
    RawDocument,
    Reagent,
    Supplier,
    Trial,
    TrialInsight,
)
from app.db.session import get_session
from openai import OpenAI

KEYWORD_MAP: Dict[str, Dict[str, str]] = {
    "flow cytometry": {
        "product_category": "Flow Cytometry Antibodies",
        "notes": "Requires fluorochrome-conjugated panels",
        "supplier_name": "CytoFlow Reagents",
        "reagent_name": "Fluorochrome Antibody Panel",
        "inventory_qty": 250,
        "expected_demand": 160,
        "signal_strength": "high",
    },
    "il-6": {
        "product_category": "ELISA Kits",
        "notes": "Cytokine quantitation",
        "supplier_name": "Cytokine Analytics Co.",
        "reagent_name": "Human IL-6 ELISA Kit",
        "inventory_qty": 180,
        "expected_demand": 120,
        "signal_strength": "high",
    },
    "hek293": {
        "product_category": "Cell Culture Media & Supplements",
        "notes": "Supports HEK293 expansion",
        "supplier_name": "CellCulture Depot",
        "reagent_name": "HEK293 Growth Bundle",
        "inventory_qty": 320,
        "expected_demand": 200,
        "signal_strength": "medium",
    },
    "crispr": {
        "product_category": "Genome Editing Reagents",
        "notes": "Guide RNA and nuclease kits",
        "supplier_name": "Precision CRISPR Labs",
        "reagent_name": "CRISPR Editing Kit",
        "inventory_qty": 150,
        "expected_demand": 110,
        "signal_strength": "medium",
    },
}

_OPENAI_CLIENT: OpenAI | None = None


def process_pending(batch_size: int) -> None:
    with get_session() as session:
        docs = session.execute(_select_pending_docs(batch_size)).scalars().all()

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

                default_lab = _extract_lab_name(doc.payload)
                insights = _translate_payload(doc.payload, default_lab=default_lab)
                for insight in insights:
                    trial_insight = TrialInsight(
                        trial=trial,
                        raw_document=doc,
                        lab_name=insight["lab_name"],
                        need_level=insight["need_level"],
                        product_category=insight["product_category"],
                        notes=insight.get("notes"),
                    )
                    session.add(trial_insight)

                    supplier = _get_or_create_supplier(session, insight["supplier_name"])
                    reagent = _get_or_create_reagent(session, supplier, insight["reagent_name"])
                    session.flush()

                    session.add(
                        InventorySnapshot(
                            reagent=reagent,
                            quantity_on_hand=insight.get("inventory_qty", 100),
                        )
                    )

                    expected, strength = _compute_demand_metrics(
                        insight.get("expected_demand"),
                        insight.get("signal_strength"),
                        insight["need_level"],
                    )

                    session.add(
                        DemandSignal(
                            trial=trial,
                            reagent=reagent,
                            expected_demand=expected,
                            signal_strength=strength,
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


def _select_pending_docs(batch_size: int) -> Select:
    return (
        select(RawDocument)
        .where(RawDocument.status == "new")
        .order_by(RawDocument.created_at)
        .limit(batch_size)
    )


def _translate_payload(payload: Dict, default_lab: str) -> List[Dict[str, str]]:
    llm_matches = _llm_translate_payload(payload, default_lab)
    if llm_matches:
        return llm_matches
    return _keyword_translate_payload(payload, default_lab)


def _keyword_translate_payload(payload: Dict, default_lab: str) -> List[Dict[str, str]]:
    text_blob = _extract_text_blob(payload).lower()

    matches = []
    for keyword, mapping in KEYWORD_MAP.items():
        if keyword in text_blob:
            matches.append(_build_match(payload, default_lab, mapping))

    if not matches:
        matches.append(
            {
                "lab_name": default_lab,
                "need_level": payload.get("need_level", "Medium"),
                "product_category": payload.get("default_category", "General Lab Supplies"),
                "notes": payload.get("default_notes", "No keyword match; generic demand created."),
                "supplier_name": payload.get("default_supplier", "General Lab Supply Co."),
                "reagent_name": payload.get("default_reagent", "General Lab Kit"),
                "inventory_qty": payload.get("default_inventory", 120),
                "expected_demand": payload.get("default_expected_demand", 90),
                "signal_strength": payload.get("default_signal_strength", "medium"),
            }
        )
    return matches


def _llm_translate_payload(payload: Dict, default_lab: str) -> List[Dict[str, str]]:
    """Use OpenAI to infer reagent needs from a trial payload."""
    client = _get_openai_client()
    if not client:
        return []

    trial_id = payload.get("nct_id") or payload.get("id") or "Unknown"
    combined_text = _extract_text_blob(payload)
    if not combined_text.strip():
        return []

    prompt = (
        "You are an analyst that maps clinical trial protocols to reagent demand.\n"
        f"Trial identifier: {trial_id}\n"
        f"Lab or sponsor: {default_lab}\n"
        "Given the protocol excerpt below, list the reagents, consumables, or cell lines required. "
        "For each item provide: lab_name, need_level (High/Medium/Low), product_category, supplier_name, "
        "reagent_name, inventory_qty (integer), expected_demand (integer), signal_strength "
        "(high/medium/low), and notes describing why it is needed.\n"
        "Return ONLY a JSON array of objects with those keys. Example:\n"
        '[{"lab_name":"ABC Lab","need_level":"High","product_category":"ELISA Kits",'
        '"supplier_name":"Cytokine Analytics","reagent_name":"IL-6 ELISA Kit","inventory_qty":150,'
        '"expected_demand":120,"signal_strength":"high","notes":"Measures IL-6 endpoints"}]\n\n'
        f"Protocol text:\n{combined_text[:6000]}"  # limit size
    )

    schema = {
        "type": "json_schema",
        "json_schema": {
            "name": "demand_items",
            "schema": {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "lab_name": {"type": "string"},
                                "need_level": {"type": "string"},
                                "product_category": {"type": "string"},
                                "supplier_name": {"type": "string"},
                                "reagent_name": {"type": "string"},
                                "inventory_qty": {"type": "integer"},
                                "expected_demand": {"type": "integer"},
                                "signal_strength": {"type": "string"},
                                "notes": {"type": "string"},
                            },
                            "required": [
                                "lab_name",
                                "need_level",
                                "product_category",
                                "supplier_name",
                                "reagent_name",
                            ],
                        },
                    }
                },
                "required": ["items"],
            },
        },
    }

    try:
        response = _call_openai(client, prompt, schema)
        raw_text = (response.output_text or "").strip()
        data = json.loads(raw_text) if raw_text else {}
        if isinstance(data, dict):
            data = data.get("items") or data.get("reagents")
        if not isinstance(data, list):
            return []
        normalized: List[Dict[str, str]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            normalized.append(
                {
                    "lab_name": item.get("lab_name") or default_lab,
                    "need_level": item.get("need_level", "High"),
                    "product_category": item.get("product_category", "General Lab Supplies"),
                    "notes": item.get("notes", ""),
                    "supplier_name": item.get("supplier_name", "Specialized Supplier"),
                    "reagent_name": item.get("reagent_name", "Custom Reagent"),
                    "inventory_qty": item.get("inventory_qty", 150),
                    "expected_demand": item.get("expected_demand", 120),
                    "signal_strength": item.get("signal_strength", "high"),
                }
            )
        return normalized
    except Exception as exc:  # pylint: disable=broad-except
        print(f"LLM translation failed for trial {trial_id}: {exc}")
        if "raw_text" in locals() and raw_text:
            print(f"Raw response: {raw_text[:2000]}")
        return []


def _call_openai(client: OpenAI, prompt: str, schema: Dict) -> Any:
    """Invoke the OpenAI Responses API, falling back if schema mode isn't supported."""
    kwargs = {
        "model": os.getenv("OPENAI_DEMAND_MODEL", "gpt-4.1-mini"),
        "input": prompt,
        "temperature": 0.1,
    }
    try:
        return client.responses.create(**kwargs, response_format=schema)
    except TypeError as exc:
        if "response_format" not in str(exc):
            raise
        print("OpenAI client does not support response_format; falling back to plain JSON prompt.")
        return client.responses.create(**kwargs)


def _build_match(payload: Dict, default_lab: str, mapping: Dict[str, str]) -> Dict[str, str]:
    return {
        "lab_name": default_lab,
        "need_level": payload.get("need_level", "High"),
        "product_category": mapping["product_category"],
        "notes": mapping["notes"],
        "supplier_name": mapping.get("supplier_name", "Specialized Supplier"),
        "reagent_name": mapping.get("reagent_name", mapping["product_category"]),
        "inventory_qty": mapping.get("inventory_qty", 150),
        "expected_demand": mapping.get("expected_demand", 120),
        "signal_strength": mapping.get("signal_strength", "high"),
    }


def _get_or_create_supplier(session, name: str) -> Supplier:
    existing = session.execute(select(Supplier).where(Supplier.name == name)).scalar_one_or_none()
    if existing:
        return existing
    supplier = Supplier(name=name)
    session.add(supplier)
    session.flush()
    return supplier


def _get_or_create_reagent(session, supplier: Supplier, name: str) -> Reagent:
    existing = session.execute(
        select(Reagent).where(Reagent.name == name, Reagent.supplier_id == supplier.id)
    ).scalar_one_or_none()
    if existing:
        return existing
    reagent = Reagent(name=name, supplier=supplier)
    session.add(reagent)
    session.flush()
    return reagent


def _compute_demand_metrics(
    expected: int | float | None, strength: str | None, need_level: str
) -> Tuple[Decimal, str]:
    level = (need_level or "Medium").lower()
    strength_value = (strength or level).lower()
    default_map = {
        "high": Decimal("150"),
        "medium": Decimal("90"),
        "low": Decimal("45"),
    }
    expected_value = (
        Decimal(str(expected)) if expected is not None else default_map.get(level, Decimal("90"))
    )
    return expected_value, strength_value


def _extract_text_blob(payload: Dict) -> str:
    """Pull relevant descriptive text from the nested ClinicalTrials structure."""
    segments: List[str] = []

    # legacy flat fields
    for key in ("brief_summary", "detailed_description", "description", "methods"):
        value = payload.get(key)
        if isinstance(value, str):
            segments.append(value)

    protocol = payload.get("protocolSection") or {}
    if isinstance(protocol, dict):
        description_module = protocol.get("descriptionModule") or {}
        if isinstance(description_module, dict):
            for key in ("briefSummary", "detailedDescription", "description"):
                value = description_module.get(key)
                if isinstance(value, str):
                    segments.append(value)
        arms_module = protocol.get("armsInterventionsModule") or {}
        if isinstance(arms_module, dict):
            for key in ("armGroups", "interventions"):
                maybe_list = arms_module.get(key, [])
                if isinstance(maybe_list, list):
                    for entry in maybe_list:
                        if isinstance(entry, dict):
                            for field in ("description", "interventionType", "name"):
                                val = entry.get(field)
                                if isinstance(val, str):
                                    segments.append(val)
        outcomes_module = protocol.get("outcomesModule") or {}
        if isinstance(outcomes_module, dict):
            for key in ("primaryOutcomes", "secondaryOutcomes"):
                maybe_list = outcomes_module.get(key, [])
                if isinstance(maybe_list, list):
                    for entry in maybe_list:
                        if isinstance(entry, dict):
                            for field in ("measure", "description"):
                                val = entry.get(field)
                                if isinstance(val, str):
                                    segments.append(val)

    return "\n".join(segment for segment in segments if segment).strip()


def _get_openai_client() -> OpenAI | None:
    global _OPENAI_CLIENT  # pylint: disable=global-statement
    if _OPENAI_CLIENT is not None:
        return _OPENAI_CLIENT
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    _OPENAI_CLIENT = OpenAI(api_key=api_key)
    return _OPENAI_CLIENT


def main() -> None:
    parser = argparse.ArgumentParser(description="Translate queued raw docs into structured insights.")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of docs to process per run.")
    args = parser.parse_args()
    process_pending(args.batch_size)


if __name__ == "__main__":
    main()
