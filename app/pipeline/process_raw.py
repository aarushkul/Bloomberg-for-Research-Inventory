"""Process queued raw documents into structured trials and insights."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import httpx
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

_EXTERNAL_CACHE: Dict[str, Dict] = {}
NCT_API_BASE = os.getenv("NCT_API_BASE", "https://clinicaltrials.gov/api/v2/studies")

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
                nct_id = _extract_nct_id(doc.payload, doc.external_id)
                trial_title = _extract_trial_name(doc.payload, nct_id)
                trial = _get_or_create_trial(
                    session,
                    nct_id,
                    trial_title,
                    _extract_description(doc.payload),
                )

                external_payload = _fetch_external_payload(nct_id)
                principal_investigator = _extract_principal_investigator(doc.payload)
                if principal_investigator == "N/A" and external_payload:
                    principal_investigator = _extract_principal_investigator(external_payload)
                if principal_investigator == "N/A":
                    pi_name, _ = _llm_extract_principal_investigator(
                        doc.payload,
                        doc.external_id,
                        trial_title,
                        external_payload=external_payload,
                        allow_guess=False,
                    )
                    if pi_name:
                        principal_investigator = pi_name
                    else:
                        pi_name, _ = _llm_extract_principal_investigator(
                            doc.payload,
                            doc.external_id,
                            trial_title,
                            external_payload=external_payload,
                            allow_guess=True,
                        )
                        if pi_name:
                            principal_investigator = "N/A"
                insights = _translate_payload(
                    doc.payload,
                    default_lab=principal_investigator,
                    external_payload=external_payload,
                )
                now = dt.datetime.now(dt.timezone.utc)
                for insight in insights:
                    notes = (insight.get("notes") or "").strip()
                    dedup_key = _build_dedup_key(nct_id, insight["product_category"], notes)
                    existing_insight = session.execute(
                        select(TrialInsight).where(
                            TrialInsight.trial_id == trial.id,
                            TrialInsight.dedup_key == dedup_key,
                        )
                    ).scalar_one_or_none()
                    if existing_insight:
                        existing_insight.last_seen_at = now
                        if existing_insight.is_new:
                            existing_insight.is_new = False
                        if existing_insight.need_level != insight["need_level"]:
                            existing_insight.is_changed = True
                            existing_insight.change_summary = (
                                f"Demand signal changed from {existing_insight.need_level} "
                                f"to {insight['need_level']}."
                            )
                            existing_insight.need_level = insight["need_level"]
                        if notes and notes != (existing_insight.notes or ""):
                            existing_insight.is_changed = True
                            existing_insight.change_summary = "Demand reason updated."
                            existing_insight.notes = notes
                        continue

                    trial_insight = TrialInsight(
                        trial=trial,
                        raw_document=doc,
                        lab_name=principal_investigator,
                        need_level=insight["need_level"],
                        product_category=insight["product_category"],
                        notes=notes,
                        dedup_key=dedup_key,
                        last_seen_at=now,
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


def _extract_nct_id(payload: Dict, fallback_id: str) -> str:
    for key in ("nct_id", "nctId", "nctid", "id", "study_id"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return fallback_id or ""


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


def _extract_principal_investigator(payload: Dict) -> str:
    """Extract principal investigator name without guessing."""
    protocol = payload.get("protocolSection") or {}
    if isinstance(protocol, dict):
        contacts = protocol.get("contactsLocationsModule") or {}
        if isinstance(contacts, dict):
            officials = contacts.get("overallOfficials") or []
            if isinstance(officials, list):
                for official in officials:
                    if not isinstance(official, dict):
                        continue
                    role = (official.get("role") or "").lower()
                    name = (official.get("name") or "").strip()
                    if name and "principal" in role:
                        return name
            central_contacts = contacts.get("centralContacts") or []
            if isinstance(central_contacts, list):
                for contact in central_contacts:
                    if not isinstance(contact, dict):
                        continue
                    role = (contact.get("role") or "").lower()
                    name = (contact.get("name") or "").strip()
                    if name and "principal" in role:
                        return name
            central_contacts = contacts.get("centralContacts") or []
            if isinstance(central_contacts, list):
                for contact in central_contacts:
                    if not isinstance(contact, dict):
                        continue
                    role = (contact.get("role") or "").lower()
                    name = (contact.get("name") or "").strip()
                    if name and "principal" in role:
                        return name
        sponsor_module = protocol.get("sponsorCollaboratorsModule") or {}
        if isinstance(sponsor_module, dict):
            lead_sponsor = sponsor_module.get("leadSponsor") or {}
            if isinstance(lead_sponsor, dict):
                sponsor_name = (lead_sponsor.get("name") or "").strip()
                if sponsor_name:
                    return sponsor_name
    return "N/A"


def _select_pending_docs(batch_size: int) -> Select:
    return (
        select(RawDocument)
        .where(RawDocument.status == "new")
        .order_by(RawDocument.created_at)
        .limit(batch_size)
    )


def _translate_payload(
    payload: Dict, default_lab: str, external_payload: Dict | None
) -> List[Dict[str, str]]:
    llm_matches = _llm_translate_payload(payload, default_lab, external_payload)
    if llm_matches:
        return llm_matches
    keyword_matches = _keyword_translate_payload(payload, default_lab)
    return _filter_broad_categories(keyword_matches)


def _keyword_translate_payload(payload: Dict, default_lab: str) -> List[Dict[str, str]]:
    text_blob = _extract_text_blob(payload).lower()

    matches = []
    for keyword, mapping in KEYWORD_MAP.items():
        if keyword in text_blob:
            matches.append(_build_match(payload, default_lab, mapping))

    return matches


def _llm_translate_payload(
    payload: Dict, default_lab: str, external_payload: Dict | None
) -> List[Dict[str, str]]:
    """Use OpenAI to infer reagent needs from a trial payload."""
    client = _get_openai_client()
    if not client:
        return []

    trial_id = payload.get("nct_id") or payload.get("id") or "Unknown"
    trial_title = _extract_trial_name(payload, trial_id)
    combined_text = _combine_payload_text(payload, external_payload)
    if not combined_text.strip():
        return []
    prompt_lab = default_lab if default_lab != "N/A" else "Unknown"

    prompt = (
        "You are an analyst that maps clinical trial protocols to reagent demand.\n"
        f"Trial identifier: {trial_id}\n"
        f"Trial title: {trial_title}\n"
        f"Lab or sponsor: {prompt_lab}\n"
        "Given the protocol excerpt below, list the reagents, consumables, or cell lines required. "
        "For each item provide: lab_name, need_level (High/Medium/Low), product_category, supplier_name, "
        "reagent_name, inventory_qty (integer), expected_demand (integer), signal_strength "
        "(high/medium/low), and notes describing why it is needed.\n"
        "product_category must be a specific reagent class. Do NOT use broad terms like "
        "\"general lab supplies\", \"consumables\", \"drugs\", or \"equipment\".\n"
        "Return ONLY a JSON array of objects with those keys. Example:\n"
        '[{"lab_name":"ABC Lab","need_level":"High","product_category":"ELISA Kits",'
        '"supplier_name":"Cytokine Analytics","reagent_name":"IL-6 ELISA Kit","inventory_qty":150,'
        '"expected_demand":120,"signal_strength":"high","notes":"Measures IL-6 endpoints"}]\n\n'
        f"Protocol text:\n{combined_text[:10000]}"  # limit size
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
                    "lab_name": default_lab,
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
        refined = _filter_broad_categories(normalized)
        if not refined or len(refined) < len(normalized):
            refined = _llm_refine_specificity(
                payload,
                default_lab,
                combined_text,
                trial_title,
                external_payload=external_payload,
            ) or refined
        return _filter_broad_categories(refined)
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


def _get_or_create_trial(session, nct_id: str, title: str, description: str) -> Trial:
    existing = None
    if nct_id:
        existing = session.execute(select(Trial).where(Trial.nct_id == nct_id)).scalar_one_or_none()
    if existing:
        if title and existing.name != title:
            existing.name = title
        if description and existing.description != description:
            existing.description = description
        return existing
    trial = Trial(
        nct_id=nct_id or None,
        name=title,
        description=description,
    )
    session.add(trial)
    session.flush()
    return trial


def _build_dedup_key(nct_id: str, product_category: str, demand_reason: str) -> str:
    normalized = "|".join(
        part.strip().lower()
        for part in (nct_id or "", product_category or "", demand_reason or "")
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


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


def _combine_payload_text(payload: Dict, external_payload: Dict | None) -> str:
    base_text = _extract_text_blob(payload)
    if not external_payload:
        return base_text
    extra_text = _extract_text_blob(external_payload)
    if extra_text and extra_text not in base_text:
        return f"{base_text}\n\nAdditional context:\n{extra_text}"
    return base_text


def _fetch_external_payload(nct_id: str) -> Dict | None:
    if not nct_id:
        return None
    if nct_id in _EXTERNAL_CACHE:
        return _EXTERNAL_CACHE[nct_id]
    url = f"{NCT_API_BASE}/{nct_id}"
    try:
        response = httpx.get(url, timeout=30.0, follow_redirects=True)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict) and "studies" in data and data["studies"]:
            data = data["studies"][0]
        if isinstance(data, dict):
            _EXTERNAL_CACHE[nct_id] = data
            return data
    except Exception as exc:  # pylint: disable=broad-except
        print(f"External lookup failed for {nct_id}: {exc}")
    return None


def _filter_broad_categories(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    banned = {
        "general lab supplies",
        "consumables",
        "drugs",
        "equipment",
        "supplies",
        "laboratory equipment",
        "laboratory supplies",
    }
    filtered = []
    for item in items:
        category = (item.get("product_category") or "").strip().lower()
        if category in banned or category.startswith("general lab"):
            continue
        filtered.append(item)
    return filtered


def _llm_refine_specificity(
    payload: Dict,
    default_lab: str,
    combined_text: str,
    trial_title: str,
    *,
    external_payload: Dict | None,
) -> List[Dict[str, str]] | None:
    """Ask the model for more specific categories when output is too broad."""
    client = _get_openai_client()
    if not client or not combined_text.strip():
        return None

    trial_id = payload.get("nct_id") or payload.get("id") or "Unknown"
    prompt = (
        "You are refining reagent demand outputs. The previous output was too broad.\n"
        f"Trial identifier: {trial_id}\n"
        f"Trial title: {trial_title}\n"
        f"Lab or sponsor: {default_lab if default_lab != 'N/A' else 'Unknown'}\n"
        "Return ONLY specific reagent classes (e.g., flow cytometry antibodies, qPCR master mix, ELISA kits, "
        "cell culture media, sequencing library prep kits). Do NOT use broad terms like "
        "\"general lab supplies\", \"consumables\", \"drugs\", or \"equipment\".\n"
        "If you cannot be specific, return an empty JSON array [].\n\n"
        f"Protocol text:\n{combined_text[:6000]}"
    )
    schema = {
        "type": "json_schema",
        "json_schema": {
            "name": "demand_items",
            "schema": {
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
            },
        },
    }
    try:
        response = _call_openai(client, prompt, schema)
        raw_text = (response.output_text or "").strip()
        data = json.loads(raw_text) if raw_text else []
        if not isinstance(data, list):
            return None
        refined: List[Dict[str, str]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            refined.append(
                {
                    "lab_name": default_lab,
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
        return refined
    except Exception as exc:  # pylint: disable=broad-except
        print(f"LLM refinement failed for trial {trial_id}: {exc}")
        return None


def _llm_extract_principal_investigator(
    payload: Dict,
    trial_id: str,
    trial_title: str,
    *,
    external_payload: Dict | None,
    allow_guess: bool,
) -> Tuple[str, bool]:
    """Attempt to extract PI from payload; optionally allow guess with explicit flag."""
    client = _get_openai_client()
    if not client:
        return "", False

    protocol = payload.get("protocolSection", {}) or {}
    contacts = protocol.get("contactsLocationsModule", {}) or {}
    context = {
        "overallOfficials": contacts.get("overallOfficials") or [],
        "centralContacts": contacts.get("centralContacts") or [],
        "leadSponsor": (protocol.get("sponsorCollaboratorsModule") or {}).get("leadSponsor") or {},
        "nctId": trial_id,
        "title": trial_title,
    }
    if external_payload:
        ext_protocol = external_payload.get("protocolSection", {}) or {}
        ext_contacts = ext_protocol.get("contactsLocationsModule", {}) or {}
        context["externalOverallOfficials"] = ext_contacts.get("overallOfficials") or []
        context["externalCentralContacts"] = ext_contacts.get("centralContacts") or []
        context["externalLeadSponsor"] = (
            ext_protocol.get("sponsorCollaboratorsModule", {}) or {}
        ).get("leadSponsor") or {}

    if allow_guess:
        instruction = (
            "Try harder to identify a principal investigator using the trial identifier/title "
            "and the provided contact data. If you must guess, set guessed to true."
        )
    else:
        instruction = "Only return a PI if explicitly present in the provided payload. Do not guess."

    prompt = (
        f"{instruction}\n"
        "Return JSON: {\"pi_name\": \"\", \"guessed\": false}.\n"
        f"Payload excerpt: {json.dumps(context)[:6000]}"
    )
    schema = {
        "type": "json_schema",
        "json_schema": {
            "name": "pi_name",
            "schema": {
                "type": "object",
                "properties": {
                    "pi_name": {"type": "string"},
                    "guessed": {"type": "boolean"},
                },
                "required": ["pi_name", "guessed"],
            },
        },
    }
    try:
        response = _call_openai(client, prompt, schema)
        raw_text = (response.output_text or "").strip()
        data = json.loads(raw_text) if raw_text else {}
        name = (data.get("pi_name") or "").strip()
        guessed = bool(data.get("guessed"))
        return name, guessed
    except Exception as exc:  # pylint: disable=broad-except
        print(f"LLM PI extraction failed for trial {trial_id}: {exc}")
        return "", False


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
