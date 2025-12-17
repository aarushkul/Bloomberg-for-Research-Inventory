"""Load saved ClinicalTrials.gov JSON exports, filter, and queue them."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List

from app.pipeline.ingest import upsert_documents
from app.pipeline import process_raw

TARGET_PHASES = {"phase2", "phase3"}


def load_file(path: Path) -> Dict:
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or "studies" not in data:
        raise ValueError("Expected JSON object with top-level 'studies' array.")
    return data


def normalize_study(study: Dict) -> Dict:
    protocol = study.get("protocolSection", {}) or {}
    identification = protocol.get("identificationModule", {}) or {}
    description = protocol.get("descriptionModule", {}) or {}
    sponsor_module = protocol.get("sponsorCollaboratorsModule", {}) or {}
    status_module = protocol.get("statusModule", {}) or {}
    contacts_module = protocol.get("contactsLocationsModule", {}) or {}
    conditions_module = protocol.get("conditionsModule", {}) or {}
    arms_module = protocol.get("armsInterventionsModule", {}) or {}

    locations = []
    for loc in contacts_module.get("locationList") or []:
        facility = loc.get("facility")
        city = loc.get("city")
        state = loc.get("state")
        country = loc.get("country")
        parts = [part for part in (facility, city, state, country) if part]
        if parts:
            locations.append(", ".join(parts))

    intervention_types: List[str] = []
    intervention_names: List[str] = []
    for intervention in arms_module.get("interventions") or []:
        if intervention.get("type"):
            intervention_types.append(intervention["type"])
        if intervention.get("name"):
            intervention_names.append(intervention["name"])

    return {
        "nct_id": identification.get("nctId", ""),
        "brief_title": identification.get("briefTitle", ""),
        "official_title": identification.get("officialTitle", ""),
        "brief_summary": description.get("briefSummary", ""),
        "detailed_description": description.get("detailedDescription", ""),
        "lead_sponsor": (sponsor_module.get("leadSponsor") or {}).get("name", ""),
        "overall_status": status_module.get("overallStatus", ""),
        "last_update_post_date": (status_module.get("lastUpdatePostDateStruct") or {}).get("date", ""),
        "start_date": (status_module.get("startDateStruct") or {}).get("date", ""),
        "primary_completion_date": (status_module.get("primaryCompletionDateStruct") or {}).get("date", ""),
        "locations": locations,
        "location_states": [],
        "location_countries": contacts_module.get("locationCountries") or [],
        "intervention_types": intervention_types,
        "intervention_names": intervention_names,
        "conditions": conditions_module.get("conditions") or [],
        "phases": (protocol.get("designModule") or {}).get("phases") or [],
    }


def is_phase_eligible(phases: Iterable[str], titles: Iterable[str]) -> bool:
    normalized = {phase.lower() for phase in phases or []}
    if normalized & TARGET_PHASES:
        return True
    for title in titles:
        if _phases_from_title(title) & TARGET_PHASES:
            return True
    return False


def _phases_from_title(title: str | None) -> set[str]:
    if not isinstance(title, str):
        return set()
    text = title.lower()
    tags: set[str] = set()
    if "phase iii" in text or "phase 3" in text or "phase iii/" in text or "phase 3/" in text:
        tags.add("phase3")
    if "phase ii" in text or "phase 2" in text or "phase ii/" in text or "phase 2/" in text:
        tags.add("phase2")
    if "phase ii/iii" in text or "phase 2/3" in text:
        tags.update({"phase2", "phase3"})
    return tags


def _title_candidates(study: Dict) -> List[str]:
    protocol = study.get("protocolSection", {}) or {}
    identification = protocol.get("identificationModule", {}) or {}
    return [
        identification.get("briefTitle", ""),
        identification.get("officialTitle", ""),
        identification.get("acronym", ""),
    ]


def run_loader(path: Path, process: bool) -> None:
    payload = load_file(path)
    normalized_records = []
    for study in payload.get("studies", []):
        record = normalize_study(study)
        if record["nct_id"] and is_phase_eligible(record.get("phases") or [], _title_candidates(study)):
            normalized_records.append(record)

    if not normalized_records:
        print("No phase 2/3 studies found in the provided file.")
        return

    count = upsert_documents("clinicaltrials_manual", "nct_id", normalized_records)
    print(f"Queued {count} study/studies from {path}.")
    if process:
        process_raw.process_pending(batch_size=count or 10)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import saved ClinicalTrials JSON file.")
    parser.add_argument("path", type=Path, help="Path to the saved JSON file.")
    parser.add_argument(
        "--skip-process",
        action="store_true",
        help="Only ingest studies; do not run the translator afterwards.",
    )
    args = parser.parse_args()
    run_loader(args.path, process=not args.skip_process)


if __name__ == "__main__":
    main()
