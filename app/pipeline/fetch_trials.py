"""Fetch recent ClinicalTrials.gov studies (v1 Study Fields API)."""

from __future__ import annotations

import argparse
import datetime as dt
from typing import Dict, Iterable, List, Tuple

import httpx

from app.pipeline import process_raw
from app.pipeline.ingest import upsert_documents

API_URL = "https://classic.clinicaltrials.gov/api/query/study_fields"

DEFAULT_FIELDS = [
    "NCTId",
    "BriefTitle",
    "OfficialTitle",
    "BriefSummary",
    "DetailedDescription",
    "LeadSponsorName",
    "OverallStatus",
    "LastUpdatePostDate",
    "StartDate",
    "PrimaryCompletionDate",
    "LocationCountry",
    "LocationCity",
    "LocationState",
    "Condition",
    "InterventionType",
    "InterventionName",
]


def fetch_studies(
    expr: str,
    fields: Iterable[str],
    min_rank: int,
    max_rank: int,
    *,
    timeout: float = 30.0,
) -> Tuple[List[Dict], int]:
    """Call the Study Fields API and return records plus total count."""
    params = {
        "expr": expr,
        "fields": ",".join(fields),
        "min_rnk": str(min_rank),
        "max_rnk": str(max_rank),
        "fmt": "json",
    }
    headers = {
        "User-Agent": "BloombergResearchFetcher/1.0",
        "Accept": "application/json",
    }
    response = httpx.get(
        API_URL,
        params=params,
        timeout=timeout,
        follow_redirects=True,
        headers=headers,
    )
    response.raise_for_status()
    payload = response.json()["StudyFieldsResponse"]
    studies = payload["StudyFields"]
    total = int(payload["NStudiesFound"])
    return studies, total


def normalize_study(study: Dict) -> Dict:
    def first(field: str) -> str:
        value = study.get(field)
        if isinstance(value, list):
            if not value:
                return ""
            return " ".join(item for item in value if item).strip()
        if isinstance(value, str):
            return value.strip()
        return ""

    return {
        "nct_id": first("NCTId"),
        "brief_title": first("BriefTitle"),
        "official_title": first("OfficialTitle"),
        "brief_summary": first("BriefSummary"),
        "detailed_description": first("DetailedDescription"),
        "lead_sponsor": first("LeadSponsorName"),
        "overall_status": first("OverallStatus"),
        "last_update_post_date": first("LastUpdatePostDate"),
        "start_date": first("StartDate"),
        "primary_completion_date": first("PrimaryCompletionDate"),
        "locations": study.get("LocationCity") or [],
        "location_states": study.get("LocationState") or [],
        "location_countries": study.get("LocationCountry") or [],
        "intervention_types": study.get("InterventionType") or [],
        "intervention_names": study.get("InterventionName") or [],
        "conditions": study.get("Condition") or [],
    }


def build_recent_expr(days_back: int) -> str:
    end = dt.date.today()
    start = end - dt.timedelta(days=days_back)
    return f"AREA[LastUpdatePostDate]RANGE[{start:%Y-%m-%d},{end:%Y-%m-%d}]"


def run_fetcher(
    expr: str,
    since_days: int,
    max_records: int,
    page_size: int,
    fields: Iterable[str],
    process: bool,
) -> None:
    if not expr:
        expr = build_recent_expr(since_days)

    fetched: List[Dict] = []
    min_rank = 1
    total = None
    while len(fetched) < max_records:
        max_rank = min(min_rank + page_size - 1, max_records)
        studies, total = fetch_studies(expr, fields, min_rank, max_rank)
        if not studies:
            break
        fetched.extend(studies)
        min_rank = max_rank + 1
        if total is not None and min_rank > total:
            break

    if not fetched:
        print("No new studies returned from ClinicalTrials.gov.")
        return

    normalized: List[Dict] = []
    for study in fetched:
        record = normalize_study(study)
        if record["nct_id"]:
            normalized.append(record)
    count = upsert_documents("clinicaltrials", "nct_id", normalized)
    print(f"Queued {count} document(s) from ClinicalTrials.gov.")

    if process:
        process_raw.process_pending(batch_size=count or 10)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch recent trials from ClinicalTrials.gov.")
    parser.add_argument(
        "--since-days",
        type=int,
        default=7,
        help="Look back this many days when constructing the search expression (default: 7).",
    )
    parser.add_argument(
        "--expr",
        default="",
        help="Custom ClinicalTrials.gov expression. Overrides --since-days when provided.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=200,
        help="Maximum number of studies to fetch per run (default: 200).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Number of records per API page (default: 100).",
    )
    parser.add_argument(
        "--fields",
        nargs="*",
        default=DEFAULT_FIELDS,
        help="Study Fields to request (default: curated set).",
    )
    parser.add_argument(
        "--skip-process",
        action="store_true",
        help="Only ingest raw documents; do not run the translator afterwards.",
    )
    args = parser.parse_args()

    run_fetcher(
        expr=args.expr,
        since_days=args.since_days,
        max_records=args.max_records,
        page_size=args.page_size,
        fields=args.fields,
        process=not args.skip_process,
    )


if __name__ == "__main__":
    main()
