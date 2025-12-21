"""Print a table of processed clinical trial insights."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from sqlalchemy import Select, select

from app.db.models import Trial, TrialInsight
from app.db.session import get_session


def fetch_rows() -> Sequence:
    """Return trial insight rows for reporting."""
    stmt: Select = (
        select(
            TrialInsight.lab_name.label("lab_name"),
            TrialInsight.need_level.label("demand_signal"),
            TrialInsight.product_category.label("product"),
            TrialInsight.notes.label("demand_reason"),
            Trial.name.label("trial_name"),
            TrialInsight.created_at.label("detected_at"),
        )
        .select_from(TrialInsight)
        .outerjoin(Trial, Trial.id == TrialInsight.trial_id)
        .order_by(TrialInsight.created_at.desc())
    )
    with get_session() as session:
        return session.execute(stmt).all()


def print_table(rows: Sequence) -> None:
    columns = [
        "Principal investigator",
        "Email",
        "Product",
        "Demand signal",
        "Demand reason",
        "Trial",
        "Detected at",
    ]
    print(" | ".join(columns))
    print("-" * 160)
    for row in rows:
        print(
            f"{row.lab_name} |  | {row.product} | {row.demand_signal} | "
            f"{row.demand_reason or ''} | {row.trial_name or 'Unknown Trial'} | "
            f"{row.detected_at:%Y-%m-%d %H:%M:%S}"
        )


def write_html(rows: Sequence) -> None:
    """Persist a styled HTML table so non-technical folks can view it."""
    head = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Clinical Trial Insights</title>
  <style>
    body { font-family: Arial, sans-serif; padding: 2rem; background: #f5f6fa; }
    table { border-collapse: collapse; width: 100%; background: white; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
    th, td { padding: 0.6rem 0.9rem; text-align: left; border-bottom: 1px solid #e1e4eb; }
    th { background: #0f6db5; color: white; }
    tr:hover { background: #f0f8ff; }
  </style>
</head>
<body>
  <h1>Lab Demand Signals</h1>
  <table>
    <tr>
      <th>Principal investigator</th>
      <th>Email</th>
      <th>Product</th>
      <th>Demand signal</th>
      <th>Demand reason</th>
      <th>Trial</th>
      <th>Detected at</th>
    </tr>
"""
    body_rows = [
        "    <tr>"
        f"<td>{row.lab_name}</td>"
        "<td></td>"
        f"<td>{row.product}</td>"
        f"<td>{row.demand_signal}</td>"
        f"<td>{row.demand_reason or ''}</td>"
        f"<td>{row.trial_name or 'Unknown Trial'}</td>"
        f"<td>{row.detected_at:%Y-%m-%d %H:%M:%S}</td>"
        "</tr>"
        for row in rows
    ]
    tail = """  </table>
</body>
</html>
"""
    Path("insights_output.html").write_text(head + "\n".join(body_rows) + "\n" + tail, encoding="utf-8")
    print("\nWrote insights_output.html. Open it in your browser for the styled table.")


def main() -> None:
    rows = fetch_rows()
    if not rows:
        print("No trial insights found. Run the ingest/process scripts first.")
        return
    print_table(rows)
    write_html(rows)


if __name__ == "__main__":
    main()
