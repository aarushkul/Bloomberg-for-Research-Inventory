"""Print a table of processed clinical trial insights."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from sqlalchemy import Select, select

from app.db.models import Trial, TrialInsight
from app.db.session import get_session


def fetch_rows() -> tuple[Sequence, Sequence]:
    """Return trial insight rows for reporting, split by Pfizer vs non-Pfizer."""
    stmt: Select = (
        select(
            TrialInsight.lab_name.label("lab_name"),
            TrialInsight.contact_email.label("email"),
            TrialInsight.contact_phone.label("phone"),
            TrialInsight.contact_institution.label("institution"),
            TrialInsight.need_level.label("demand_signal"),
            TrialInsight.product_category.label("product"),
            TrialInsight.notes.label("demand_reason"),
            TrialInsight.is_new.label("is_new"),
            TrialInsight.is_changed.label("is_changed"),
            TrialInsight.is_pfizer_trial.label("is_pfizer_trial"),
            TrialInsight.sponsor_name.label("sponsor_name"),
            Trial.name.label("trial_name"),
            Trial.nct_id.label("nct_id"),
            TrialInsight.created_at.label("detected_at"),
        )
        .select_from(TrialInsight)
        .outerjoin(Trial, Trial.id == TrialInsight.trial_id)
        .order_by(TrialInsight.created_at.desc())
    )
    with get_session() as session:
        all_rows = session.execute(stmt).all()
        # Split into Pfizer and non-Pfizer
        pfizer_rows = [row for row in all_rows if row.is_pfizer_trial]
        non_pfizer_rows = [row for row in all_rows if not row.is_pfizer_trial]
        return non_pfizer_rows, pfizer_rows


def print_table(rows: Sequence) -> None:
    columns = [
        "Principal investigator",
        "Email",
        "Phone",
        "Institution",
        "Product",
        "Demand signal",
        "Status",
        "Demand reason",
        "Trial (NCT ID)",
        "Detected at",
    ]
    print(" | ".join(columns))
    print("-" * 200)
    for row in rows:
        # Status priority: new > changed > existing
        if row.is_new:
            status = "NEW"
        elif row.is_changed:
            status = "CHANGED"
        else:
            status = "existing"
        
        # Format trial name with NCT ID
        trial_display = f"{(row.trial_name or 'Unknown')[:25]} ({row.nct_id or 'N/A'})" if row.nct_id else (row.trial_name or 'Unknown')[:30]
        
        print(
            f"{row.lab_name} | {row.email or 'N/A'} | {row.phone or 'N/A'} | "
            f"{(row.institution or 'N/A')[:40]} | {row.product} | {row.demand_signal} | {status} | "
            f"{(row.demand_reason or '')[:50]} | {trial_display} | "
            f"{row.detected_at:%Y-%m-%d %H:%M:%S}"
        )


def write_html(non_pfizer_rows: Sequence, pfizer_rows: Sequence) -> None:
    """Persist a styled HTML table so non-technical folks can view it."""
    head = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Clinical Trial Insights</title>
  <style>
    body { font-family: Arial, sans-serif; padding: 2rem; background: #f5f6fa; }
    h1 { color: #2c3e50; }
    h2 { color: #34495e; margin-top: 2rem; border-bottom: 2px solid #0f6db5; padding-bottom: 0.5rem; }
    table { border-collapse: collapse; width: 100%; background: white; box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-bottom: 2rem; }
    th, td { padding: 0.6rem 0.9rem; text-align: left; border-bottom: 1px solid #e1e4eb; }
    th { background: #0f6db5; color: white; }
    tr:hover { background: #f0f8ff; }
    a { color: #0066cc; text-decoration: none; }
    a:hover { text-decoration: underline; color: #004499; }
    .status-new { background: #d4edda; color: #155724; font-weight: bold; padding: 4px 8px; border-radius: 4px; }
    .status-changed { background: #fff3cd; color: #856404; font-weight: bold; padding: 4px 8px; border-radius: 4px; }
    .status-existing { color: #6c757d; padding: 4px 8px; }
    .pfizer-section { background: #fff9e6; padding: 1rem; border-radius: 8px; margin-top: 2rem; }
  </style>
</head>
<body>
  <h1>Lab Demand Signals</h1>
"""

    def format_rows(rows):
        body_rows = []
        for row in rows:
            # Determine status with styling
            if row.is_new:
                status_html = '<span class="status-new">NEW</span>'
            elif row.is_changed:
                status_html = '<span class="status-changed">CHANGED</span>'
            else:
                status_html = '<span class="status-existing">existing</span>'
            
            # Format email as clickable mailto link if available
            email_html = f'<a href="mailto:{row.email}">{row.email}</a>' if row.email else 'N/A'
            
            # Format trial as clickable link to ClinicalTrials.gov
            if row.nct_id:
                trial_html = (
                    f'<a href="https://clinicaltrials.gov/study/{row.nct_id}" target="_blank" '
                    f'title="{row.trial_name or row.nct_id}">'
                    f'{(row.trial_name or "Unknown")[:50]}...<br>'
                    f'<small style="color: #0066cc;">{row.nct_id}</small></a>'
                )
            else:
                trial_html = row.trial_name or 'Unknown Trial'
            
            body_rows.append(
                "    <tr>"
                f"<td>{row.lab_name}</td>"
                f"<td>{email_html}</td>"
                f"<td>{row.phone or 'N/A'}</td>"
                f"<td>{row.institution or 'N/A'}</td>"
                f"<td>{row.product}</td>"
                f"<td>{row.demand_signal}</td>"
                f"<td>{status_html}</td>"
                f"<td>{row.demand_reason or ''}</td>"
                f"<td>{trial_html}</td>"
                f"<td>{row.detected_at:%Y-%m-%d %H:%M:%S}</td>"
                "</tr>"
            )
        return body_rows
    
    # Regular trials section
    if non_pfizer_rows:
        head += """
  <h2>Academic & Small Biotech Trials</h2>
  <table>
    <tr>
      <th>Principal investigator</th>
      <th>Email</th>
      <th>Phone</th>
      <th>Institution</th>
      <th>Product</th>
      <th>Demand signal</th>
      <th>Status</th>
      <th>Demand reason</th>
      <th>Trial (NCT Link)</th>
      <th>Detected at</th>
    </tr>
"""
        head += "\n".join(format_rows(non_pfizer_rows))
        head += "\n  </table>\n"
    
    # Pfizer trials section
    if pfizer_rows:
        head += """
  <div class="pfizer-section">
    <h2>Pfizer Trials (Specialty Products Only)</h2>
    <p><strong>Note:</strong> These are Pfizer-sponsored trials. Only specialty outsourced products are shown.</p>
  </div>
  <table>
    <tr>
      <th>Principal investigator</th>
      <th>Email</th>
      <th>Phone</th>
      <th>Institution</th>
      <th>Product</th>
      <th>Demand signal</th>
      <th>Status</th>
      <th>Demand reason</th>
      <th>Trial (NCT Link)</th>
      <th>Detected at</th>
    </tr>
"""
        head += "\n".join(format_rows(pfizer_rows))
        head += "\n  </table>\n"
    
    tail = """</body>
</html>
"""
    Path("insights_output.html").write_text(head + tail, encoding="utf-8")
    print("\nWrote insights_output.html. Open it in your browser for the styled table.")


def main() -> None:
    non_pfizer_rows, pfizer_rows = fetch_rows()
    
    if not non_pfizer_rows and not pfizer_rows:
        print("No trial insights found. Run the ingest/process scripts first.")
        return
    
    # Print regular trials
    if non_pfizer_rows:
        print("\n" + "=" * 80)
        print("ACADEMIC & SMALL BIOTECH TRIALS")
        print("=" * 80)
        print_table(non_pfizer_rows)
    
    # Print Pfizer trials separately
    if pfizer_rows:
        print("\n" + "=" * 80)
        print("PFIZER TRIALS (Specialty Products Only)")
        print("=" * 80)
        print_table(pfizer_rows)
    
    write_html(non_pfizer_rows, pfizer_rows)


if __name__ == "__main__":
    main()
