"""Print a table of processed clinical trial insights."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from sqlalchemy import Select, func, select

from app.db.models import DemandSignal, InventorySnapshot, Reagent, Supplier, Trial
from app.db.session import get_session


def fetch_rows() -> Sequence:
    """Return supplier/reagent demand rows built from processed signals."""
    latest_inventory = (
        select(
            InventorySnapshot.reagent_id.label("inv_reagent_id"),
            func.max(InventorySnapshot.recorded_at).label("latest_recorded_at"),
        )
        .group_by(InventorySnapshot.reagent_id)
        .subquery()
    )

    stmt: Select = (
        select(
            Supplier.name.label("supplier"),
            Reagent.name.label("reagent"),
            InventorySnapshot.quantity_on_hand.label("inventory"),
            DemandSignal.expected_demand.label("expected_demand"),
            DemandSignal.signal_strength.label("signal_strength"),
            Trial.name.label("trial_name"),
            DemandSignal.detected_at.label("detected_at"),
        )
        .select_from(DemandSignal)
        .join(Reagent, DemandSignal.reagent_id == Reagent.id)
        .join(Supplier, Reagent.supplier_id == Supplier.id)
        .join(Trial, DemandSignal.trial_id == Trial.id)
        .outerjoin(
            latest_inventory,
            latest_inventory.c.inv_reagent_id == Reagent.id,
        )
        .outerjoin(
            InventorySnapshot,
            (InventorySnapshot.reagent_id == Reagent.id)
            & (InventorySnapshot.recorded_at == latest_inventory.c.latest_recorded_at),
        )
        .order_by(DemandSignal.detected_at.desc())
    )
    with get_session() as session:
        return session.execute(stmt).all()


def print_table(rows: Sequence) -> None:
    columns = ["Supplier", "Reagent", "Inventory", "Demand", "Signal", "Trial", "Detected at"]
    print(" | ".join(columns))
    print("-" * 140)
    for row in rows:
        print(
            f"{row.supplier} | {row.reagent} | {row.inventory or 'N/A'} | "
            f"{row.expected_demand} | {row.signal_strength} | "
            f"{row.trial_name} | {row.detected_at:%Y-%m-%d %H:%M:%S}"
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
  <h1>Supplier Demand Signals</h1>
  <table>
    <tr>
      <th>Supplier</th>
      <th>Reagent</th>
      <th>Inventory</th>
      <th>Demand</th>
      <th>Signal strength</th>
      <th>Trial</th>
      <th>Detected at</th>
    </tr>
"""
    body_rows = [
        "    <tr>"
        f"<td>{row.supplier}</td>"
        f"<td>{row.reagent}</td>"
        f"<td>{row.inventory or 'N/A'}</td>"
        f"<td>{row.expected_demand}</td>"
        f"<td>{row.signal_strength}</td>"
        f"<td>{row.trial_name}</td>"
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
