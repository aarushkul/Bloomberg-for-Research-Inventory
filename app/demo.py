"""Simple script that seeds data and prints a reagent demand table."""

from __future__ import annotations

from pathlib import Path
from typing import List

from sqlalchemy import select, text

from app.db.models import DemandSignal, InventorySnapshot, Reagent, Supplier, Trial
from app.db.session import get_session


def seed_demo_data() -> None:
    demo_rows = [
        {
            "supplier": "Acme Biotech",
            "reagent": "PCR Master Mix",
            "trial": "Phase II Oncology Study",
            "description": "Focused on targeted therapies",
            "inventory": 120,
            "expected_demand": 45.5,
            "signal_strength": "high",
        },
        {
            "supplier": "POOPIE",
            "reagent": "Agarose",
            "trial": "Phase II Poop Study",
            "description": "Focused on poop",
            "inventory": 500,
            "expected_demand": 75,
            "signal_strength": "medium",
        },
    ]

    with get_session() as session:
        # Reset tables so rerunning the script doesn't hit unique constraints.
        session.execute(text("TRUNCATE demand_signals, inventory_snapshots, reagents, suppliers, trials RESTART IDENTITY CASCADE"))

        records = []
        for row in demo_rows:
            supplier = Supplier(name=row["supplier"])
            reagent = Reagent(name=row["reagent"], supplier=supplier)
            trial = Trial(name=row["trial"], description=row["description"])

            inventory = InventorySnapshot(reagent=reagent, quantity_on_hand=row["inventory"])
            demand_signal = DemandSignal(
                trial=trial,
                reagent=reagent,
                expected_demand=row["expected_demand"],
                signal_strength=row["signal_strength"],
            )

            records.extend([supplier, reagent, trial, inventory, demand_signal])

        session.add_all(records)


def print_demand_table() -> None:
    with get_session() as session:
        stmt = (
            select(
                Supplier.name.label("supplier"),
                Reagent.name.label("reagent"),
                InventorySnapshot.quantity_on_hand.label("inventory"),
                DemandSignal.expected_demand.label("demand"),
                DemandSignal.detected_at.label("detected_at"),
            )
            .join(Reagent, Reagent.supplier_id == Supplier.id)
            .join(InventorySnapshot, InventorySnapshot.reagent_id == Reagent.id)
            .join(DemandSignal, DemandSignal.reagent_id == Reagent.id)
        )

        rows = session.execute(stmt).all()

    columns = ["Suppliers", "Reagents", "Inventory levels", "Demand signals", "Timestamps"]
    print(" | ".join(columns))
    print("-" * 80)
    for row in rows:
        print(
            f"{row.supplier} | {row.reagent} | {row.inventory} | {row.demand} | {row.detected_at:%Y-%m-%d %H:%M:%S}"
        )
    _write_html(rows)


def _write_html(rows: List) -> None:
    """Persist a simple HTML table so non-technical folks get a nicer view."""
    head = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Bloomberg Lab Demand Snapshot</title>
  <style>
    body { font-family: Arial, sans-serif; padding: 2rem; background: #f5f6fa; }
    table { border-collapse: collapse; width: 100%; background: white; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
    th, td { padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid #e1e4eb; }
    th { background: #0f6db5; color: white; position: sticky; top: 0; }
    tr:hover { background: #f0f8ff; }
  </style>
</head>
<body>
  <h1>Supplier Demand Signals</h1>
  <table>
    <tr>
      <th>Supplier</th>
      <th>Reagent</th>
      <th>Inventory levels</th>
      <th>Demand signals</th>
      <th>Timestamps</th>
    </tr>
"""
    body_rows = [
        f"    <tr><td>{row.supplier}</td><td>{row.reagent}</td><td>{row.inventory}</td>"
        f"<td>{row.demand}</td><td>{row.detected_at:%Y-%m-%d %H:%M:%S}</td></tr>"
        for row in rows
    ]
    tail = """  </table>
</body>
</html>
"""
    html = head + "\n".join(body_rows) + "\n" + tail
    output_path = Path("demo_output.html")
    output_path.write_text(html, encoding="utf-8")
    print(f"\nWrote {output_path}. Open it in your browser for the styled table.")


if __name__ == "__main__":
    seed_demo_data()
    print_demand_table()
