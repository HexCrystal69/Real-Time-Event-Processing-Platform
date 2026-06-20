"""
GRIP — Data export service.

Exports live pipeline data as CSV, JSON, or PDF reports.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from backend.config.logger import get_logger
from backend.database.connection import get_connection
from backend.services.analytics import get_dashboard_summary
from backend.services.risk_scoring import get_latest_risk_scores

logger = get_logger("export")

EXPORT_TABLES = {
    "earthquakes": ("earthquakes_processed", "event_time"),
    "weather": ("weather_processed", "observed_at"),
    "air_quality": ("air_quality_processed", "observed_at"),
    "wildfires": ("wildfires_processed", "acq_date"),
    "anomalies": ("anomaly_events", "detected_at"),
    "alerts": ("alerts", "created_at"),
}


def _fetch_records(table: str, order_col: str, limit: int = 1000) -> tuple[list[str], list[tuple]]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {table} ORDER BY {order_col} DESC LIMIT %s",
                (limit,),
            )
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    return cols, rows


def export_csv(data_type: str, limit: int = 1000) -> str:
    """Export data as CSV string."""
    if data_type not in EXPORT_TABLES:
        raise ValueError(f"Unknown data type: {data_type}")

    table, order_col = EXPORT_TABLES[data_type]
    cols, rows = _fetch_records(table, order_col, limit)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(cols)
    for row in rows:
        writer.writerow([
            val.isoformat() if isinstance(val, datetime) else val
            for val in row
        ])
    return output.getvalue()


def export_json(data_type: str, limit: int = 1000) -> str:
    """Export data as JSON string."""
    if data_type not in EXPORT_TABLES:
        raise ValueError(f"Unknown data type: {data_type}")

    table, order_col = EXPORT_TABLES[data_type]
    cols, rows = _fetch_records(table, order_col, limit)

    records = []
    for row in rows:
        record = {}
        for i, col in enumerate(cols):
            val = row[i]
            if isinstance(val, datetime):
                val = val.isoformat()
            record[col] = val
        records.append(record)

    return json.dumps({
        "data_type": data_type,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "count": len(records),
        "records": records,
    }, indent=2, default=str)


def export_pdf_report() -> bytes:
    """Generate a PDF intelligence report from live data."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    elements: list[Any] = []

    elements.append(Paragraph(
        "GRIP — Global Risk Intelligence Report",
        styles["Title"],
    ))
    elements.append(Paragraph(
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        styles["Normal"],
    ))
    elements.append(Spacer(1, 20))

    summary = get_dashboard_summary()
    summary_data = [
        ["Metric", "Value"],
        ["Total Events", str(summary["total_events"])],
        ["Events (Last Hour)", str(summary["events_last_hour"])],
        ["Active Alerts", str(summary["active_alerts"])],
        ["Anomalies", str(summary["total_anomalies"])],
    ]
    summary_table = Table(summary_data, colWidths=[250, 200])
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
    ]))
    elements.append(Paragraph("Dashboard Summary", styles["Heading2"]))
    elements.append(summary_table)
    elements.append(Spacer(1, 20))

    risk_scores = get_latest_risk_scores()
    if risk_scores:
        elements.append(Paragraph("Regional Risk Scores", styles["Heading2"]))
        risk_data = [["Region", "Unified Score", "Risk Level"]]
        for rs in risk_scores:
            risk_data.append([
                rs["region_name"],
                str(rs["unified_score"]),
                rs["risk_level"],
            ])
        risk_table = Table(risk_data, colWidths=[200, 150, 150])
        risk_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("FONTSIZE", (0, 0), (-1, -1), 10),
        ]))
        elements.append(risk_table)

    doc.build(elements)
    return buffer.getvalue()
