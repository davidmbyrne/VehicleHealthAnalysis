#!/usr/bin/env python3
from __future__ import annotations

"""
Convert the risk analysis Markdown report into a branded PDF.
"""

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import List, Set

# Add parent directory to path
# Resolve to absolute path to handle symlinks and relative paths
_script_dir = Path(__file__).resolve().parent.parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


BRAND_GREEN = colors.HexColor("#0f2623")
BRAND_GOLD = colors.HexColor("#c8a15d")
DEAD_VEHICLE_COLOR = colors.HexColor("#ffcccc")  # Light red


def load_dead_vehicles(is_dead_csv: Path | None = None) -> Set[str]:
    """Load set of dead vehicle IDs from CSV file."""
    dead_vehicles: Set[str] = set()
    
    # Check multiple possible locations
    if is_dead_csv is None:
        # Try config directory first, then root
        csv_path = Path("config/isDead.csv")
        if not csv_path.exists():
            csv_path = Path("isDead.csv")
    else:
        csv_path = is_dead_csv
    
    if not csv_path.exists():
        return dead_vehicles
    
    try:
        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                vehicle_id = row.get("vehicle_id", "").strip()
                dead = row.get("dead", "0").strip()
                if dead == "1" and vehicle_id:
                    dead_vehicles.add(vehicle_id.upper())
    except Exception as e:
        # If CSV can't be read, log and return empty set
        import sys
        print(f"Warning: Could not read dead vehicles CSV: {e}", file=sys.stderr)
    
    return dead_vehicles


def build_pdf(md_path: Path, out_pdf: Path, is_dead_csv: Path | None = None) -> None:
    lines = md_path.read_text(encoding="utf-8").splitlines()
    dead_vehicles = load_dead_vehicles(is_dead_csv)
    
    # Debug: print loaded dead vehicles
    if dead_vehicles:
        import sys
        print(f"Loaded {len(dead_vehicles)} dead vehicles from CSV", file=sys.stderr)

    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=LETTER,
        leftMargin=36,  # Reduced margins for more table space
        rightMargin=36,
        topMargin=130,
        bottomMargin=72,
    )

    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="Heading1Brand",
            parent=styles["Heading1"],
            fontSize=20,
            leading=24,
            textColor=BRAND_GREEN,
            spaceAfter=12,
        )
    )
    styles.add(
        ParagraphStyle(
            name="Heading2Brand",
            parent=styles["Heading2"],
            fontSize=14,
            leading=18,
            textColor=BRAND_GREEN,
            spaceBefore=12,
            spaceAfter=6,
        )
    )
    styles.add(
        ParagraphStyle(
            name="Heading3Brand",
            parent=styles["Heading3"],
            fontSize=12,
            leading=16,
            textColor=BRAND_GREEN,
            spaceBefore=10,
            spaceAfter=4,
        )
    )
    styles.add(
        ParagraphStyle(
            name="BodyBrand",
            parent=styles["BodyText"],
            fontSize=11,
            leading=15,
        )
    )
    styles.add(
        ParagraphStyle(
            name="BulletBrand",
            parent=styles["BodyText"],
            fontSize=11,
            leading=15,
            leftIndent=18,
            bulletIndent=8,
        )
    )

    story = []
    bullet_buffer: List[str] = []
    table_buffer: List[str] = []

    def flush_bullets() -> None:
        if not bullet_buffer:
            return
        for item in bullet_buffer:
            story.append(Paragraph(item, styles["BulletBrand"], bulletText="•"))
        story.append(Spacer(1, 8))
        bullet_buffer.clear()

    def flush_table() -> None:
        if not table_buffer:
            return
        data: List[List[str]] = []
        for idx, raw in enumerate(table_buffer):
            row = parse_table_row(raw)
            if not row:
                continue
            if idx == 1 and _is_alignment_row(row):
                continue
            data.append(row)

        if not data:
            table_buffer.clear()
            return

        # Calculate table width (LETTER width minus margins)
        page_width = LETTER[0] - 72  # 36pt left + 36pt right margins
        num_cols = len(data[0]) if data else 0
        
        # Define proportional column widths for the risk table (12 columns)
        # Widths sum to ~1.0 to use full page width
        if num_cols == 12:  # Risk report table format
            col_widths = [
                page_width * 0.07,  # Rank
                page_width * 0.11,  # Vehicle
                page_width * 0.08,  # Risk Score
                page_width * 0.07,  # Vib
                page_width * 0.07,  # Motor
                page_width * 0.08,  # Fatigue
                page_width * 0.09,  # High Vib %
                page_width * 0.08,  # Sat %
                page_width * 0.09,  # Peak Events
                page_width * 0.10,  # Clipping Events
                page_width * 0.11,  # Flight Time
                page_width * 0.05,  # Logs
            ]
        else:
            # Fallback: equal widths
            col_widths = [page_width / num_cols] * num_cols

        table = Table(data, colWidths=col_widths, hAlign="LEFT")
        table_style = TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), BRAND_GREEN),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 7),  # Smaller header font
                ("ALIGN", (0, 0), (0, -1), "CENTER"),  # Rank center-aligned
                ("ALIGN", (1, 0), (1, -1), "LEFT"),  # Vehicle left-aligned
                ("ALIGN", (2, 0), (-1, -1), "CENTER"),  # Other columns center-aligned
                ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 1), (-1, -1), 7),  # Smaller body font
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),  # Vertical center alignment
            ]
        )
        
        # Apply row backgrounds: check for dead vehicles
        # Vehicle ID is in column 1 (index 1) of the table
        for row_idx in range(1, len(data)):  # Skip header row
            if row_idx < len(data):
                vehicle_cell = data[row_idx][1] if len(data[row_idx]) > 1 else ""
                vehicle_id = str(vehicle_cell).strip().upper()
                
                if vehicle_id in dead_vehicles:
                    # Dead vehicle: light red background
                    table_style.add("BACKGROUND", (0, row_idx), (-1, row_idx), DEAD_VEHICLE_COLOR)
                else:
                    # Normal vehicle: alternating white/whitesmoke
                    bg_color = colors.white if row_idx % 2 == 1 else colors.whitesmoke
                    table_style.add("BACKGROUND", (0, row_idx), (-1, row_idx), bg_color)
        table.setStyle(table_style)
        story.append(table)
        story.append(Spacer(1, 12))
        table_buffer.clear()

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("|") and stripped.endswith("|"):
            flush_bullets()
            table_buffer.append(stripped)
            continue
        else:
            flush_table()

        if not stripped:
            flush_bullets()
            story.append(Spacer(1, 10))
            continue

        if stripped.startswith("- "):
            item = stripped[2:].strip()
            bullet_buffer.append(convert_inline(item))
            continue

        flush_bullets()

        if stripped.startswith("###"):
            story.append(Paragraph(convert_inline(stripped[3:].strip()), styles["Heading3Brand"]))
        elif stripped.startswith("##"):
            story.append(Paragraph(convert_inline(stripped[2:].strip()), styles["Heading2Brand"]))
        elif stripped.startswith("#"):
            story.append(Paragraph(convert_inline(stripped[1:].strip()), styles["Heading1Brand"]))
        else:
            story.append(Paragraph(convert_inline(stripped), styles["BodyBrand"]))

    flush_bullets()
    flush_table()

    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    doc.build(story, onFirstPage=draw_brand_header, onLaterPages=draw_brand_header)


def parse_table_row(line: str) -> List[str]:
    cells = [c.strip() for c in line.strip().split("|")]
    return [cell for cell in cells if cell]


def convert_inline(text: str) -> str:
    # Bold (**text** or __text__)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"__(.+?)__", r"<b>\1</b>", text)
    # Italic (*text* or _text_)
    text = re.sub(r"\*(.+?)\*", r"<i>\1</i>", text)
    text = re.sub(r"_(.+?)_", r"<i>\1</i>", text)
    return text


def _is_alignment_row(cells: List[str]) -> bool:
    if not cells:
        return False
    for cell in cells:
        cleaned = cell.replace("-", "").replace(":", "").strip()
        if cleaned:
            return False
        if "-" not in cell:
            return False
    return True


def draw_brand_header(canvas, doc) -> None:
    canvas.saveState()
    width, height = LETTER
    banner_height = 80
    canvas.setFillColor(BRAND_GREEN)
    canvas.rect(0, height - banner_height, width, banner_height, stroke=0, fill=1)

    canvas.setFont("Helvetica-Bold", 26)
    canvas.setFillColor(BRAND_GOLD)
    text = "RAINMAKER"
    text_width = canvas.stringWidth(text, "Helvetica-Bold", 26)
    canvas.drawString((width - text_width) / 2, height - banner_height + 30, text)

    tagline = "Vehicle Risk Analysis"
    canvas.setFont("Helvetica", 12)
    tagline_width = canvas.stringWidth(tagline, "Helvetica", 12)
    canvas.drawString((width - tagline_width) / 2, height - banner_height + 12, tagline)

    canvas.restoreState()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render risk report.md to branded PDF.")
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("output/risk_report.md"),
        help="Path to the risk Markdown report (default: output/risk_report.md)",
    )
    parser.add_argument(
        "--pdf",
        type=Path,
        default=Path("output/risk_report.pdf"),
        help="Destination PDF path (default: output/risk_report.pdf)",
    )
    parser.add_argument(
        "--is_dead_csv",
        type=Path,
        default=None,
        help="Path to CSV file with dead vehicle information (default: checks config/isDead.csv then isDead.csv)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    md_path = Path(args.report)
    if not md_path.exists():
        raise FileNotFoundError(f"Risk report not found: {md_path}")
    out_pdf = Path(args.pdf)
    build_pdf(md_path, out_pdf, is_dead_csv=args.is_dead_csv)
    print(f"Risk report PDF saved to {out_pdf}")


if __name__ == "__main__":
    main()

