#!/usr/bin/env python3
from __future__ import annotations

"""
Convert the Markdown report into a branded PDF inspired by the Rainmaker template.
"""

import argparse
import re
import sys
from pathlib import Path
from typing import List

# Add parent directory to path
# Resolve to absolute path to handle symlinks and relative paths
_script_dir = Path(__file__).resolve().parent.parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle, PageBreak


BRAND_GREEN = colors.HexColor("#0f2623")
BRAND_GOLD = colors.HexColor("#c8a15d")


def build_pdf(md_path: Path, out_pdf: Path) -> None:
    lines = md_path.read_text(encoding="utf-8").splitlines()

    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=LETTER,
        leftMargin=72,
        rightMargin=72,
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
    styles.add(
        ParagraphStyle(
            name="ItalicBrand",
            parent=styles["BodyText"],
            fontSize=11,
            leading=15,
            fontName="Helvetica-Oblique",
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

        table = Table(data, hAlign="LEFT")
        table_style = TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), BRAND_GREEN),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("BACKGROUND", (0, 1), (-1, -1), colors.whitesmoke),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ]
        )
        header_label = data[0][0] if data and data[0] else ""
        is_accel_table = isinstance(header_label, str) and header_label.strip().lower().startswith("accel bin")
        if len(data) > 1 and is_accel_table:
            table_style.add("LINEABOVE", (0, -1), (-1, -1), 1.5, BRAND_GOLD)
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
            flush_bullets()
            flush_table()
            if story and stripped.lower().startswith("### vehicle"):
                story.append(PageBreak())
            story.append(Paragraph(convert_inline(stripped[3:].strip()), styles["Heading3Brand"]))
        elif stripped.startswith("##"):
            story.append(Paragraph(convert_inline(stripped[2:].strip()), styles["Heading2Brand"]))
        elif stripped.startswith("#"):
            story.append(Paragraph(convert_inline(stripped[1:].strip()), styles["Heading1Brand"]))
        elif stripped.startswith("_") and stripped.endswith("_"):
            story.append(Paragraph(f"<i>{convert_inline(stripped.strip('_'))}</i>", styles["ItalicBrand"]))
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

    tagline = "Elijah Flight Analytics"
    canvas.setFont("Helvetica", 12)
    tagline_width = canvas.stringWidth(tagline, "Helvetica", 12)
    canvas.drawString((width - tagline_width) / 2, height - banner_height + 12, tagline)

    canvas.restoreState()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render report.md to branded PDF.")
    parser.add_argument("--report", required=True, help="Path to the Markdown report (e.g., output/report.md)")
    parser.add_argument("--pdf", required=True, help="Destination PDF path (e.g., output/report.pdf)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    md_path = Path(args.report)
    if not md_path.exists():
        raise FileNotFoundError(f"Report not found: {md_path}")
    out_pdf = Path(args.pdf)
    build_pdf(md_path, out_pdf)
    print(f"Rendered PDF saved to {out_pdf}")


if __name__ == "__main__":
    main()


