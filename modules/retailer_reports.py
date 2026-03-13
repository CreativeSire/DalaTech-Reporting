"""
retailer_reports.py

HTML renderers for retailer intelligence reports. The interactive HTML is used
directly in-browser and the print HTML is passed to the existing Playwright PDF
pipeline.
"""

from __future__ import annotations

import os
from datetime import datetime

from jinja2 import Environment, FileSystemLoader, select_autoescape

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")

jinja_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(["html", "xml"]),
)


def _money(value) -> str:
    try:
        return f"{float(value or 0):,.2f}"
    except Exception:
        return "0.00"


jinja_env.filters["money2"] = _money


def render_retailer_html_report(detail: dict) -> str:
    template = jinja_env.get_template("report_retailer_interactive.html")
    return template.render(
        detail=detail,
        generated_at=datetime.now().strftime("%d %b %Y %H:%M"),
    )


def render_retailer_pdf_report_html(detail: dict) -> str:
    template = jinja_env.get_template("report_retailer_template.html")
    return template.render(
        detail=detail,
        generated_at=datetime.now().strftime("%d %b %Y %H:%M"),
    )
