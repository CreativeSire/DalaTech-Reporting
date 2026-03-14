"""
coach_operations.py

Operational runners for Sales Coach backfill, refresh, and threshold
validation. These functions intentionally keep computation deterministic and
persist coach state through the existing datastore primitives.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

import pandas as pd

from .coach_features import (
    _between,
    _report_window,
    build_scope_snapshot,
    load_sales_history,
)
from .coach_signals import build_coach_payload, derive_snapshot_signals, get_signal_thresholds


def _collect_targets(ds, report_id: int | None = None, month_value: str | None = None) -> dict[str, Any]:
    df = load_sales_history()
    if df.empty:
        return {
            "df": df,
            "window": {
                "label": "",
                "start_date": None,
                "end_date": None,
                "period_type": "monthly",
                "report": None,
            },
            "brands": [],
            "retailers": [],
            "pairs": [],
        }

    window = _report_window(ds, df, report_id=report_id, month_value=month_value)
    current_frame = _between(df, window["start_date"], window["end_date"])
    sales_frame = current_frame[current_frame["Vch Type"] == "Sales"].copy()

    brands = sorted(
        {
            ds.analytics_brand_name(value)
            for value in sales_frame["Brand Partner Canonical"].dropna().tolist()
            if str(value).strip()
        }
    )
    retailers = sorted(
        {
            str(value).strip()
            for value in sales_frame["Retailer Key"].dropna().tolist()
            if str(value).strip()
        }
    )
    pairs = []
    if not sales_frame.empty:
        pair_df = (
            sales_frame.groupby(["Brand Partner Canonical", "Retailer Key"])["Sales_Value"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        pairs = [
            {
                "brand_name": ds.analytics_brand_name(row["Brand Partner Canonical"]),
                "retailer_code": str(row["Retailer Key"]).strip(),
                "revenue": round(float(row["Sales_Value"] or 0), 2),
            }
            for _, row in pair_df.iterrows()
            if str(row["Brand Partner Canonical"]).strip() and str(row["Retailer Key"]).strip()
        ]
    return {
        "df": df,
        "window": window,
        "brands": brands,
        "retailers": retailers,
        "pairs": pairs,
    }


def _progress(progress_cb, current: int, total: int, message: str):
    if not progress_cb:
        return
    pct = int(round((current / max(total, 1)) * 100))
    progress_cb(pct, message)


def run_coach_refresh(
    ds,
    report_id: int | None = None,
    month_value: str | None = None,
    include_pairs: bool = True,
    persist: bool = True,
    progress_cb=None,
) -> dict[str, Any]:
    target_set = _collect_targets(ds, report_id=report_id, month_value=month_value)
    window = target_set["window"]
    brands = target_set["brands"]
    retailers = target_set["retailers"]
    pairs = target_set["pairs"] if include_pairs else []

    tasks = [("portfolio", "global", None)]
    tasks.extend(("brand", brand_name, None) for brand_name in brands)
    tasks.extend(("retailer", retailer_code, None) for retailer_code in retailers)
    tasks.extend(("brand_retailer", item["brand_name"], item["retailer_code"]) for item in pairs)

    signal_counter: Counter[str] = Counter()
    severity_counter: Counter[str] = Counter()
    snapshots = []
    total = len(tasks)

    for index, (scope_type, scope_key, retailer_code) in enumerate(tasks, start=1):
        label = scope_key if scope_type != "portfolio" else "portfolio"
        _progress(progress_cb, index - 1, total, f"Refreshing {scope_type}: {label}")
        snapshot = build_scope_snapshot(
            ds,
            scope_type,
            scope_key=scope_key if scope_type != "portfolio" else "global",
            report_id=report_id,
            month_value=month_value,
            retailer_code=retailer_code,
            persist=persist,
        )
        coach = build_coach_payload(ds, snapshot, persist=persist, use_gemini=False)
        snapshots.append({
            "scope_type": snapshot.get("scope_type"),
            "scope_key": snapshot.get("scope_key"),
            "signal_count": len(coach.get("signals") or []),
        })
        for signal in coach.get("signals") or []:
            signal_counter.update([signal.get("signal_type") or "unknown"])
            severity_counter.update([signal.get("severity") or "low"])
        _progress(progress_cb, index, total, f"Refreshed {scope_type}: {label}")

    return {
        "period_label": window.get("label"),
        "period_start": window.get("start_date"),
        "period_end": window.get("end_date"),
        "period_type": window.get("period_type"),
        "report_id": window.get("report", {}).get("id") if window.get("report") else report_id,
        "counts": {
            "portfolio": 1,
            "brands": len(brands),
            "retailers": len(retailers),
            "brand_retailer_pairs": len(pairs),
            "snapshots": len(tasks),
            "signals": int(sum(signal_counter.values())),
        },
        "signal_counts": dict(signal_counter),
        "severity_counts": dict(severity_counter),
        "snapshots": snapshots[:50],
    }


def backfill_recent_periods(
    ds,
    monthly_count: int = 6,
    weekly_count: int = 4,
    include_pairs: bool = True,
    progress_cb=None,
) -> dict[str, Any]:
    reports = ds.get_all_reports()
    monthly_reports = [row for row in reports if str(row.get("report_type") or "").lower() == "monthly"][:monthly_count]
    weekly_reports = [row for row in reports if str(row.get("report_type") or "").lower() == "weekly"][:weekly_count]
    selected = monthly_reports + weekly_reports

    results = []
    signal_counter: Counter[str] = Counter()
    severity_counter: Counter[str] = Counter()
    total_reports = len(selected)

    for index, report in enumerate(selected, start=1):
        base_pct = int(((index - 1) / max(total_reports, 1)) * 100)

        def _report_progress(inner_pct: int, message: str):
            if not progress_cb:
                return
            span = 100 / max(total_reports, 1)
            overall = min(99, int(base_pct + (inner_pct / 100.0) * span))
            progress_cb(overall, f"{report.get('month_label')}: {message}")

        refresh = run_coach_refresh(
            ds,
            report_id=report["id"],
            include_pairs=include_pairs,
            persist=True,
            progress_cb=_report_progress,
        )
        results.append({
            "report_id": report["id"],
            "month_label": report.get("month_label"),
            "report_type": report.get("report_type"),
            "counts": refresh.get("counts", {}),
            "signal_counts": refresh.get("signal_counts", {}),
        })
        signal_counter.update(refresh.get("signal_counts", {}))
        severity_counter.update(refresh.get("severity_counts", {}))

    if progress_cb:
        progress_cb(100, "Coach backfill complete")

    return {
        "processed_reports": len(selected),
        "monthly_reports": len(monthly_reports),
        "weekly_reports": len(weekly_reports),
        "reports": results,
        "signal_counts": dict(signal_counter),
        "severity_counts": dict(severity_counter),
    }


def validate_signal_quality(
    ds,
    report_id: int | None = None,
    month_value: str | None = None,
    include_pairs: bool = False,
    sample_limit: int = 5,
) -> dict[str, Any]:
    target_set = _collect_targets(ds, report_id=report_id, month_value=month_value)
    window = target_set["window"]
    samples: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    counts: Counter[str] = Counter()
    severities: Counter[str] = Counter()

    tasks = [("portfolio", "global", None)]
    tasks.extend(("brand", brand_name, None) for brand_name in target_set["brands"])
    tasks.extend(("retailer", retailer_code, None) for retailer_code in target_set["retailers"])
    if include_pairs:
        tasks.extend(("brand_retailer", item["brand_name"], item["retailer_code"]) for item in target_set["pairs"])

    for scope_type, scope_key, retailer_code in tasks:
        snapshot = build_scope_snapshot(
            ds,
            scope_type,
            scope_key=scope_key if scope_type != "portfolio" else "global",
            report_id=report_id,
            month_value=month_value,
            retailer_code=retailer_code,
            persist=False,
        )
        for signal in derive_snapshot_signals(snapshot):
            signal_type = signal.get("signal_type") or "unknown"
            counts.update([signal_type])
            severities.update([signal.get("severity") or "low"])
            if len(samples[signal_type]) < sample_limit:
                samples[signal_type].append({
                    "scope_type": signal.get("scope_type"),
                    "scope_key": signal.get("scope_key"),
                    "severity": signal.get("severity"),
                    "confidence": signal.get("confidence"),
                    "message": (signal.get("evidence") or {}).get("message"),
                })

    return {
        "period_label": window.get("label"),
        "period_start": window.get("start_date"),
        "period_end": window.get("end_date"),
        "thresholds": get_signal_thresholds(),
        "signal_counts": dict(counts),
        "severity_counts": dict(severities),
        "samples": dict(samples),
    }
