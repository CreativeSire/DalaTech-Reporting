"""
coach_features.py

Deterministic feature builders for the Sales Coach and Retailer Intelligence
surfaces. These helpers work from the bundled historical workbook plus the
existing SQLite datastore so Gemini reasons over structured facts instead of
raw rows.
"""

from __future__ import annotations

import os
from calendar import monthrange
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from .brand_names import canonicalize_brand_name
from .ingestion import load_and_clean
from .predictor import build_brand_forecasts

BASE_DIR = Path(__file__).resolve().parents[1]
HISTORY_PATH = BASE_DIR / "2024to2026salesreport.xlsx"


def history_available(path: str | os.PathLike[str] | None = None) -> bool:
    return Path(path or HISTORY_PATH).is_file()


@lru_cache(maxsize=4)
def _load_sales_history_cached(path_str: str, mtime: float) -> pd.DataFrame:
    df = load_and_clean(path_str)
    df["Brand Partner Canonical"] = (
        df["Brand Partner"].fillna("").astype(str).map(canonicalize_brand_name)
    )
    df["Retailer Key"] = df["Particulars"].fillna("").astype(str).str.strip()
    df["YearMonth"] = df["Date"].dt.to_period("M")
    return df


def load_sales_history(path: str | os.PathLike[str] | None = None) -> pd.DataFrame:
    target = Path(path or HISTORY_PATH)
    if not target.is_file():
        return pd.DataFrame()
    df = _load_sales_history_cached(str(target), target.stat().st_mtime)
    return df.copy()


def _native(value: Any):
    if hasattr(value, "item"):
        return value.item()
    return value


def _to_period(month_value: str | None):
    if not month_value:
        return None
    try:
        year, month = map(int, str(month_value).split("-"))
        return pd.Period(year=year, month=month, freq="M")
    except Exception:
        return None


def _latest_available_month(df: pd.DataFrame):
    sales_df = df[df["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return None
    periods = sorted(sales_df["YearMonth"].dropna().unique())
    return periods[-1] if periods else None


def _month_bounds(period: pd.Period) -> tuple[str, str]:
    start = pd.Timestamp(year=period.year, month=period.month, day=1)
    end = pd.Timestamp(year=period.year, month=period.month, day=monthrange(period.year, period.month)[1])
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _report_window(ds, df: pd.DataFrame, report_id: int | None = None,
                   period_type: str = "monthly", month_value: str | None = None):
    report = ds.get_report(report_id) if report_id else None
    reports = ds.get_all_reports()
    if report:
        current_type = str(report.get("report_type") or period_type or "monthly").lower()
        previous = next(
            (
                row for row in reports
                if row.get("report_type") == current_type and row["start_date"] < report["start_date"]
            ),
            None,
        )
        return {
            "period_type": current_type,
            "report": report,
            "label": report.get("month_label") or report["start_date"],
            "start_date": report["start_date"],
            "end_date": report["end_date"],
            "previous_report": previous,
            "previous_start_date": previous.get("start_date") if previous else None,
            "previous_end_date": previous.get("end_date") if previous else None,
        }

    selected_period = _to_period(month_value)
    if selected_period is None:
        selected_period = _latest_available_month(df)
    if selected_period is None:
        return {
            "period_type": "monthly",
            "report": None,
            "label": "",
            "start_date": None,
            "end_date": None,
            "previous_report": None,
            "previous_start_date": None,
            "previous_end_date": None,
        }

    current_start, current_end = _month_bounds(selected_period)
    previous_period = selected_period - 1
    prev_start, prev_end = _month_bounds(previous_period)
    return {
        "period_type": "monthly",
        "report": ds.get_report_by_month(selected_period.year, selected_period.month, "monthly"),
        "label": selected_period.strftime("%b %Y"),
        "start_date": current_start,
        "end_date": current_end,
        "previous_report": ds.get_report_by_month(previous_period.year, previous_period.month, "monthly"),
        "previous_start_date": prev_start,
        "previous_end_date": prev_end,
    }


def _filter_scope(ds, df: pd.DataFrame, scope_type: str, scope_key: str | None = None,
                  retailer_code: str | None = None) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    scope_type = (scope_type or "portfolio").strip().lower()
    if scope_type == "brand":
        brand_name = ds.analytics_brand_name(scope_key or "")
        return df[df["Brand Partner Canonical"] == brand_name].copy()
    if scope_type == "retailer":
        retailer_code = str(scope_key or "").strip()
        return df[df["Retailer Key"] == retailer_code].copy()
    if scope_type == "brand_retailer":
        brand_name = ds.analytics_brand_name(scope_key or "")
        retailer_code = str(retailer_code or "").strip()
        scoped = df[df["Brand Partner Canonical"] == brand_name].copy()
        if retailer_code:
            scoped = scoped[scoped["Retailer Key"] == retailer_code].copy()
        return scoped
    return df.copy()


def _between(df: pd.DataFrame, start_date: str | None, end_date: str | None) -> pd.DataFrame:
    if df.empty or not start_date or not end_date:
        return df.iloc[0:0].copy() if df.empty else df.copy()
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    return df[(df["Date"] >= start) & (df["Date"] <= end)].copy()


def _money(value: float) -> float:
    return round(float(value or 0), 2)


def _pct_delta(current_value: float, previous_value: float) -> float | None:
    previous_value = float(previous_value or 0)
    current_value = float(current_value or 0)
    if previous_value == 0:
        return None if current_value == 0 else 100.0
    return round(((current_value - previous_value) / previous_value) * 100, 2)


def _count_delta(current_value: float, previous_value: float) -> float:
    return round(float(current_value or 0) - float(previous_value or 0), 2)


def _portfolio_brand_rank(df: pd.DataFrame, brand_name: str) -> tuple[int | None, int]:
    sales_df = df[df["Vch Type"] == "Sales"].copy()
    if sales_df.empty or not brand_name:
        return None, 0
    ranked = (
        sales_df.groupby("Brand Partner Canonical")["Sales_Value"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    ranked["rank"] = range(1, len(ranked) + 1)
    row = ranked[ranked["Brand Partner Canonical"] == brand_name]
    return (int(row.iloc[0]["rank"]), len(ranked)) if not row.empty else (None, len(ranked))


def _metrics_for_scope_frame(frame: pd.DataFrame, scope_type: str) -> dict[str, Any]:
    sales_df = frame[frame["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return {
            "revenue": 0.0,
            "quantity": 0.0,
            "transactions": 0,
            "active_days": 0,
            "unique_skus": 0,
            "active_stores": 0,
            "active_brands": 0,
            "repeat_rate": 0.0,
            "repeat_entities": 0,
            "single_entities": 0,
            "avg_revenue_per_entity": 0.0,
            "top_store_name": None,
            "top_store_revenue": 0.0,
            "top_brand_name": None,
            "top_brand_revenue": 0.0,
        }

    revenue = _money(sales_df["Sales_Value"].sum())
    quantity = _money(sales_df["Quantity"].sum())
    transactions = int(len(sales_df))
    active_days = int(sales_df["Date"].dt.date.nunique())
    unique_skus = int(sales_df["SKUs"].nunique())
    active_stores = int(sales_df["Retailer Key"].nunique())
    active_brands = int(sales_df["Brand Partner Canonical"].nunique())

    if scope_type in {"portfolio", "brand"}:
        entity_orders = sales_df.groupby("Retailer Key").size()
        entity_count = active_stores
    elif scope_type == "retailer":
        entity_orders = sales_df.groupby("Brand Partner Canonical").size()
        entity_count = active_brands
    else:
        entity_orders = pd.Series([transactions], dtype=float)
        entity_count = 1 if transactions else 0

    repeat_entities = int((entity_orders > 1).sum()) if len(entity_orders) else 0
    single_entities = int((entity_orders == 1).sum()) if len(entity_orders) else 0
    repeat_rate = round((repeat_entities / entity_count) * 100, 2) if entity_count else 0.0
    avg_revenue_per_entity = round(revenue / max(entity_count, 1), 2)

    top_store = (
        sales_df.groupby("Retailer Key")["Sales_Value"].sum().sort_values(ascending=False)
        if not sales_df.empty else pd.Series(dtype=float)
    )
    top_brand = (
        sales_df.groupby("Brand Partner Canonical")["Sales_Value"].sum().sort_values(ascending=False)
        if not sales_df.empty else pd.Series(dtype=float)
    )

    return {
        "revenue": revenue,
        "quantity": quantity,
        "transactions": transactions,
        "active_days": active_days,
        "unique_skus": unique_skus,
        "active_stores": active_stores,
        "active_brands": active_brands,
        "repeat_rate": repeat_rate,
        "repeat_entities": repeat_entities,
        "single_entities": single_entities,
        "avg_revenue_per_entity": avg_revenue_per_entity,
        "top_store_name": str(top_store.index[0]) if len(top_store) else None,
        "top_store_revenue": _money(top_store.iloc[0]) if len(top_store) else 0.0,
        "top_brand_name": str(top_brand.index[0]) if len(top_brand) else None,
        "top_brand_revenue": _money(top_brand.iloc[0]) if len(top_brand) else 0.0,
    }


def _monthly_history(scope_df: pd.DataFrame, scope_type: str, limit: int = 12) -> list[dict[str, Any]]:
    sales_df = scope_df[scope_df["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return []
    periods = sorted(sales_df["YearMonth"].dropna().unique())
    history: list[dict[str, Any]] = []
    previous = None
    for period in periods[-limit:]:
        period_frame = sales_df[sales_df["YearMonth"] == period].copy()
        metrics = _metrics_for_scope_frame(period_frame, scope_type)
        row = {
            "month_label": period.strftime("%b %Y"),
            "period_start": f"{period.year:04d}-{period.month:02d}-01",
            "period_end": f"{period.year:04d}-{period.month:02d}-{monthrange(period.year, period.month)[1]:02d}",
            **metrics,
        }
        if previous:
            row["revenue_mom"] = _pct_delta(row["revenue"], previous["revenue"])
            row["repeat_rate_delta"] = _count_delta(row["repeat_rate"], previous["repeat_rate"])
        else:
            row["revenue_mom"] = None
            row["repeat_rate_delta"] = 0.0
        history.append(row)
        previous = row
    return history


def _top_brands(frame: pd.DataFrame, limit: int = 10) -> list[dict[str, Any]]:
    sales_df = frame[frame["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return []
    grouped = (
        sales_df.groupby("Brand Partner Canonical")
        .agg(
            revenue=("Sales_Value", "sum"),
            quantity=("Quantity", "sum"),
            transactions=("Vch No.", "nunique"),
            active_days=("Date", lambda s: s.dt.date.nunique()),
        )
        .sort_values("revenue", ascending=False)
        .head(limit)
        .reset_index()
    )
    total = float(grouped["revenue"].sum() or 0)
    rows = []
    for _, row in grouped.iterrows():
        rows.append({
            "brand_name": str(row["Brand Partner Canonical"]),
            "revenue": _money(row["revenue"]),
            "quantity": _money(row["quantity"]),
            "transactions": int(row["transactions"]),
            "active_days": int(row["active_days"]),
            "share_pct": round((float(row["revenue"]) / total) * 100, 2) if total else 0.0,
        })
    return rows


def _top_products(frame: pd.DataFrame, limit: int = 12) -> list[dict[str, Any]]:
    sales_df = frame[frame["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return []
    grouped = (
        sales_df.groupby("SKUs")
        .agg(revenue=("Sales_Value", "sum"), quantity=("Quantity", "sum"), transactions=("Vch No.", "nunique"))
        .sort_values("revenue", ascending=False)
        .head(limit)
        .reset_index()
    )
    return [
        {
            "sku": str(row["SKUs"]),
            "revenue": _money(row["revenue"]),
            "quantity": _money(row["quantity"]),
            "transactions": int(row["transactions"]),
        }
        for _, row in grouped.iterrows()
    ]


def _top_retailers_for_brand(current_frame: pd.DataFrame, previous_frame: pd.DataFrame, limit: int = 10):
    current_sales = current_frame[current_frame["Vch Type"] == "Sales"].copy()
    previous_sales = previous_frame[previous_frame["Vch Type"] == "Sales"].copy()
    if current_sales.empty and previous_sales.empty:
        return []
    current_grouped = current_sales.groupby("Retailer Key").agg(
        revenue=("Sales_Value", "sum"),
        quantity=("Quantity", "sum"),
        transactions=("Vch No.", "nunique"),
        unique_skus=("SKUs", "nunique"),
    )
    previous_grouped = previous_sales.groupby("Retailer Key").agg(revenue=("Sales_Value", "sum"))
    rows = []
    total = float(current_grouped["revenue"].sum() or 0)
    for retailer_name, row in current_grouped.sort_values("revenue", ascending=False).head(limit).iterrows():
        previous_revenue = float(previous_grouped.loc[retailer_name]["revenue"]) if retailer_name in previous_grouped.index else 0.0
        rows.append({
            "retailer_code": str(retailer_name),
            "retailer_name": str(retailer_name),
            "revenue": _money(row["revenue"]),
            "quantity": _money(row["quantity"]),
            "transactions": int(row["transactions"]),
            "unique_skus": int(row["unique_skus"]),
            "share_pct": round((float(row["revenue"]) / total) * 100, 2) if total else 0.0,
            "revenue_mom": _pct_delta(float(row["revenue"]), previous_revenue),
            "previous_revenue": _money(previous_revenue),
        })
    return rows


def _brand_rows_for_retailer(current_frame: pd.DataFrame, previous_frame: pd.DataFrame, limit: int = 12):
    current_sales = current_frame[current_frame["Vch Type"] == "Sales"].copy()
    previous_sales = previous_frame[previous_frame["Vch Type"] == "Sales"].copy()
    if current_sales.empty:
        return []
    current_grouped = current_sales.groupby("Brand Partner Canonical").agg(
        revenue=("Sales_Value", "sum"),
        quantity=("Quantity", "sum"),
        transactions=("Vch No.", "nunique"),
        active_days=("Date", lambda s: s.dt.date.nunique()),
        unique_skus=("SKUs", "nunique"),
    )
    previous_grouped = previous_sales.groupby("Brand Partner Canonical").agg(revenue=("Sales_Value", "sum"))
    total = float(current_grouped["revenue"].sum() or 0)
    rows = []
    for brand_name, row in current_grouped.sort_values("revenue", ascending=False).head(limit).iterrows():
        previous_revenue = float(previous_grouped.loc[brand_name]["revenue"]) if brand_name in previous_grouped.index else 0.0
        rows.append({
            "brand_name": str(brand_name),
            "revenue": _money(row["revenue"]),
            "quantity": _money(row["quantity"]),
            "transactions": int(row["transactions"]),
            "active_days": int(row["active_days"]),
            "unique_skus": int(row["unique_skus"]),
            "share_pct": round((float(row["revenue"]) / total) * 100, 2) if total else 0.0,
            "revenue_mom": _pct_delta(float(row["revenue"]), previous_revenue),
            "previous_revenue": _money(previous_revenue),
            "repeat_ready": bool(row["transactions"] > 1),
        })
    return rows


def _opportunity_brands(ds, global_frame: pd.DataFrame, current_frame: pd.DataFrame, limit: int = 8):
    current_brands = {
        str(value).strip()
        for value in current_frame["Brand Partner Canonical"].dropna().tolist()
        if str(value).strip()
    }
    portfolio_rows = _top_brands(global_frame, limit=50)
    missing = []
    for row in portfolio_rows:
        if row["brand_name"] in current_brands:
            continue
        rank, peer_count = _portfolio_brand_rank(global_frame, row["brand_name"])
        row["portfolio_rank"] = rank
        row["peer_count"] = peer_count
        missing.append(row)
        if len(missing) >= limit:
            break
    return missing


def build_scope_snapshot(ds, scope_type: str, scope_key: str | None = None,
                         report_id: int | None = None, month_value: str | None = None,
                         retailer_code: str | None = None, persist: bool = True) -> dict[str, Any]:
    df = load_sales_history()
    scope_df = _filter_scope(ds, df, scope_type, scope_key=scope_key, retailer_code=retailer_code)
    window = _report_window(ds, scope_df if not scope_df.empty else df, report_id=report_id, month_value=month_value)

    global_current_frame = _between(df, window["start_date"], window["end_date"])
    current_frame = _between(scope_df, window["start_date"], window["end_date"])
    previous_frame = _between(scope_df, window["previous_start_date"], window["previous_end_date"])
    current_metrics = _metrics_for_scope_frame(current_frame, scope_type)
    previous_metrics = _metrics_for_scope_frame(previous_frame, scope_type)
    monthly_history = _monthly_history(scope_df, scope_type, limit=12)

    comparisons = {
        "revenue_mom": _pct_delta(current_metrics["revenue"], previous_metrics["revenue"]),
        "quantity_mom": _pct_delta(current_metrics["quantity"], previous_metrics["quantity"]),
        "repeat_rate_delta": _count_delta(current_metrics["repeat_rate"], previous_metrics["repeat_rate"]),
        "active_store_delta": _count_delta(current_metrics["active_stores"], previous_metrics["active_stores"]),
        "active_brand_delta": _count_delta(current_metrics["active_brands"], previous_metrics["active_brands"]),
        "transaction_delta": _count_delta(current_metrics["transactions"], previous_metrics["transactions"]),
    }

    activity = {}
    if scope_type == "retailer":
        activity = ds.get_retailer_activity_summary(scope_key, report_id=window["report"]["id"] if window["report"] else None)
    elif scope_type == "brand":
        activity = ds.get_activity_brand_summary(scope_key, limit=8)

    resolved_scope_key = scope_key or "portfolio"
    if scope_type == "brand_retailer":
        resolved_scope_key = f"{ds.analytics_brand_name(scope_key or '')}::{str(retailer_code or '').strip()}"

    snapshot = {
        "scope_type": scope_type,
        "scope_key": resolved_scope_key,
        "period_type": window["period_type"],
        "period_label": window["label"],
        "period_start": window["start_date"],
        "period_end": window["end_date"],
        "report_id": window["report"]["id"] if window["report"] else None,
        "metrics": current_metrics,
        "previous_metrics": previous_metrics,
        "comparisons": comparisons,
        "historical": monthly_history,
        "activity": activity,
    }

    if scope_type == "retailer":
        snapshot["brand_rows"] = _brand_rows_for_retailer(current_frame, previous_frame, limit=14)
        snapshot["top_products"] = _top_products(current_frame, limit=12)
        snapshot["opportunity_brands"] = _opportunity_brands(ds, global_current_frame, current_frame, limit=8)
        top_profile = activity.get("store") or {}
        ds.upsert_retailer_profile(
            scope_key,
            retailer_name=top_profile.get("retailer_name") or scope_key,
            state=top_profile.get("retailer_state"),
            city=top_profile.get("retailer_city"),
            first_seen=monthly_history[0]["period_start"] if monthly_history else None,
            last_seen=window["end_date"],
            profile={
                "latest_period_label": window["label"],
                "latest_revenue": current_metrics["revenue"],
                "active_brands": current_metrics["active_brands"],
            },
        )
        if persist:
            for row in snapshot["brand_rows"]:
                ds.save_retailer_brand_metrics(
                    scope_key,
                    row["brand_name"],
                    snapshot["period_type"],
                    snapshot["period_start"],
                    snapshot["period_end"],
                    row,
                    report_id=snapshot["report_id"],
                )
    elif scope_type == "brand":
        snapshot["retailer_rows"] = _top_retailers_for_brand(current_frame, previous_frame, limit=12)
        try:
            history_rows = list(reversed(ds.get_brand_history(scope_key, limit=18)))
            snapshot["forecast"] = build_brand_forecasts({scope_key: history_rows}).get(ds.analytics_brand_name(scope_key), {})
        except Exception:
            snapshot["forecast"] = {}

    if persist and snapshot["period_start"] and snapshot["period_end"]:
        ds.save_coach_feature_snapshot(
            scope_type=snapshot["scope_type"],
            scope_key=snapshot["scope_key"],
            period_type=snapshot["period_type"],
            period_start=snapshot["period_start"],
            period_end=snapshot["period_end"],
            feature_data=snapshot,
            report_id=snapshot["report_id"],
        )
    return snapshot


def build_retailer_index(ds, report_id: int | None = None, month_value: str | None = None,
                         limit: int | None = None) -> dict[str, Any]:
    df = load_sales_history()
    if df.empty:
        return {"rows": [], "period_label": "", "period_start": None, "period_end": None, "available_months": []}
    window = _report_window(ds, df, report_id=report_id, month_value=month_value)
    current_frame = _between(df, window["start_date"], window["end_date"])
    previous_frame = _between(df, window["previous_start_date"], window["previous_end_date"])
    current_sales = current_frame[current_frame["Vch Type"] == "Sales"].copy()
    previous_sales = previous_frame[previous_frame["Vch Type"] == "Sales"].copy()
    if current_sales.empty:
        return {"rows": [], "period_label": window["label"], "period_start": window["start_date"], "period_end": window["end_date"], "available_months": []}

    current_grouped = current_sales.groupby("Retailer Key").agg(
        total_revenue=("Sales_Value", "sum"),
        total_qty=("Quantity", "sum"),
        transactions=("Vch No.", "nunique"),
        active_brands=("Brand Partner Canonical", "nunique"),
        unique_skus=("SKUs", "nunique"),
        active_days=("Date", lambda s: s.dt.date.nunique()),
    )
    previous_grouped = previous_sales.groupby("Retailer Key").agg(total_revenue=("Sales_Value", "sum"))
    total_portfolio = float(current_grouped["total_revenue"].sum() or 0)
    profile_map = {row["retailer_code"]: row for row in ds.list_retailer_profiles(limit=1000)}
    rows = []
    for retailer_code, row in current_grouped.sort_values("total_revenue", ascending=False).iterrows():
        previous_revenue = float(previous_grouped.loc[retailer_code]["total_revenue"]) if retailer_code in previous_grouped.index else 0.0
        current_store_frame = current_sales[current_sales["Retailer Key"] == retailer_code].copy()
        brand_orders = current_store_frame.groupby("Brand Partner Canonical").size()
        repeat_brands = int((brand_orders > 1).sum()) if len(brand_orders) else 0
        repeat_rate = round((repeat_brands / max(int(row["active_brands"]), 1)) * 100, 2)
        profile = profile_map.get(str(retailer_code), {})
        rows.append({
            "retailer_code": str(retailer_code),
            "retailer_name": profile.get("retailer_name") or str(retailer_code),
            "state": profile.get("state"),
            "city": profile.get("city"),
            "total_revenue": _money(row["total_revenue"]),
            "total_qty": _money(row["total_qty"]),
            "transactions": int(row["transactions"]),
            "active_brands": int(row["active_brands"]),
            "unique_skus": int(row["unique_skus"]),
            "active_days": int(row["active_days"]),
            "repeat_rate": repeat_rate,
            "portfolio_share_pct": round((float(row["total_revenue"]) / total_portfolio) * 100, 2) if total_portfolio else 0.0,
            "revenue_mom": _pct_delta(float(row["total_revenue"]), previous_revenue),
            "previous_revenue": _money(previous_revenue),
        })
    if limit:
        rows = rows[:limit]
    available_months = [
        {"value": period.strftime("%Y-%m"), "label": period.strftime("%b %Y")}
        for period in sorted(df[df["Vch Type"] == "Sales"]["YearMonth"].dropna().unique())
    ]
    return {
        "rows": rows,
        "period_label": window["label"],
        "period_start": window["start_date"],
        "period_end": window["end_date"],
        "report_id": window["report"]["id"] if window["report"] else None,
        "available_months": available_months,
    }


def build_retailer_detail(ds, retailer_code: str, report_id: int | None = None,
                          month_value: str | None = None) -> dict[str, Any]:
    snapshot = build_scope_snapshot(ds, "retailer", scope_key=retailer_code, report_id=report_id, month_value=month_value, persist=True)
    profile = ds.get_retailer_profile(retailer_code) or {}
    return {
        "retailer_code": retailer_code,
        "retailer_name": profile.get("retailer_name") or retailer_code,
        "profile": profile,
        **snapshot,
    }


def build_brand_coach_data(ds, brand_name: str, report_id: int | None = None,
                           month_value: str | None = None) -> dict[str, Any]:
    snapshot = build_scope_snapshot(ds, "brand", scope_key=brand_name, report_id=report_id, month_value=month_value, persist=True)
    retailer_rows = snapshot.get("retailer_rows", [])
    at_risk = [row for row in retailer_rows if (row.get("revenue_mom") or 0) <= -10][:5]
    growth = [row for row in retailer_rows if (row.get("revenue_mom") or 0) >= 10][:5]
    activity = snapshot.get("activity") or {}
    stores_seen = activity.get("stores_seen") or []
    current_retailers = {row.get("retailer_name") for row in retailer_rows}
    activity_mismatches = [
        {
            "retailer_code": row.get("retailer_code"),
            "retailer_name": row.get("retailer_name"),
            "mentions": row.get("mentions"),
        }
        for row in stores_seen
        if row.get("retailer_name") not in current_retailers
    ][:5]
    return {
        "snapshot": snapshot,
        "top_risks": at_risk,
        "top_opportunities": growth,
        "activity_mismatches": activity_mismatches,
    }
