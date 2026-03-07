"""
seed_db_from_combined.py
------------------------
Rebuilds dala_data.db from scratch using the combined 2024to2026salesreport.xlsx file.
Each calendar month is imported as a separate report.

Usage:
    python seed_db_from_combined.py

The script clears ALL existing data in dala_data.db and re-imports every month
found in the combined xlsx. Run this locally, then commit the updated dala_data.db.
"""

import os
import sys
import calendar
import sqlite3

import pandas as pd

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
XLSX_PATH = os.path.join(BASE_DIR, '2024to2026salesreport.xlsx')

sys.path.insert(0, BASE_DIR)


def wipe_database(db_path):
    """Delete all report data so we start clean."""
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM daily_sales")
    conn.execute("DELETE FROM brand_kpis")
    conn.execute("DELETE FROM alerts")
    conn.execute("DELETE FROM reports")
    try:
        conn.execute("DELETE FROM ai_narratives")
    except Exception:
        pass
    conn.execute("DELETE FROM activity_log")
    conn.commit()
    conn.close()
    print("  [wipe] Cleared existing data from database.")


def import_month(df_all, year, month, ds):
    from modules.ingestion import filter_by_date, split_by_brand
    from modules.kpi import calculate_kpis, calculate_perf_score
    from modules.alerts import check_and_save_alerts, run_portfolio_alerts

    start_date = f"{year}-{month:02d}-01"
    last_day   = calendar.monthrange(year, month)[1]
    end_date   = f"{year}-{month:02d}-{last_day}"

    df_filtered = filter_by_date(df_all, start_date, end_date)
    if df_filtered.empty:
        return False, 0, 0

    brand_data = split_by_brand(df_filtered)
    brands     = list(brand_data.keys())
    if not brands:
        return False, 0, 0

    all_kpis  = {b: calculate_kpis(brand_data[b]) for b in brands}
    total_rev = sum(k['total_revenue'] for k in all_kpis.values())
    avg_rev   = total_rev / max(len(brands), 1)
    for b in brands:
        all_kpis[b]['perf_score'] = calculate_perf_score(all_kpis[b], avg_rev)

    all_stores: set = set()
    for k in all_kpis.values():
        if k.get('top_stores') is not None and not k['top_stores'].empty:
            all_stores.update(k['top_stores']['Store'].tolist())

    total_qty = sum(k['total_qty'] for k in all_kpis.values())
    existing  = ds.get_report_by_date_range(start_date, end_date)
    if existing:
        report_id = existing['id']
        ds.clear_report_data(report_id)
        ds.update_report(report_id, xls_filename='2024to2026salesreport.xlsx',
                         total_revenue=total_rev, total_qty=total_qty,
                         total_stores=len(all_stores), brand_count=len(brands))
    else:
        report_id = ds.save_report(
            start_date=start_date, end_date=end_date,
            xls_filename='2024to2026salesreport.xlsx',
            total_revenue=total_rev, total_qty=total_qty,
            total_stores=len(all_stores), brand_count=len(brands),
        )

    for b in brands:
        k = all_kpis[b]
        share = round(k['total_revenue'] / max(total_rev, 1) * 100, 2)
        ds.save_brand_kpis(report_id, b, k, k.get('perf_score', {}), share)
        if not k['daily_sales'].empty:
            ds.save_daily_sales(report_id, b, k['daily_sales'])
        history = ds.get_brand_history(b, limit=3)
        check_and_save_alerts(report_id, b, k, avg_rev, history[1:], ds)

    run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)
    ds.log_activity('seed_import', f'{year}-{month:02d} from combined xlsx', report_id=report_id)

    return True, len(brands), total_rev


def main():
    from modules.data_store import DataStore
    from modules.ingestion import load_and_clean

    if not os.path.isfile(XLSX_PATH):
        print(f"ERROR: file not found: {XLSX_PATH}")
        sys.exit(1)

    print(f"Loading {XLSX_PATH} ...")
    df = load_and_clean(XLSX_PATH)
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['Date'])

    min_dt = df['Date'].min()
    max_dt = df['Date'].max()
    print(f"  Date range in file: {min_dt.date()} to {max_dt.date()}")
    print(f"  Total rows (all Vch Types): {len(df)}")

    # Determine months to import
    months = []
    yr, mo = min_dt.year, min_dt.month
    while (yr, mo) <= (max_dt.year, max_dt.month):
        months.append((yr, mo))
        mo += 1
        if mo > 12:
            mo = 1
            yr += 1

    print(f"  {len(months)} months to import: "
          f"{months[0][0]}-{months[0][1]:02d} to {months[-1][0]}-{months[-1][1]:02d}")

    ds = DataStore()

    # Wipe and re-seed
    print("\nWiping existing database data...")
    wipe_database(ds.db_path)

    ok = fail = 0
    print("\nImporting months:")
    for year, month in months:
        label = f"{year}-{month:02d}"
        success, n_brands, rev = import_month(df, year, month, ds)
        if success:
            print(f"  {label}  OK  brands={n_brands}  rev=N{rev:>15,.0f}")
            ok += 1
        else:
            print(f"  {label}  SKIP (no data in range)")
            fail += 1

    print(f"\n=== Done: {ok} months imported, {fail} skipped ===")

    # Summary
    conn = sqlite3.connect(ds.db_path)
    rows = conn.execute(
        "SELECT month_label, brand_count, total_revenue FROM reports ORDER BY start_date"
    ).fetchall()
    conn.close()
    print("\nFinal database contents:")
    total_r = 0
    for r in rows:
        print(f"  {r[0]:15s}  brands:{r[1]:3d}  rev:N{r[2]:>15,.0f}")
        total_r += r[2]
    print(f"\n  TOTAL REVENUE across all periods: N{total_r:,.0f}")
    print(f"  DB size: {os.path.getsize(ds.db_path)/1024:.1f} KB")


if __name__ == '__main__':
    main()
