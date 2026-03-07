"""
import_2025_local.py
--------------------
Imports all 2025 per-brand Excel files from the locally-extracted folder
into the DALA Analytics database, grouped by month.

Usage:
    python import_2025_local.py

Point EXTRACT_ROOT at the folder produced by unzipping the 2025 archive.
"""

import os
import sys
import calendar

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
EXTRACT_ROOT = os.path.join(BASE_DIR, 'test', 'extracted_2025',
                            '2025 Dala BP Sales Report')

sys.path.insert(0, BASE_DIR)

# ── Folders to skip (not real month data) ────────────────────────────────────
SKIP_FOLDERS = {
    'audit',
    'brand partners sales report template',
    'partners analysis folder',
}

MONTH_NAMES = {
    'january': 1, 'february': 2, 'march': 3,    'april': 4,
    'may': 5,     'june': 6,     'july': 7,      'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
}


def extract_month(folder_name: str):
    n = folder_name.lower()
    for mname, mnum in MONTH_NAMES.items():
        if mname in n:
            return mnum
    return None


def extract_brand(filename: str) -> str:
    import re
    name = os.path.splitext(filename)[0]
    name = re.sub(r'\s+Sales\s+Report.*$', '', name, flags=re.IGNORECASE)
    months_pat = (
        r'\b(January|February|March|April|May|June|July|'
        r'August|September|October|November|December)\b.*$'
    )
    name = re.sub(months_pat, '', name, flags=re.IGNORECASE)
    return name.strip()


def collect_month_groups():
    """Walk EXTRACT_ROOT and group xlsx files by their month subfolder."""
    groups = {}
    for entry in sorted(os.listdir(EXTRACT_ROOT)):
        if entry.lower() in SKIP_FOLDERS:
            continue
        folder_path = os.path.join(EXTRACT_ROOT, entry)
        if not os.path.isdir(folder_path):
            continue
        month_num = extract_month(entry)
        if month_num is None:
            print(f'  [skip] {entry}  (no month detected)')
            continue
        # Collect all xlsx files recursively
        xlsx_files = []
        for root, dirs, files in os.walk(folder_path):
            for f in files:
                if f.lower().endswith('.xlsx') or f.lower().endswith('.xls'):
                    xlsx_files.append(os.path.join(root, f))
        if xlsx_files:
            groups[entry] = {'month': month_num, 'year': 2025, 'files': xlsx_files}
    return groups


def import_month_group(label, group, ds):
    from modules.ingestion import load_and_clean, load_brand_file, filter_by_date, split_by_brand
    from modules.kpi import calculate_kpis, calculate_perf_score
    from modules.alerts import check_and_save_alerts, run_portfolio_alerts

    year  = group['year']
    month = group['month']
    start_date = f"{year}-{month:02d}-01"
    last_day   = calendar.monthrange(year, month)[1]
    end_date   = f"{year}-{month:02d}-{last_day}"

    print(f'\n  [{label}]  {start_date} -> {end_date}  ({len(group["files"])} files)')

    combined_dfs, brand_dfs, errors = [], [], []
    for fpath in group['files']:
        fname = os.path.basename(fpath)
        try:
            # Try standard combined format first
            try:
                df = load_and_clean(fpath)
                combined_dfs.append(df)
                continue
            except (ValueError, Exception):
                pass
            # Per-brand format
            brand_name = extract_brand(fname) or fname
            df = load_brand_file(fpath, brand_name)
            if not df.empty:
                brand_dfs.append(df)
        except Exception as e:
            errors.append(f'{fname}: {e}')

    dfs = combined_dfs if combined_dfs else brand_dfs
    if not dfs:
        print(f'    ERROR: no data loaded. Sample errors: {errors[:2]}')
        return False

    combined = pd.concat(dfs, ignore_index=True)

    # Run pipeline
    df_filtered = filter_by_date(combined, start_date, end_date)
    if df_filtered.empty:
        print(f'    SKIP: no rows in date range {start_date}->{end_date}')
        return False

    brand_data = split_by_brand(df_filtered)
    brands     = list(brand_data.keys())
    if not brands:
        print(f'    SKIP: no brands after split')
        return False

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
        ds.update_report(report_id, xls_filename=label,
                         total_revenue=total_rev, total_qty=total_qty,
                         total_stores=len(all_stores), brand_count=len(brands))
    else:
        report_id = ds.save_report(
            start_date=start_date, end_date=end_date, xls_filename=label,
            total_revenue=total_rev, total_qty=total_qty,
            total_stores=len(all_stores), brand_count=len(brands),
        )

    for b in brands:
        k = all_kpis[b]
        share = round(k['total_revenue'] / max(total_rev, 1) * 100, 2)
        ds.save_brand_kpis(report_id, b, k, k.get('perf_score', {}), share)
        ds.save_brand_detail_json(report_id, b, k)
        if not k['daily_sales'].empty:
            ds.save_daily_sales(report_id, b, k['daily_sales'])
        history = ds.get_brand_history(b, limit=3)
        check_and_save_alerts(report_id, b, k, avg_rev, history[1:], ds)

    run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)

    print(f'    OK  report_id={report_id}  brands={len(brands)}  rows={len(df_filtered)}  rev={total_rev:,.0f}')
    return True


def main():
    from modules.data_store import DataStore
    ds = DataStore()

    if not os.path.isdir(EXTRACT_ROOT):
        print(f'ERROR: folder not found: {EXTRACT_ROOT}')
        sys.exit(1)

    groups = collect_month_groups()
    print(f'Found {len(groups)} month groups in 2025 data:')
    for label, g in groups.items():
        print(f'  {label}: {len(g["files"])} files  ->  month {g["month"]}')

    ok, fail = 0, 0
    for label, group in groups.items():
        success = import_month_group(label, group, ds)
        if success:
            ok += 1
        else:
            fail += 1

    print(f'\n=== Done: {ok} months imported, {fail} skipped/errored ===')


if __name__ == '__main__':
    main()
