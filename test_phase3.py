"""
test_phase3.py — Phase 3 validation (writes output to test_output.txt)
Run: python test_phase3.py   (from project root)
"""
import sys, os, io, traceback

# Write all output to file AND console
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_output.txt')
log = open(OUT, 'w', encoding='utf-8')

def p(msg=''):
    print(msg)
    log.write(msg + '\n')
    log.flush()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

p('=' * 60)
p('  DALA Phase 3 — Google Sheets Integration Test')
p('=' * 60)

# ── 1. Check credentials ────────────────────────────────────────
try:
    from modules.sheets import sheets_available, push_brand_to_sheets, _get_client
    creds_ok = sheets_available()
    p(f'\n[1] Credentials file present: {"YES" if creds_ok else "NO — place google_credentials.json in project root"}')
    if not creds_ok:
        log.close(); sys.exit(1)
except Exception as e:
    p(f'[1] Import error: {e}')
    traceback.print_exc(file=log)
    log.close(); sys.exit(1)

# ── 2. Authenticate ─────────────────────────────────────────────
try:
    client = _get_client()
    p(f'[2] Authentication:           OK — service account connected')
except Exception as e:
    p(f'[2] Authentication FAILED:    {e}')
    traceback.print_exc(file=log)
    log.close(); sys.exit(1)

# ── 3. Load sample data ─────────────────────────────────────────
try:
    from modules.ingestion import load_and_clean, filter_by_date, split_by_brand
    XLS = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       'extracted', 'February Monthly Report',
                       'Raw_Files_From_Tally', 'febSalesReportData.xls')
    p(f'[3] Data file exists:         {"YES" if os.path.isfile(XLS) else "NO — " + XLS}')
    if not os.path.isfile(XLS):
        p('    Cannot run live push test without data. Credentials look good!')
        log.close(); sys.exit(0)
    df_all    = load_and_clean(XLS)
    df_ranged = filter_by_date(df_all, '2026-02-01', '2026-02-28')
    brands    = split_by_brand(df_ranged)
    p(f'    Brands loaded:             {len(brands)}')
    TEST_BRAND = next(iter(brands))
except Exception as e:
    p(f'[3] Data load FAILED:         {e}')
    traceback.print_exc(file=log)
    log.close(); sys.exit(1)

# ── 4. Push ONE brand to Sheets ─────────────────────────────────
p(f'\n[4] Pushing "{TEST_BRAND}" to Google Sheets ...')
try:
    url = push_brand_to_sheets(
        brand_name  = TEST_BRAND,
        brand_df    = brands[TEST_BRAND],
        start_date  = '2026-02-01',
        end_date    = '2026-02-28',
    )
    p(f'    Sheet created:  OK')
    p(f'    URL: {url}')
except Exception as e:
    p(f'    Push FAILED: {e}')
    traceback.print_exc(file=log)
    log.close(); sys.exit(1)

p('\n' + '=' * 60)
p('  Phase 3 smoke-test PASSED — full pipeline ready!')
p('=' * 60)
log.close()
