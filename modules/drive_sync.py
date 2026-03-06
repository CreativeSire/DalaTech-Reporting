"""
drive_sync.py - Google Drive Integration for DALA Analytics

Automatically watches folders, detects new/modified Excel files,
and runs the full generation pipeline (KPIs, alerts, DB save).

Auth hierarchy (same as sheets.py):
  1. google_token.json  — OAuth2 user token (preferred)
  2. google_credentials.json — Service account (fallback)

Folders watched:
  2025: 1I6b9ytn6XR0QHtr9tBRzXXe1xhMKW7dD
  2026: 1dLbLm-O66ySffXUHlmAsHNIiayKHkEol
"""

import os
import io
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

# Google API
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2 import service_account

import pandas as pd

# DALA modules
from .data_store import DataStore
from .ingestion import load_and_clean, filter_by_date, split_by_brand

# ── Configuration ─────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

DRIVE_FOLDERS = [
    {
        'name': '2025 Sales Reports',
        'id': '1I6b9ytn6XR0QHtr9tBRzXXe1xhMKW7dD',
        'year': 2025,
    },
    {
        'name': '2026 Sales Reports',
        'id': '1dLbLm-O66ySffXUHlmAsHNIiayKHkEol',
        'year': 2026,
    },
]

# ── Auth helper ───────────────────────────────────────────────────────────────

def _get_drive_service():
    """
    Build an authenticated Google Drive API service.
    Tries OAuth2 (google_token.json) first, then service account.
    Raises RuntimeError if no credentials found.
    """
    token_path  = os.path.join(BASE_DIR, 'google_token.json')
    
    # Support GOOGLE_APPLICATION_CREDENTIALS env var (Railway)
    sa_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', os.path.join(BASE_DIR, 'google_credentials.json'))

    # ── 1. OAuth2 user token ─────────────────────────────────────────────────
    if os.path.isfile(token_path):
        from google.oauth2.credentials import Credentials as OAuthCreds
        with open(token_path, encoding='utf-8') as f:
            token_data = json.load(f)
        creds = OAuthCreds.from_authorized_user_info(token_data, SCOPES)
        if not creds.valid:
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                with open(token_path, 'w', encoding='utf-8') as f:
                    f.write(creds.to_json())
        if creds.valid:
            return build('drive', 'v3', credentials=creds, cache_discovery=False)

    # ── 2. Service account fallback ──────────────────────────────────────────
    # Also support GOOGLE_CREDENTIALS_JSON env var for Railway
    if not os.path.isfile(sa_path):
        creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')
        if creds_json:
            # Ensure directory exists
            os.makedirs(os.path.dirname(sa_path), exist_ok=True)
            with open(sa_path, 'w', encoding='utf-8') as f:
                f.write(creds_json)
            print(f"[DriveSync] Credentials written to {sa_path}")

    if os.path.isfile(sa_path):
        creds = service_account.Credentials.from_service_account_file(sa_path, scopes=SCOPES)
        return build('drive', 'v3', credentials=creds, cache_discovery=False)

    raise RuntimeError(
        "No Google credentials found for Drive access.\n"
        "Run `python setup_oauth.py` to set up OAuth2, or place a service account "
        "JSON at google_credentials.json in the project root."
    )


def drive_available() -> bool:
    """Returns True if Drive credentials are present (doesn't test connection)."""
    token_path = os.path.join(BASE_DIR, 'google_token.json')
    sa_path    = os.path.join(BASE_DIR, 'google_credentials.json')
    return os.path.isfile(token_path) or os.path.isfile(sa_path) or \
           bool(os.environ.get('GOOGLE_CREDENTIALS_JSON'))


# ── Google Drive API wrapper ──────────────────────────────────────────────────

class DriveService:
    """Thin wrapper around the Google Drive v3 API."""

    def __init__(self):
        self.service = _get_drive_service()

    def list_excel_files(self, folder_id: str) -> List[Dict]:
        """List all Excel files (.xls / .xlsx) in a folder."""
        # Match both old .xls and new .xlsx MIME types
        q = (
            f"'{folder_id}' in parents and trashed=false and ("
            "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
            "mimeType='application/vnd.ms-excel'"
            ")"
        )
        files, page_token = [], None
        while True:
            resp = self.service.files().list(
                q=q,
                spaces='drive',
                fields='nextPageToken, files(id, name, modifiedTime, size)',
                pageToken=page_token,
                orderBy='name',
            ).execute()
            files.extend(resp.get('files', []))
            page_token = resp.get('nextPageToken')
            if not page_token:
                break
        return files

    def download_file(self, file_id: str) -> io.BytesIO:
        """Download a Drive file to an in-memory buffer."""
        request = self.service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buf.seek(0)
        return buf


# ── Sync State (SQLite-backed via DataStore) ───────────────────────────────────

class SyncState:
    """
    Tracks which Drive files have been imported.
    State is persisted in the DataStore SQLite DB (drive_sync_files table),
    with a JSON file fallback for legacy compatibility.
    """

    def __init__(self, ds: DataStore = None):
        self.ds = ds or DataStore()
        self._ensure_table()
        # Also keep in-memory cache for the current session
        self._cache: Dict[str, Dict] = self._load_from_db()

    def _ensure_table(self):
        with self.ds._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS drive_sync_files (
                    file_id      TEXT PRIMARY KEY,
                    folder_id    TEXT,
                    file_name    TEXT NOT NULL,
                    modified_time TEXT NOT NULL,
                    status       TEXT DEFAULT 'pending',
                    report_id    INTEGER,
                    error_msg    TEXT,
                    imported_at  TEXT,
                    updated_at   TEXT NOT NULL
                )
            """)

    def _load_from_db(self) -> Dict[str, Dict]:
        with self.ds._connect() as conn:
            rows = conn.execute("SELECT * FROM drive_sync_files").fetchall()
        return {r['file_id']: dict(r) for r in rows}

    def is_imported(self, file_id: str, modified_time: str) -> bool:
        row = self._cache.get(file_id)
        if not row:
            return False
        return row['modified_time'] == modified_time and row['status'] == 'imported'

    def mark_imported(self, file_id: str, folder_id: str, file_name: str,
                      modified_time: str, report_id: int = None):
        now = datetime.now().isoformat(timespec='seconds')
        with self.ds._connect() as conn:
            conn.execute("""
                INSERT INTO drive_sync_files
                    (file_id, folder_id, file_name, modified_time, status, report_id, imported_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(file_id) DO UPDATE SET
                    modified_time=excluded.modified_time,
                    status='imported', report_id=excluded.report_id,
                    imported_at=excluded.imported_at, updated_at=excluded.updated_at
            """, (file_id, folder_id, file_name, modified_time, 'imported', report_id, now, now))
        self._cache[file_id] = {
            'file_id': file_id, 'folder_id': folder_id, 'file_name': file_name,
            'modified_time': modified_time, 'status': 'imported',
            'report_id': report_id, 'imported_at': now, 'updated_at': now,
        }

    def mark_error(self, file_id: str, folder_id: str, file_name: str,
                   modified_time: str, error_msg: str):
        now = datetime.now().isoformat(timespec='seconds')
        with self.ds._connect() as conn:
            conn.execute("""
                INSERT INTO drive_sync_files
                    (file_id, folder_id, file_name, modified_time, status, error_msg, updated_at)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(file_id) DO UPDATE SET
                    status='error', error_msg=excluded.error_msg, updated_at=excluded.updated_at
            """, (file_id, folder_id, file_name, modified_time, 'error', error_msg, now))
        self._cache[file_id] = {
            'file_id': file_id, 'folder_id': folder_id, 'file_name': file_name,
            'modified_time': modified_time, 'status': 'error',
            'error_msg': error_msg, 'updated_at': now,
        }

    def get_all_files(self) -> List[Dict]:
        with self.ds._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM drive_sync_files ORDER BY updated_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_stats(self) -> Dict:
        with self.ds._connect() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*) as total,
                    COALESCE(SUM(CASE WHEN status='imported' THEN 1 ELSE 0 END), 0) as imported,
                    COALESCE(SUM(CASE WHEN status='error'    THEN 1 ELSE 0 END), 0) as errors,
                    MAX(imported_at) as last_import
                FROM drive_sync_files
            """).fetchone()
        return dict(row) if row else {'total': 0, 'imported': 0, 'errors': 0, 'last_import': None}


# ── Smart Date Extractor ──────────────────────────────────────────────────────

class DateExtractor:
    """Extracts date ranges from filenames or Excel content."""

    MONTH_MAP = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }

    @classmethod
    def from_filename(cls, filename: str, default_year: int = 2026) -> Tuple[str, str]:
        import re
        fn = filename.lower()
        for abbr, num in cls.MONTH_MAP.items():
            if abbr in fn:
                yr_m = re.search(r'20(\d{2})', fn)
                year = 2000 + int(yr_m.group(1)) if yr_m else default_year
                start = datetime(year, num, 1)
                end = (datetime(year + 1, 1, 1) if num == 12
                       else datetime(year, num + 1, 1)) - timedelta(days=1)
                return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
        # Default: folder year + current month
        today = datetime.now()
        start = datetime(default_year, today.month, 1)
        end = (datetime(default_year + 1, 1, 1) if today.month == 12
               else datetime(default_year, today.month + 1, 1)) - timedelta(days=1)
        return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')

    @classmethod
    def from_excel_content(cls, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
        if 'Date' in df.columns:
            dates = pd.to_datetime(df['Date'], errors='coerce').dropna()
            if len(dates) > 0:
                return dates.min().strftime('%Y-%m-%d'), dates.max().strftime('%Y-%m-%d')
        return None, None


# ── Full Generation Pipeline (same as app.py _run_generation) ────────────────

def _run_pipeline(file_buffer: io.BytesIO, filename: str,
                  start_date: str, end_date: str, ds: DataStore) -> Dict:
    """
    Run the full DALA generation pipeline on a file buffer.
    Returns a summary dict with keys: report_id, brands, rows, start_date, end_date.
    Raises on hard failure.
    """
    from .ingestion import load_and_clean, filter_by_date, split_by_brand
    from .kpi import calculate_kpis, calculate_perf_score
    from .alerts import check_and_save_alerts, run_portfolio_alerts

    # 1. Load and filter
    df = load_and_clean(file_buffer)
    df_filtered = filter_by_date(df, start_date, end_date)
    if df_filtered.empty:
        raise ValueError(f"No data in date range {start_date} to {end_date}")

    brand_data = split_by_brand(df_filtered)
    brands = list(brand_data.keys())
    if not brands:
        raise ValueError("No brand data found after split")

    # 2. KPIs + perf scores
    all_kpis = {b: calculate_kpis(brand_data[b]) for b in brands}
    total_rev = sum(k['total_revenue'] for k in all_kpis.values())
    avg_rev   = total_rev / max(len(brands), 1)
    for b in brands:
        all_kpis[b]['perf_score'] = calculate_perf_score(all_kpis[b], avg_rev)

    # 3. Upsert report row
    all_stores: set = set()
    for k in all_kpis.values():
        if k.get('top_stores') is not None and not k['top_stores'].empty:
            all_stores.update(k['top_stores']['Store'].tolist())

    total_qty = sum(k['total_qty'] for k in all_kpis.values())
    existing  = ds.get_report_by_date_range(start_date, end_date)
    if existing:
        report_id = existing['id']
        ds.clear_report_data(report_id)
        ds.update_report(report_id, xls_filename=filename,
                         total_revenue=total_rev, total_qty=total_qty,
                         total_stores=len(all_stores), brand_count=len(brands))
    else:
        report_id = ds.save_report(
            start_date=start_date, end_date=end_date,
            xls_filename=filename,
            total_revenue=total_rev, total_qty=total_qty,
            total_stores=len(all_stores), brand_count=len(brands),
        )

    # 4. Save brand KPIs + daily sales + alerts
    for b in brands:
        k = all_kpis[b]
        share = round(k['total_revenue'] / max(total_rev, 1) * 100, 2)
        ds.save_brand_kpis(report_id, b, k, k.get('perf_score', {}), share)
        if not k['daily_sales'].empty:
            ds.save_daily_sales(report_id, b, k['daily_sales'])
        history = ds.get_brand_history(b, limit=3)
        check_and_save_alerts(report_id, b, k, avg_rev, history[1:], ds)

    run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)

    return {
        'report_id':  report_id,
        'brands':     len(brands),
        'rows':       len(df_filtered),
        'start_date': start_date,
        'end_date':   end_date,
    }


# ── Main Sync Orchestrator ────────────────────────────────────────────────────

class DriveSyncOrchestrator:
    """Orchestrates Drive listing, downloading, and pipeline execution."""

    def __init__(self):
        self.ds    = DataStore()
        self.state = SyncState(self.ds)
        self.drive = DriveService()

    # ── Public API ────────────────────────────────────────────────────────────

    def check_new_files(self) -> List[Dict]:
        """
        Check all folders for NEW or CHANGED files (since last import).
        Runs the full pipeline for each new file.
        """
        results = []
        for folder in DRIVE_FOLDERS:
            try:
                files = self.drive.list_excel_files(folder['id'])
                for f in files:
                    if not self.state.is_imported(f['id'], f['modifiedTime']):
                        result = self._import_file(f, folder['id'], folder['year'])
                        results.append(result)
            except Exception as e:
                results.append({'folder': folder['name'], 'status': 'error', 'error': str(e)})
        return results

    def full_historical_sync(self, progress_cb=None) -> List[Dict]:
        """
        Import ALL files from all folders regardless of sync state.
        Used for the initial "build the database" operation.
        progress_cb: optional callable(current, total, file_name) for progress updates.
        """
        # Collect all files first
        all_files: List[Tuple[Dict, str, int]] = []  # (file, folder_id, year)
        for folder in DRIVE_FOLDERS:
            try:
                files = self.drive.list_excel_files(folder['id'])
                for f in files:
                    all_files.append((f, folder['id'], folder['year']))
            except Exception as e:
                print(f"Could not list {folder['name']}: {e}")

        total = len(all_files)
        results = []
        for i, (f, folder_id, year) in enumerate(all_files):
            if progress_cb:
                progress_cb(i, total, f['name'])
            result = self._import_file(f, folder_id, year)
            results.append(result)

        if progress_cb:
            progress_cb(total, total, 'Done')
        return results

    def list_all_files(self) -> List[Dict]:
        """
        List all files from all Drive folders (with sync status).
        Used by the dashboard without importing anything.
        """
        synced = {r['file_id']: r for r in self.state.get_all_files()}
        all_files = []
        for folder in DRIVE_FOLDERS:
            try:
                files = self.drive.list_excel_files(folder['id'])
                for f in files:
                    state = synced.get(f['id'], {})
                    all_files.append({
                        'id':           f['id'],
                        'name':         f['name'],
                        'folder':       folder['name'],
                        'folder_id':    folder['id'],
                        'modifiedTime': f.get('modifiedTime', ''),
                        'size':         f.get('size', 0),
                        'status':       state.get('status', 'pending'),
                        'imported_at':  state.get('imported_at', ''),
                        'report_id':    state.get('report_id'),
                        'list_error':   None,
                    })
            except Exception as e:
                err_msg = str(e)
                print(f"Could not list {folder['name']}: {err_msg}")
                # Surface error as a sentinel entry so the dashboard can show it
                all_files.append({
                    'id':           None,
                    'name':         None,
                    'folder':       folder['name'],
                    'folder_id':    folder['id'],
                    'list_error':   err_msg,
                })
        return sorted([f for f in all_files if f['name']], key=lambda x: x['name'])

    def get_sync_summary(self) -> Dict:
        stats = self.state.get_stats()
        return {
            'total_files_tracked': stats['total'],
            'total_imports':       stats['imported'],
            'total_errors':        stats['errors'],
            'last_import':         stats['last_import'],
            'folders':             [{'name': f['name'], 'folder_id': f['id']} for f in DRIVE_FOLDERS],
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    def _import_file(self, file: Dict, folder_id: str, default_year: int) -> Dict:
        """Download and run the full generation pipeline for one Drive file."""
        file_id   = file['id']
        file_name = file['name']
        print(f"  Importing: {file_name}")

        try:
            # Download
            buf = self.drive.download_file(file_id)

            # Detect date range from content first, then filename
            df_preview = load_and_clean(io.BytesIO(buf.read()))
            buf.seek(0)
            start_date, end_date = DateExtractor.from_excel_content(df_preview)
            if not start_date:
                start_date, end_date = DateExtractor.from_filename(file_name, default_year)

            # Run full pipeline
            summary = _run_pipeline(buf, file_name, start_date, end_date, self.ds)

            self.state.mark_imported(
                file_id, folder_id, file_name,
                file.get('modifiedTime', ''), summary['report_id']
            )
            return {
                'file':       file_name,
                'status':     'success',
                'date_range': f"{start_date} to {end_date}",
                'brands':     summary['brands'],
                'rows':       summary['rows'],
                'report_id':  summary['report_id'],
            }

        except Exception as e:
            error_msg = str(e)
            print(f"  Error importing {file_name}: {error_msg}")
            self.state.mark_error(
                file_id, folder_id, file_name,
                file.get('modifiedTime', ''), error_msg
            )
            return {'file': file_name, 'status': 'error', 'error': error_msg}


if __name__ == '__main__':
    orch = DriveSyncOrchestrator()
    print("Starting full historical sync...")
    results = orch.full_historical_sync()
    ok  = sum(1 for r in results if r.get('status') == 'success')
    err = sum(1 for r in results if r.get('status') == 'error')
    print(f"Done. Imported: {ok}  Errors: {err}")
