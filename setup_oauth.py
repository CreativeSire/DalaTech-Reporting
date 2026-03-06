"""
setup_oauth.py — One-time Google OAuth2 setup for DALA Report Automation.

Run this ONCE from the project root:
    python setup_oauth.py

What it does:
  1. Reads google_oauth_credentials.json (OAuth2 Desktop client from Google Cloud)
  2. Opens your browser to Google's login page
  3. You log in and click "Allow"
  4. Saves the access + refresh token to google_token.json
  5. All future report runs use this token silently (auto-refreshed)

After running this, restart the Flask server and Sheets will work.
"""

import os
import sys

BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
OAUTH_CREDS_PATH = os.path.join(BASE_DIR, 'google_oauth_credentials.json')
TOKEN_PATH       = os.path.join(BASE_DIR, 'google_token.json')

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]


def main():
    print('=' * 60)
    print('  DALA Report — Google OAuth2 Setup')
    print('=' * 60)

    # ── Check for OAuth client credentials ───────────────────────────────────
    if not os.path.isfile(OAUTH_CREDS_PATH):
        print(f'\n[ERROR] OAuth credentials file not found at:')
        print(f'  {OAUTH_CREDS_PATH}')
        print()
        print('To create it:')
        print('  1. Go to https://console.cloud.google.com')
        print('  2. Select your project (dala-report-automation)')
        print('  3. APIs & Services → Credentials')
        print('  4. + CREATE CREDENTIALS → OAuth 2.0 Client IDs')
        print('  5. Application type: Desktop app')
        print('  6. Name: DALA Report Desktop → Create')
        print('  7. Download JSON → rename to google_oauth_credentials.json')
        print('  8. Place in:', BASE_DIR)
        print('  9. Run this script again.')
        sys.exit(1)

    # ── Run OAuth2 flow ───────────────────────────────────────────────────────
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print('[ERROR] Missing package. Run: pip install google-auth-oauthlib')
        sys.exit(1)

    print('\nOpening browser for Google login...')
    print('(If the browser does not open automatically, check your taskbar)\n')

    flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CREDS_PATH, SCOPES)
    creds = flow.run_local_server(port=0)

    # ── Save token ────────────────────────────────────────────────────────────
    with open(TOKEN_PATH, 'w', encoding='utf-8') as f:
        f.write(creds.to_json())

    print('\n' + '=' * 60)
    print('  Setup complete!')
    print(f'  Token saved to: google_token.json')
    print()
    print('  Next steps:')
    print('  1. Restart the Flask server (python app.py)')
    print('  2. Upload your data file with "Push to Sheets" enabled')
    print('  3. Each brand will get a formatted Google Sheet')
    print('=' * 60)


if __name__ == '__main__':
    main()
