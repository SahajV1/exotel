import os
import requests
import csv
import io
import time
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote
import gspread
from google.oauth2.service_account import Credentials

# ==============================
# 🔐 LOAD SECRETS
# ==============================

EXOTEL_COOKIES = os.getenv("EXOTEL_COOKIES")
EXOTEL_ACCOUNT_SID = os.getenv("EXOTEL_ACCOUNT_SID")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

# ==============================
# 🍪 PARSE COOKIES
# ==============================

def parse_cookies(cookie_string):
    cookies = {}
    for part in cookie_string.split(';'):
        part = part.strip()
        if '=' in part:
            key, _, val = part.partition('=')
            cookies[key.strip()] = val.strip()
    return cookies

# ==============================
# 📅 GET YESTERDAY
# ==============================

def get_yesterday():
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')
    start = f"{date_str} 00:00:00"
    end = f"{date_str} 23:59:59"
    return start, end, date_str

# ==============================
# 📥 DOWNLOAD REPORT
# ==============================

def download_exotel_report(start, end):
    api_url = (
        f"https://my.exotel.com/accounts/{EXOTEL_ACCOUNT_SID}/reports/custom-download"
        f"?reportType=call"
        f"&startDate={quote(start)}"
        f"&endDate={quote(end)}"
        f"&VN=all"
        f"&agentreporttype="
        f"&agentgroup="
        f"&agentuser="
        f"&selectedToday=false"
        f"&_={int(time.time() * 1000)}"
    )

    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': f'https://my.exotel.com/accounts/{EXOTEL_ACCOUNT_SID}/reports',
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
    }

    cookies = parse_cookies(EXOTEL_COOKIES)

    print(f'📡 Fetching: {start}')
    r1 = requests.get(api_url, headers=headers, cookies=cookies, timeout=60)

    if r1.status_code != 200:
        raise Exception(f'❌ Step 1 failed')

    data = r1.json()

    s3_url = data.get('report', {}).get('url', '')
    if not s3_url:
        raise Exception(f'❌ No S3 URL')

    print('✅ Got S3 URL')

    r2 = requests.get(s3_url, timeout=60)

    if r2.status_code != 200:
        raise Exception(f'❌ Step 2 failed')

    content = r2.content.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    print(f'✅ {len(rows)} records')

    return pd.DataFrame(rows)

# ==============================
# 📊 UPLOAD (APPEND)
# ==============================

def upload_to_sheets(df):
    with open("credentials.json", "w") as f:
        f.write(GOOGLE_CREDENTIALS)

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    sheet = client.open("Exotel Dashboard").sheet1

    data = [df.columns.values.tolist()] + df.values.tolist()

    existing = sheet.get_all_values()

    if len(existing) == 0:
        # First time
        sheet.update("A1", data)
    else:
        # Append
        next_row = len(existing) + 1
        sheet.update(f"A{next_row}", data[1:])  # skip header

    print("✅ Data appended")

# ==============================
# 🚀 MAIN
# ==============================

if __name__ == "__main__":
    # 🔒 FIXED DATE: 26 March 2026
    start = "2026-03-26 00:00:00"
    end = "2026-03-26 23:59:59"

    df = download_exotel_report(start, end)

    upload_to_sheets(df)
