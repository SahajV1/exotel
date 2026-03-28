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
# 📅 FIXED DATE (26 MARCH)
# ==============================

def get_fixed_date():
    start = "2026-03-26 00:00:00"
    end = "2026-03-26 23:59:59"
    return start, end

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
        raise Exception('❌ Step 1 failed')

    data = r1.json()
    s3_url = data.get('report', {}).get('url', '')

    if not s3_url:
        raise Exception('❌ No S3 URL')

    print('✅ Got S3 URL')

    r2 = requests.get(s3_url, timeout=60)

    if r2.status_code != 200:
        raise Exception('❌ Step 2 failed')

    content = r2.content.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    print(f'✅ {len(rows)} records')

    return pd.DataFrame(rows)

# ==============================
# 🧹 CLEAN OLD DATA (30 DAYS)
# ==============================

def keep_last_30_days(sheet, df_new):
    existing = sheet.get_all_values()

    if len(existing) == 0:
        return df_new

    df_old = pd.DataFrame(existing[1:], columns=existing[0])

    # 🔴 CHANGE THIS COLUMN NAME IF NEEDED
    date_col = "Start Time"

    df_old[date_col] = pd.to_datetime(df_old[date_col], errors='coerce')
    df_new[date_col] = pd.to_datetime(df_new[date_col], errors='coerce')

    combined = pd.concat([df_old, df_new])

    cutoff = datetime.now() - timedelta(days=30)

    filtered = combined[combined[date_col] >= cutoff]

    return filtered

# ==============================
# 📊 UPLOAD CLEAN DATA
# ==============================

def upload_to_sheets(df_new):
    with open("credentials.json", "w") as f:
        f.write(GOOGLE_CREDENTIALS)

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    sheet = client.open("Exotel Dashboard").sheet1

    # 🧹 Keep only last 30 days
    df_final = keep_last_30_days(sheet, df_new)

    data = [df_final.columns.values.tolist()] + df_final.values.tolist()

    # 🔥 Resize sheet
    sheet.clear()

    required_rows = len(data)
    current_rows = sheet.row_count

    if required_rows > current_rows:
        sheet.add_rows(required_rows - current_rows)

    # ✅ Correct update format
    sheet.update(values=data, range_name="A1")

    print("✅ Sheet updated (last 30 days only)")

# ==============================
# 🚀 MAIN
# ==============================

if __name__ == "__main__":
    start, end = get_fixed_date()

    df = download_exotel_report(start, end)

    upload_to_sheets(df)
