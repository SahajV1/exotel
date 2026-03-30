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
# 📅 YESTERDAY DATE
# ==============================

def get_yesterday_date():
    yesterday = datetime.now() - timedelta(days=1)

    start = yesterday.strftime("%Y-%m-%d 00:00:00")
    end = yesterday.strftime("%Y-%m-%d 23:59:59")

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
        raise Exception(f'❌ Step 1 failed: {r1.text}')

    data = r1.json()
    s3_url = data.get('report', {}).get('url', '')

    if not s3_url:
        raise Exception('❌ No S3 URL received')

    print('✅ Got S3 URL')

    r2 = requests.get(s3_url, timeout=60)

    if r2.status_code != 200:
        raise Exception('❌ Step 2 failed')

    content = r2.content.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    print(f'✅ {len(rows)} records fetched')

    return pd.DataFrame(rows)

# ==============================
# 📊 UPLOAD TO GOOGLE SHEETS
# ==============================

def upload_to_sheets(df_new):
    # 🔑 Write credentials
    with open("credentials.json", "w") as f:
        f.write(GOOGLE_CREDENTIALS)

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    sheet = client.open("Exotel Dashboard").sheet1

    # 🧹 Remove duplicates in new data
    if "Id" in df_new.columns:
        df_new = df_new.drop_duplicates(subset=["Id"])

    existing_data = sheet.get_all_values()

    # 🟢 FIRST RUN
    if len(existing_data) == 0:
        print("🆕 First time upload")
        sheet.update([df_new.columns.values.tolist()] + df_new.values.tolist())
        return

    # 🟡 APPEND MODE
    df_old = pd.DataFrame(existing_data[1:], columns=existing_data[0])

    if "Id" in df_old.columns:
        df_old = df_old.drop_duplicates(subset=["Id"])

    # 🔥 Combine & remove duplicates
    combined = pd.concat([df_old, df_new], ignore_index=True)

    if "Id" in combined.columns:
        combined = combined.drop_duplicates(subset=["Id"])

    # 📤 Clear & upload clean data
    sheet.clear()
    sheet.update([combined.columns.values.tolist()] + combined.values.tolist())

    print("✅ Sheet updated (No duplicates, appended safely)")

# ==============================
# 🚀 MAIN
# ==============================

if __name__ == "__main__":
    start, end = get_yesterday_date()

    df = download_exotel_report(start, end)

    if df.empty:
        print("⚠️ No data fetched")
    else:
        upload_to_sheets(df)
