import os
import requests
import csv
import io
import time
import pytz
import pandas as pd
from urllib.parse import quote
import gspread
from google.oauth2.service_account import Credentials
import json
from datetime import datetime, timedelta

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
        if '=' in part:
            key, val = part.strip().split('=', 1)
            cookies[key] = val
    return cookies

# ==============================
# 📅 YESTERDAY DATE (IST)
# ==============================

def get_date():
    india = pytz.timezone('Asia/Kolkata')
    now = datetime.now(india)
    yesterday = now - timedelta(days=1)

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
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://my.exotel.com/accounts/{EXOTEL_ACCOUNT_SID}/reports",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }

    cookies = parse_cookies(EXOTEL_COOKIES)

    print(f"📡 Fetching: {start}")

    response = requests.get(api_url, headers=headers, cookies=cookies)

    if response.status_code != 200:
        raise Exception(f"❌ API failed: {response.text}")

    data = response.json()
    s3_url = data.get("report", {}).get("url")

    if not s3_url:
        print("⚠️ No S3 URL (no data or cookies expired)")
        return pd.DataFrame()

    print("✅ Got S3 URL")

    csv_response = requests.get(s3_url)

    if csv_response.status_code != 200:
        raise Exception("❌ CSV download failed")

    content = csv_response.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    print(f"✅ Rows fetched: {len(rows)}")

    return pd.DataFrame(rows)

# ==============================
# 📊 UPLOAD TO GOOGLE SHEETS (APPEND)
# ==============================

def upload_to_sheets(df):
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

    client = gspread.authorize(creds)

    # 🔥 Using Sheet ID (reliable)
    sheet = client.open_by_key("1E4N_qMD-WwV2sW4g8mbi4PgSWHc6juXRwsTVqDbffjg").sheet1

    print("📤 Uploading (append mode)...")

    existing_data = sheet.get_all_values()

    # 🟢 FIRST RUN
    if len(existing_data) == 0:
        data = [df.columns.values.tolist()] + df.values.tolist()
        sheet.update(values=data, range_name="A1")
        print("✅ First upload complete")
        return

    # 🟡 REMOVE DUPLICATES (based on Id column)
    if "Id" in df.columns:
        existing_ids = set(row[0] for row in existing_data[1:])
        df = df[~df["Id"].isin(existing_ids)]

    # 🔥 APPEND DATA
    if not df.empty:
        sheet.append_rows(df.values.tolist())
        print(f"✅ Appended {len(df)} new rows")
    else:
        print("⚠️ No new data to append")

# ==============================
# 🚀 MAIN
# ==============================

if __name__ == "__main__":
    start, end = get_date()

    df = download_exotel_report(start, end)

    if df.empty:
        print("⚠️ No data fetched for yesterday")
    else:
        upload_to_sheets(df)
