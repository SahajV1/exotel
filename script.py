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
    if cookie_string:
        for part in cookie_string.split(";"):
            if "=" in part:
                key, val = part.strip().split("=", 1)
                cookies[key] = val
    return cookies

# ==============================
# 📅 YESTERDAY DATE (IST)
# ==============================

def get_date():
    india = pytz.timezone("Asia/Kolkata")
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

    print(f"📡 Fetching report from {start} to {end}")

    response = requests.get(api_url, headers=headers, cookies=cookies)

    if response.status_code != 200:
        raise Exception(f"❌ API failed: {response.text}")

    data = response.json()
    s3_url = data.get("report", {}).get("url")

    if not s3_url:
        print("⚠️ No report URL found (no data or expired cookies)")
        return pd.DataFrame()

    print("✅ Got report download URL")

    csv_response = requests.get(s3_url)

    if csv_response.status_code != 200:
        raise Exception("❌ CSV download failed")

    content = csv_response.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    print(f"✅ Rows fetched: {len(rows)}")

    if len(rows) == 0:
        return pd.DataFrame()

    return pd.DataFrame(rows)

# ==============================
# 📤 UPLOAD TO GOOGLE SHEETS
# ==============================

def upload_to_sheets(df):
    try:
        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        creds_dict = json.loads(GOOGLE_CREDENTIALS)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        client = gspread.authorize(creds)

        # Sheet1 tab
        sheet = client.open_by_key(
            "1E4N_qMD-WwV2sW4g8mbi4PgSWHc6juXRwsTVqDbffjg"
        ).sheet1

        print("✅ Connected to Google Sheet")

        print("📌 DataFrame rows:", len(df))
        print("📌 Columns:", df.columns.tolist())

        existing_data = sheet.get_all_values()
        print("📌 Existing sheet rows:", len(existing_data))

        # FIRST UPLOAD
        if len(existing_data) == 0:
            data = [df.columns.tolist()] + df.astype(str).values.tolist()
            sheet.update("A1", data)
            print("✅ First upload complete")
            return

        # DUPLICATE REMOVE (if Id exists)
        matched_col = None
        possible_cols = ["Id", "id", "ID"]

        for col in possible_cols:
            if col in df.columns:
                matched_col = col
                break

        if matched_col:
            existing_ids = set(row[0] for row in existing_data[1:] if len(row) > 0)

            before = len(df)
            df = df[~df[matched_col].astype(str).isin(existing_ids)]
            after = len(df)

            print(f"🔁 Removed duplicates: {before - after}")

        else:
            print("⚠️ No Id column found, skipping duplicate check")

        # APPEND NEW ROWS
        if not df.empty:
            rows_to_add = df.astype(str).values.tolist()
            sheet.append_rows(rows_to_add, value_input_option="USER_ENTERED")
            print(f"✅ Appended {len(rows_to_add)} new rows")
        else:
            print("⚠️ No new rows to append")

    except Exception as e:
        print("❌ Upload Error:", str(e))

# ==============================
# 🚀 MAIN
# ==============================

if __name__ == "__main__":
    try:
        start, end = get_date()

        df = download_exotel_report(start, end)

        if df.empty:
            print("⚠️ No data fetched")
        else:
            upload_to_sheets(df)

    except Exception as e:
        print("❌ Main Error:", str(e))
