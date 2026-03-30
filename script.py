import os
import requests
import csv
import io
import time
import pandas as pd
from urllib.parse import quote
import gspread
from google.oauth2.service_account import Credentials
import json

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
# 📅 FIXED DATE (27 MARCH)
# ==============================

def get_date():
    return "2026-03-27 00:00:00", "2026-03-27 23:59:59"

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

    print("🔍 DEBUG RESPONSE:", response.text)

    if response.status_code != 200:
        raise Exception(f"❌ API failed: {response.text}")

    data = response.json()
    s3_url = data.get("report", {}).get("url")

    if not s3_url:
        print("❌ No S3 URL — possible reasons:")
        print("1. No data on this date")
        print("2. Cookies expired")
        return pd.DataFrame()

    print("✅ Got S3 URL")

    # 🔽 Download CSV
    csv_response = requests.get(s3_url)

    if csv_response.status_code != 200:
        raise Exception("❌ CSV download failed")

    content = csv_response.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    print(f"✅ Rows fetched: {len(rows)}")

    return pd.DataFrame(rows)

# ==============================
# 📊 UPLOAD TO GOOGLE SHEETS
# ==============================

def upload_to_sheets(df):
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # 🔥 Load creds from GitHub secret
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

    client = gspread.authorize(creds)

    sheet = client.open("Exotel Dashboard").sheet1

    print("📤 Uploading to Google Sheets...")

    # Clear old data
    sheet.clear()

    # Prepare data
    data = [df.columns.values.tolist()] + df.values.tolist()

    # Upload
    sheet.update(values=data, range_name="A1")

    print("✅ Sheet updated successfully")

# ==============================
# 🚀 MAIN
# ==============================

if __name__ == "__main__":
    start, end = get_date()

    df = download_exotel_report(start, end)

    if df.empty:
        print("⚠️ No data fetched for 27 March")
    else:
        print("🔥 SUCCESS — pushing to sheet")
        upload_to_sheets(df)
