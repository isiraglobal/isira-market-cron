import requests
import csv
import io
import zipfile
import json
from datetime import datetime

# Use today's date to auto-build the NSE file URL
today = datetime.utcnow()

# Format date to match NSE file naming, example: 05DEC2025
url_date = today.strftime("%d%b%Y").upper() 
year = today.strftime("%Y")
month = today.strftime("%b").upper()

file_name = f"cm{url_date}bhav.csv"
zip_name = file_name + ".zip"

BHAVCOPY_URL = f"https://www1.nseindia.com/content/historical/EQUITIES/{year}/{month}/{zip_name}"

# Base44 endpoint where parsed data will be sent
BASE44_ENDPOINT = "https://market.isira.club/api/functions/ingestDailyData"


def fetch_zip():
    print("Downloading:", BHAVCOPY_URL)
    headers = {"User-Agent": "Mozilla/5.0"}  # Required by NSE
    resp = requests.get(BHAVCOPY_URL, headers=headers)
    resp.raise_for_status()
    return resp.content


def extract_csv(zip_bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        with z.open(file_name) as csv_file:
            return csv_file.read().decode("utf-8")


def parse_csv(csv_text):
    f = io.StringIO(csv_text)
    reader = csv.DictReader(f)
    rows = []

    for row in reader:
        rows.append({
            "symbol": row["SYMBOL"],
            "open": float(row["OPEN"]),
            "high": float(row["HIGH"]),
            "low": float(row["LOW"]),
            "close": float(row["CLOSE"]),
            "volume": int(row["TOTTRDQTY"])
        })

    return rows


def push_to_base44(rows):
    payload = {
        "exchange": "NSE",
        "date": today.strftime("%Y-%m-%d"),
        "prices": rows
    }

    print(f"Sending {len(rows)} rows to Base44...")
    resp = requests.post(
        BASE44_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    print("Base44 Response:", resp.text)


if __name__ == "__main__":
    zip_bytes = fetch_zip()
    csv_text = extract_csv(zip_bytes)
    rows = parse_csv(csv_text)
    push_to_base44(rows)
