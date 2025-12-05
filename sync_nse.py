import requests
import csv
import io
import zipfile
import json
from datetime import datetime

today = datetime.utcnow()

# Format NSE URL parts
url_date = today.strftime("%d%b%Y").upper()
year = today.strftime("%Y")
month = today.strftime("%b").upper()

# File names
file_name = f"cm{url_date}bhav.csv"
zip_name = f"{file_name}.zip"

# NSE official bhavcopy URL
BHAVCOPY_URL = f"https://www1.nseindia.com/content/historical/EQUITIES/{year}/{month}/{zip_name}"

# Base44 ingestion endpoint
BASE44_ENDPOINT = "https://market.isira.club/api/functions/ingestDailyData"


def fetch_zip():
    print("Downloading:", BHAVCOPY_URL)
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(BHAVCOPY_URL, headers=headers)
    resp.raise_for_status()
    return resp.content


def extract_csv(zip_bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        with z.open(file_name) as csv_file:
            return csv_file.read().decode("utf-8")


def parse_csv(csv_text):
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = []

    for row in reader:
        rows.append({
            "symbol": row["SYMBOL"].strip(),
            "open": float(row["OPEN"]),
            "high": float(row["HIGH"]),
            "low": float(row["LOW"]),
            "close": float(row["CLOSE"]),
            "volume": int(row["TOTTRDQTY"]),
        })

    return rows


def push_to_base44(rows):
    payload = {
        "exchange": "NSE",
        "date": today.strftime("%Y-%m-%d"),
        "prices": rows,
    }

    print(f"Sending {len(rows)} rows to Base44...")
    resp = requests.post(
        BASE44_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    print("Base44 Response:", resp.text)


if __name__ == "__main__":
    try:
        zip_bytes = fetch_zip()
        csv_text = extract_csv(zip_bytes)
        rows = parse_csv(csv_text)
        push_to_base44(rows)
        print("NSE sync completed.")
    except Exception as e:
        print("Error during NSE sync:", e)
