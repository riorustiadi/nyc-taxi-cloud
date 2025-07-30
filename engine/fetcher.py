import requests
import json
import os
from datetime import datetime

REGISTER = os.path.join("data", "stored_files.json")

def is_parquet_available(url):
    try:
        resp = requests.head(url, timeout=5)
        return resp.status_code == 200
    except Exception:
        return False

def get_stored_files():
    os.makedirs(os.path.dirname(REGISTER), exist_ok=True)
    if os.path.exists(REGISTER):
        with open(REGISTER, "r") as f:
            return set(json.load(f))
    return set()

def mark_file_as_stored(fname):
    files = get_stored_files()
    files.add(fname)
    os.makedirs(os.path.dirname(REGISTER), exist_ok=True)
    with open(REGISTER, "w") as f:
        json.dump(list(files), f)

def get_new_tripdata_url():
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{:02d}.parquet"
    stored = get_stored_files()
    current_year = datetime.now().year
    # Cek dari 2015 sampai tahun depan
    for year in range(2025, current_year + 2):
        for month in range(1, 13):
            fname = f"yellow_tripdata_{year}-{month:02d}.parquet"
            url = base_url.format(year, month)
            if is_parquet_available(url) and fname not in stored:
                return url, fname
    return None, None

def get_all_new_tripdata_urls():
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{:02d}.parquet"
    stored = get_stored_files()
    current_year = datetime.now().year
    new_files = []
    for year in range(2025, current_year + 1):
        for month in range(1, 3):
            fname = f"yellow_tripdata_{year}-{month:02d}.parquet"
            url = base_url.format(year, month)
            if is_parquet_available(url) and fname not in stored:
                new_files.append((url, fname))
    return new_files