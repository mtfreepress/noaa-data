import requests
import json
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise ValueError("NOAA API token not found in .env file.")
BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
HEADERS = {"token": TOKEN}
LOCATION_ID = "FIPS:30"
DATASET_ID = "GHCND"
DATATYPE_ID = "PRCP"
UNITS = "metric"
LIMIT = 1000  # Max per request
PROGRESS_FILE = "progress.txt"

def load_progress():
    progress = {}
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    year, date = line.split('=', 1)
                    try:
                        progress[int(year)] = date
                    except ValueError:
                        continue
    return progress

def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        for year, date in progress.items():
            f.write(f"{year}={date}\n")

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

STATIONS_FILE = "stations-mt.json"

def fetch_stations():
    """Fetch and cache all Montana stations metadata for decoding station IDs."""
    if os.path.exists(STATIONS_FILE):
        with open(STATIONS_FILE, "r") as f:
            try:
                stations = json.load(f)
                print(f"Loaded {len(stations)} stations from cache.")
                return stations
            except Exception:
                print("Failed to load cached stations, refetching...")
    print("Fetching Montana stations metadata from NOAA API...")
    stations = {}
    endpoint = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations"
    offset = 1
    limit = 1000
    while True:
        params = {
            "locationid": LOCATION_ID,
            "limit": limit,
            "offset": offset
        }
        resp = requests.get(endpoint, headers=HEADERS, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"Error fetching stations: {resp.status_code} {resp.text}")
            break
        data = resp.json()
        results = data.get("results", [])
        for s in results:
            stations[s["id"]] = {
                "name": s.get("name"),
                "latitude": s.get("latitude"),
                "longitude": s.get("longitude"),
                "elevation": s.get("elevation"),
                "elevationUnit": s.get("elevationUnit"),
                "datacoverage": s.get("datacoverage"),
                "mindate": s.get("mindate"),
                "maxdate": s.get("maxdate")
            }
        print(f"Fetched {len(results)} stations (offset {offset})")
        if len(results) < limit:
            break
        offset += limit
        time.sleep(1)
    with open(STATIONS_FILE, "w") as f:
        json.dump(stations, f, indent=2)
    print(f"Saved {len(stations)} stations to {STATIONS_FILE}")
    return stations

def fetch_and_save(year, resume_date=None):
    start_date = datetime(year, 6, 1)
    end_date = datetime(year, 8, 31)
    if resume_date:
        start_date = datetime.strptime(resume_date, "%Y-%m-%d")
    all_results = []
    for single_date in daterange(start_date, end_date):
        date_str = single_date.strftime("%Y-%m-%d")
        params = {
            "datasetid": DATASET_ID,
            "datatypeid": DATATYPE_ID,
            "locationid": LOCATION_ID,
            "startdate": date_str,
            "enddate": date_str,
            "units": UNITS,
            "limit": LIMIT
        }
        tries = 0
        while tries < 5:
            try:
                response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    results = data.get("results", [])
                    all_results.extend(results)
                    print(f"Fetched {len(results)} records for {date_str}")
                    break
                elif response.status_code == 429:
                    print(f"Rate limited. Sleeping 60s...")
                    time.sleep(60)
                elif response.status_code >= 500:
                    print(f"Server error {response.status_code}. Retrying in 30s...")
                    time.sleep(30)
                else:
                    print(f"Error fetching {date_str}: {response.status_code} {response.text}")
                    break
            except Exception as e:
                print(f"Exception fetching {date_str}: {e}. Retrying in 30s...")
                time.sleep(30)
            tries += 1
        # Save progress after each day
        progress[year] = date_str
        save_progress(progress)
        time.sleep(1)  # Be polite to the API
    # Save all results for the year
    with open(f"summer-precip-data-{year}.json", "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"Saved {len(all_results)} records to summer-precip-data-{year}.json")


if __name__ == "__main__":
    # Fetch and cache station metadata first
    stations = fetch_stations()
    progress = load_progress()
    for year in range(2005, 2026):
        resume_date = progress.get(year)
        fetch_and_save(year, resume_date=resume_date)
        time.sleep(2)  # Avoid hitting rate limits

def fetch_and_save(year):
    startdate = f"{year}-06-01"
    enddate = f"{year}-08-31"
    offset = 1
    all_results = []

    while True:
        params = {
            "datasetid": DATASET_ID,
            "datatypeid": DATATYPE_ID,
            "locationid": LOCATION_ID,
            "startdate": startdate,
            "enddate": enddate,
            "units": UNITS,
            "limit": LIMIT,
            "offset": offset
        }
        response = requests.get(BASE_URL, headers=HEADERS, params=params)
        if response.status_code != 200:
            print(f"Error fetching {year} offset {offset}: {response.status_code}")
            break
        data = response.json()
        results = data.get("results", [])
        if not results:
            break
        all_results.extend(results)
        print(f"Fetched {len(results)} records for {year} (offset {offset})")
        if len(results) < LIMIT:
            break
        offset += LIMIT
        time.sleep(1)  # Be polite to the API

    with open(f"summer-precip-data-{year}.json", "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"Saved {len(all_results)} records to summer-precip-data-{year}.json")

for year in range(2005, 2026):
    fetch_and_save(year)
    time.sleep(2)  # Avoid hitting rate limits