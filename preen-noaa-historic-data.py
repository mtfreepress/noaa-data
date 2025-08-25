import os
import json
import csv
import shutil

DATA_DIR = "noaa-daily-summary-data-aug-18-2025"
STATIONS_FILE = "stations-mt.json"
NOT_MT_DIR = os.path.join(DATA_DIR, "not-montana")


os.makedirs(NOT_MT_DIR, exist_ok=True)

for fname in os.listdir(DATA_DIR):
    if not fname.endswith(".csv"):
        continue
    fpath = os.path.join(DATA_DIR, fname)
    with open(fpath, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        try:
            first_row = next(reader)
        except StopIteration:
            # Empty file, move it
            shutil.move(fpath, os.path.join(NOT_MT_DIR, fname))
            continue
        # Check if the NAME field ends with ', MT US'
        if not first_row.get("NAME", "").strip().endswith(", MT US"):
            os.makedirs(NOT_MT_DIR, exist_ok=True)  # Ensure directory exists
            shutil.move(fpath, os.path.join(NOT_MT_DIR, fname))
            print(f"Moved {fname} (not a Montana station)")