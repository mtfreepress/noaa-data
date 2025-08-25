import os
import csv
import json
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor

def is_summer(dt):
    # June 1 to September 15
    if dt.month == 6 or dt.month == 7 or dt.month == 8:
        return True
    if dt.month == 9 and dt.day <= 15:
        return True
    return False

def in_year_range(dt, start_year=1980, end_year=2025):
    return start_year <= dt.year <= end_year

def to_mst(dt):
    # Convert UTC datetime to MST (UTC-6)
    return dt - timedelta(hours=6)

def process_file(filepath):
    station = os.path.basename(filepath).replace(".csv", "")
    hourly = {}
    storms = []
    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            precip = row["precip"].strip()
            if precip == "T" or precip == "":
                val = 0.0
            else:
                try:
                    val = float(precip)
                    if val > 150:
                        continue  # Skip this row if precip is over 150 to filter out erroneous values like 8" in 1 hr @ Glendive
                except ValueError:
                    val = 0.0
            dt = datetime.strptime(row["datetime"], "%Y-%m-%d %H:%M")
            hour_key = dt.replace(minute=0)
            lat = row["lat"].strip()
            lon = row["lon"].strip()
            if hour_key not in hourly or val > hourly[hour_key][1]:
                hourly[hour_key] = (hour_key, val, lat, lon)
    records = [v for k, v in sorted(hourly.items())]
    storm = []
    for rec in records:
        dt, val, lat, lon = rec
        if val > 0:
            if not storm:
                storm = [rec]
            else:
                prev_dt = storm[-1][0]
                if (dt - prev_dt) <= timedelta(hours=1):
                    storm.append(rec)
                else:
                    if (
                        len(storm) < 3
                        and is_summer(storm[0][0])
                        and in_year_range(storm[0][0])
                    ):
                        total = sum(r[1] for r in storm)
                        storms.append((
                            station,
                            storm[0][2],  # lat
                            storm[0][3],  # lon
                            storm[0][0],  # start UTC
                            storm[-1][0], # end UTC
                            total,
                            len(storm)
                        ))
                    storm = [rec]
        else:
            if storm:
                if (
                    len(storm) < 3
                    and is_summer(storm[0][0])
                    and in_year_range(storm[0][0])
                ):
                    total = sum(r[1] for r in storm)
                    storms.append((
                        station,
                        storm[0][2],
                        storm[0][3],
                        storm[0][0],
                        storm[-1][0],
                        total,
                        len(storm)
                    ))
                storm = []
    if storm:
        if (
            len(storm) < 3
            and is_summer(storm[0][0])
            and in_year_range(storm[0][0])
        ):
            total = sum(r[1] for r in storm)
            storms.append((
                station,
                storm[0][2],
                storm[0][3],
                storm[0][0],
                storm[-1][0],
                total,
                len(storm)
            ))
    return storms

if __name__ == "__main__":
    data_dir = os.path.join(os.path.dirname(__file__), "asos-noaa-hourly-split")
    input_files = [
        os.path.join(data_dir, fname)
        for fname in os.listdir(data_dir)
        if fname.endswith(".csv") and fname != "station.csv"
    ]

    all_storms = []
    with ProcessPoolExecutor() as executor:
        for storms in executor.map(process_file, input_files):
            all_storms.extend(storms)

    top_storms = sorted(all_storms, key=lambda x: x[5], reverse=True)[:10]

    output_file = os.path.join(os.path.dirname(__file__), "top_storms.csv")
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["site", "location", "date", "hourRange", "length", "mm", "in"])
        for s in top_storms:
            # s = (station, lat, lon, start_utc, end_utc, total_mm, hours)
            start_mst = to_mst(s[3])
            end_mst = to_mst(s[4])
            date_str = start_mst.strftime("%m-%d-%Y")
            hour_range = f"{start_mst.strftime('%H:%M')}-{end_mst.strftime('%H:%M')}"
            location = f"{s[1]} {s[2]}"
            mm = round(s[5], 2)
            inches = round(s[5] / 25.4, 2)
            writer.writerow([
                s[0],
                location,
                date_str,
                hour_range,
                s[6],
                mm,
                inches
            ])

    print(f"Wrote top storms to {output_file}")

    # --- GeoJSON output ---
    geojson_features = []
    for s in top_storms:
        start_mst = to_mst(s[3])
        end_mst = to_mst(s[4])
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(s[2]), float(s[1])]  # lon, lat
            },
            "properties": {
                "site": s[0],
                "location": f"{s[1]} {s[2]}",
                "date": start_mst.strftime("%m-%d-%Y"),
                "hourRange": f"{start_mst.strftime('%H:%M')}-{end_mst.strftime('%H:%M')}",
                "length": s[6],
                "mm": round(s[5], 2),
                "in": round(s[5] / 25.4, 2),
                "start_utc": s[3].isoformat(),
                "end_utc": s[4].isoformat()
            }
        }
        geojson_features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": geojson_features
    }

    geojson_file = os.path.join(os.path.dirname(__file__), "top_storms.geojson")
    with open(geojson_file, "w") as f:
        json.dump(geojson, f, indent=2)

    print(f"Wrote top storms to {geojson_file}")