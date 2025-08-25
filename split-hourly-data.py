import os

# add station names here
station_names = {
#     "BTM": "bert-mooney-airport",
}

input_file = "noaa-hourly.csv"
output_dir = os.path.join(os.path.dirname(__file__), "asos-noaa-hourly-split")
os.makedirs(output_dir, exist_ok=True)

station_files = {}

with open(input_file, "r") as infile:
    for line in infile:
        line = line.strip()
        if not line or ',' not in line:
            continue
        parts = line.split(",")
        station = parts[0]
        name = station_names.get(station, station)
        out_path = os.path.join(output_dir, f"{name}.csv")
        if out_path not in station_files:
            station_files[out_path] = open(out_path, "w")
            station_files[out_path].write("station,datetime,lon,lat,elev,precip\n")
        station_files[out_path].write(line + "\n")

for f in station_files.values():
    f.close()