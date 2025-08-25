# NOAA Precipitation 

### Data source:
[Iowa State Environmental Mesonet](https://mesonet.agron.iastate.edu/request/download.phtml?network=MT_ASOS)

### Query to get data:
```
curl 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?network=MT_ASOS&station=BIL&station=BZN&station=GDV&station=GFA&station=GGW&station=GPI&station=GTF&station=HLN&station=JDN&station=LVM&station=LWT&station=MLS&station=MSO&station=OLF&station=RPX&station=SDY&station=WYS&station=HVR&station=DLN&station=BTM&station=BHK&station=CTB&data=p01m&year1={startYear}&month1={startMonth}&day1={startDay}&year2={endYear}&month2={endMonth}&day2={endDay}&tz=Etc%2FUTC&format=onlycomma&latlon=yes&elev=yes&missing=empty&trace=T&direct=no&report_type=3&report_type=4' -o 2005-2025.csv
```
*note: data we used was from June 1, 2005 - Aug 19, 2025*
### Prerequisites:
1. Python 3 — tested on `3.13.7`

## Setup Instructions

### 1. Create and activate a Python virtual environment

```sh
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```sh
pip install -r requirements.txt
```

### 3. Project Files Overview

#### For data from mesonet:
- **split-hourly-data.py**  
  Splits the large hourly CSV into per-station files for easier processing.  
  _Run:_  
  ```sh
  python split-hourly-data.py
  ```


- **find-highest-precip-storms.py**  
  Analyzes per-station hourly data to find the highest precipitation storms and outputs results as CSV and GeoJSON.  
  _Run:_  
  ```sh
  python find-highest-precip-storms.py
  ```

#### For NOAA data
##### Ended up being a dead end, data went back a long ways but was daily data so not suitable for finding the largest thunderstorms in the state
- **get-precipitation-data.py**  
  Fetches and saves NOAA precipitation data for Montana stations using the NOAA API.  
  _Run:_  
  ```sh
  python get-precipitation-data.py
  ```
- **preen-noaa-historic-data.py**  
  Cleans and filters daily summary data, moving non-Montana stations to a separate folder.  
  _Run:_  
  ```sh
  python preen-noaa-historic-data.py
  ```

### Notes

- All scripts assume data files are in the correct locations as described above.
- Output files (CSV, GeoJSON) will be written to the project directory.
- See comments in each script for more details or customization options.


## License

"New" BSD License (aka "3-clause"). See [LICENSE](LICENSE) for details.