import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm import tqdm
import os

# Base URL
url = "https://nwfc.pmd.gov.pk/new/max-temp.php"

# Start session
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
})

# Step 1: Get station list
try:
    response = session.get(url, timeout=20)
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    print(f"❌ Error fetching station list: {e}")
    exit()

soup = BeautifulSoup(response.text, 'html.parser')
stations = soup.select("select[name='station'] option")
station_list = [(opt['value'], opt.text.strip()) for opt in stations if opt['value'].isdigit()]

# Step 2: Scrape temperature data
temp_data = []
print("\n🌡️ Scraping Max Temperature Data...\n")
for station_id, station_name in tqdm(station_list, desc="🔍 Scraping", unit="station"):
    form_data = {
        'station': station_id,
        'filter': 'station'
    }

    try:
        res = session.post(url, data=form_data, timeout=10)
        res.raise_for_status()
        page = BeautifulSoup(res.text, 'html.parser')
        table = page.find("table", class_="table table-bordered")

        if table:
            rows = table.find_all("tr")[1:]
            for row in rows:
                cols = row.find_all("td")
                if len(cols) == 4:
                    province = cols[0].text.strip()
                    reported_station = cols[1].text.strip()
                    temp_str = cols[2].text.strip()
                    date_str = cols[3].text.strip()

                    if not date_str:
                        continue

                    parsed_date = pd.to_datetime(date_str, format='%d %b, %Y', dayfirst=True, errors='coerce')
                    if pd.isna(parsed_date):
                        continue

                    record = {
                        'Station ID': str(station_id),
                        'Station Name': station_name,
                        'Province': province,
                        'Reported Station': reported_station,
                        'Max Temp (°C)': temp_str,
                        'Date': parsed_date
                    }
                    temp_data.append(record)

    except Exception as e:
        print(f"❌ Error at {station_name} (ID: {station_id}): {e}")

    time.sleep(0.25)

# Step 3: New scraped data to DataFrame
new_df = pd.DataFrame(temp_data)
if new_df.empty:
    print("⚠️ No new temperature data found. Exiting.")
    exit()

new_df['Station ID'] = new_df['Station ID'].astype(str)
new_df['Date'] = pd.to_datetime(new_df['Date'], errors='coerce')
new_df.dropna(subset=['Date'], inplace=True)

# Step 4: Load existing CSV data if available
csv_file = "testTemp.csv"
if os.path.exists(csv_file):
    try:
        print(f"\n📂 Reading existing data from '{csv_file}'...")
        existing_df = pd.read_csv(csv_file, encoding='utf-8-sig', dtype={'Station ID': str})
        # Parse dates from existing CSV properly
        existing_df['Date'] = pd.to_datetime(existing_df['Date'], format='%d %b, %Y', errors='coerce')
        existing_df.dropna(subset=['Date'], inplace=True)
        print(f"✅ Existing data loaded: {len(existing_df)} rows.")
    except Exception as e:
        print(f"⚠️ Failed to read existing file: {e}")
        existing_df = pd.DataFrame()
else:
    existing_df = pd.DataFrame()

# Step 5: Combine and remove duplicates
combined_df = pd.concat([existing_df, new_df], ignore_index=True)
before_dedup = len(combined_df)
combined_df.drop_duplicates(subset=['Station ID', 'Date', 'Reported Station'], keep='last', inplace=True)
after_dedup = len(combined_df)
print(f"🧹 Removed {before_dedup - after_dedup} duplicates. Final dataset: {after_dedup} rows.")

# Step 6: Filter from 1 Apr 2025 to today
combined_df = combined_df[combined_df['Date'] >= pd.to_datetime('2025-04-01')]

# Step 7: Final sort & save
combined_df.sort_values(by=['Date', 'Station Name'], ascending=[False, True], inplace=True)
combined_df['Date'] = combined_df['Date'].dt.strftime('%d %b, %Y')

try:
    combined_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"\n✅ Final data saved to '{csv_file}' with {len(combined_df)} rows (from 1 Apr 2025 onwards).")
except Exception as e:
    print(f"❌ Error saving file: {e}")
