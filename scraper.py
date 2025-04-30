import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm import tqdm
import os

# Base URL for Max Temperature
url = "https://nwfc.pmd.gov.pk/new/max-temp.php"

# Start session
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0"
})

# Step 1: Fetch station options
response = session.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
stations = soup.select("select[name='station'] option")
station_list = [(opt['value'], opt.text.strip()) for opt in stations if opt['value'].isdigit()]

# Step 2: Prepare to store scraped data
temp_data = []

# Step 3: Scrape each station using tqdm
print("\n🌡️ Scraping Max Temperature Data...\n")
for station_id, station_name in tqdm(station_list, desc="🔍 Scraping", unit="station"):
    form_data = {
        'station': station_id,
        'filter': 'station'
    }

    try:
        res = session.post(url, data=form_data, timeout=10)
        page = BeautifulSoup(res.text, 'html.parser')
        table = page.find("table", class_="table table-bordered")

        if table:
            rows = table.find_all("tr")[1:]  # skip header row
            for row in rows:
                cols = row.find_all("td")
                if len(cols) == 4:
                    province = cols[0].text.strip()
                    reported_station = cols[1].text.strip()
                    temp = cols[2].text.strip()
                    date = cols[3].text.strip()

                    # Explicitly format the date (e.g., 30 Apr, 2025)
                    date = pd.to_datetime(date, errors='coerce').strftime('%d %b, %Y') if date else None

                    record = {
                        'Station ID': station_id,
                        'Station Name': station_name,
                        'Province': province,
                        'Reported Station': reported_station,
                        'Max Temp (°C)': temp,
                        'Date': date
                    }

                    temp_data.append(record)

                    # 👇 Live output row by row
                    print(f"{record['Station ID']}, {record['Station Name']}, {record['Province']}, "
                          f"{record['Reported Station']}, {record['Max Temp (°C)']}, {record['Date']}")

    except Exception as e:
        print(f"❌ Error scraping {station_name}: {e}")

    time.sleep(0.5)  # Respect server

# Step 4: Convert to DataFrame
new_df = pd.DataFrame(temp_data)

# Step 5: Remove rows with blank or NaT in date before proceeding
new_df = new_df[new_df['Date'].str.strip() != ""]

# Step 6: Load existing CSV if exists, then merge
csv_file = "testTemp.csv"
if os.path.exists(csv_file):
    existing_df = pd.read_csv(csv_file)
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
else:
    combined_df = new_df

# Step 7: Convert Date column to datetime and clean
combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce', dayfirst=True)
combined_df = combined_df.dropna(subset=['Date'])  # Remove rows where Date couldn't be parsed
combined_df = combined_df.drop_duplicates(subset=['Station ID', 'Date', 'Reported Station'])

# Step 8: Sort and save
combined_df = combined_df.sort_values(by='Date', ascending=False)

# Save the CSV with proper encoding to handle special characters like °C
combined_df.to_csv(csv_file, index=False, encoding='utf-8-sig')

print("\n✅ Max Temperature scraping completed successfully!")
print(f"📁 Updated data saved to: {csv_file}\n")
