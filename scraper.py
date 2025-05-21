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
    # A more common User-Agent string
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
})

# Step 1: Fetch station options
try:
    response = session.get(url, timeout=20) # Increased timeout
    response.raise_for_status() # Check for HTTP errors
except requests.exceptions.RequestException as e:
    print(f"❌ Error fetching station list for temperature data: {e}")
    exit() # Exit if we can't get the station list

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
        res.raise_for_status() # Check for HTTP errors
        page = BeautifulSoup(res.text, 'html.parser')
        table = page.find("table", class_="table table-bordered")

        if table:
            rows = table.find_all("tr")[1:]  # skip header row
            for row in rows:
                cols = row.find_all("td")
                if len(cols) == 4:
                    province = cols[0].text.strip()
                    reported_station = cols[1].text.strip()
                    temp_str = cols[2].text.strip() # Max Temp (°C)
                    date_str = cols[3].text.strip()

                    if not date_str: # Skip if date string is empty
                        # print(f"ℹ️ Empty date for {station_name}, temp row. Skipping.")
                        continue
                    
                    # Parse date string to datetime object
                    # The site seems to use "DD Mon, YYYY" e.g., "29 May, 2024"
                    # pd.to_datetime usually handles this well
                    parsed_date = pd.to_datetime(date_str, errors='coerce') 
                    
                    if pd.isna(parsed_date): # Skip if date could not be parsed
                        # print(f"⚠️ Warning: Could not parse date '{date_str}' for {station_name} (temp). Skipping row.")
                        continue

                    record = {
                        'Station ID': station_id,
                        'Station Name': station_name,
                        'Province': province,
                        'Reported Station': reported_station,
                        'Max Temp (°C)': temp_str, # Keep as string, assuming it's like "35.0"
                        'Date': parsed_date # Store as datetime object
                    }
                    temp_data.append(record)

                    # 👇 Live output row by row (format date for printing)
                    print(f"{record['Station ID']}, {record['Station Name']}, {record['Province']}, "
                          f"{record['Reported Station']}, {record['Max Temp (°C)']}, {record['Date'].strftime('%d %b, %Y')}")
    
    except requests.exceptions.Timeout:
        print(f"❌ Timeout occurred for {station_name} (temp)")
    except requests.exceptions.RequestException as e:
        print(f"❌ Request Error on {station_name} (temp): {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred on {station_name} (temp): {e}")

    time.sleep(0.5)  # Respect server

# Step 4: Convert to DataFrame
# 'Date' column will be of datetime64[ns] type here
new_df = pd.DataFrame(temp_data)

if new_df.empty:
    print("\n⚠️ No new temperature data was scraped. Exiting.")
    if os.path.exists("testTemp.csv"):
         print(f"📁 Existing data remains in: testTemp.csv\n")
    exit()

# Step 5: (Original step for blank dates now handled by parsing checks above)
# Ensure 'Date' column is clean in new_df
new_df = new_df.dropna(subset=['Date'])


# Step 6: Load existing CSV if exists, then merge
csv_file = "testTemp.csv"
if os.path.exists(csv_file):
    try:
        existing_df = pd.read_csv(csv_file)
        # Convert 'Date' column in existing_df to datetime objects
        # Explicitly try to infer datetime format if dayfirst might be ambiguous
        # However, if we save in '%d %b, %Y', pandas should handle it
        existing_df['Date'] = pd.to_datetime(existing_df['Date'], errors='coerce')
        existing_df = existing_df.dropna(subset=['Date']) # Remove rows where date couldn't be parsed
        
        # Concatenate old and new data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    except pd.errors.EmptyDataError:
        print(f"⚠️ Existing CSV '{csv_file}' is empty. Will use new temperature data only.")
        combined_df = new_df
    except Exception as e:
        print(f"⚠️ Error reading existing CSV '{csv_file}': {e}. Will use new temperature data only.")
        combined_df = new_df
else:
    combined_df = new_df

# Step 7: Convert Date column to datetime and clean
# This ensures consistency if there were different paths taken above.
combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
combined_df = combined_df.dropna(subset=['Date'])  # Remove rows where Date couldn't be parsed

# Remove duplicates:
# key for duplicates: Station ID, Date, and Reported Station
# keep='last' ensures that data from the latest scrape (new_df) is preferred.
combined_df = combined_df.drop_duplicates(subset=['Station ID', 'Date', 'Reported Station'], keep='last')

# Step 8: Sort and save
combined_df = combined_df.sort_values(by=['Date', 'Station Name'], ascending=[False, True])

# OPTIONAL: Convert 'Date' column to desired string format '%d %b, %Y' before saving.
# If you prefer the default pandas date format in CSV (YYYY-MM-DD), you can remove/comment out this line.
combined_df['Date'] = combined_df['Date'].dt.strftime('%d %b, %Y')

# Save the CSV with proper encoding to handle special characters like °C
combined_df.to_csv(csv_file, index=False, encoding='utf-8-sig')

print("\n✅ Max Temperature scraping and processing completed successfully!")
print(f"📁 Updated data saved to: {csv_file}\n")
