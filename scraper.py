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
        'station': station_id, # station_id is already a string from opt['value']
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
                        continue
                    
                    parsed_date = pd.to_datetime(date_str, errors='coerce') 
                    
                    if pd.isna(parsed_date): # Skip if date could not be parsed
                        continue

                    record = {
                        'Station ID': str(station_id), # Ensure station_id is stored as string
                        'Station Name': station_name,
                        'Province': province,
                        'Reported Station': reported_station,
                        'Max Temp (°C)': temp_str,
                        'Date': parsed_date # Store as datetime object
                    }
                    temp_data.append(record)

                    # Optional: Live output row by row
                    # print(f"{record['Station ID']}, {record['Station Name']}, {record['Province']}, "
                    #       f"{record['Reported Station']}, {record['Max Temp (°C)']}, {record['Date'].strftime('%d %b, %Y')}")
    
    except requests.exceptions.Timeout:
        print(f"❌ Timeout occurred for {station_name} (ID: {station_id}) (temp)")
    except requests.exceptions.RequestException as e:
        print(f"❌ Request Error on {station_name} (ID: {station_id}) (temp): {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred on {station_name} (ID: {station_id}) (temp): {e}")

    time.sleep(0.25)  # Adjusted sleep time, modify if necessary

# Step 4: Convert to DataFrame
new_df = pd.DataFrame(temp_data)

if new_df.empty:
    print("\n⚠️ No new temperature data was scraped. Exiting.")
    if os.path.exists("testTemp.csv"):
         print(f"📁 Existing data remains in: testTemp.csv\n")
    exit()
else:
    # Ensure 'Station ID' in new_df is string type.
    if 'Station ID' in new_df.columns:
        new_df['Station ID'] = new_df['Station ID'].astype(str)
    else:
        print("⚠️ Critical Error: 'Station ID' column is missing in newly scraped temperature data. Cannot proceed.")
        exit()
    # 'Date' column in new_df is already datetime64 type.

# Step 5: (Original step for blank dates now handled by parsing checks above)
# new_df should already have a clean 'Date' column.

# Step 6: Load existing CSV if exists, then merge
csv_file = "testTemp.csv"
combined_df = new_df.copy() # Initialize with new_df.

if os.path.exists(csv_file):
    print(f"\nℹ️ Existing CSV file '{csv_file}' found. Attempting to load and merge temperature data.")
    try:
        # Read 'Station ID' as string to ensure type consistency.
        existing_df = pd.read_csv(csv_file, dtype={'Station ID': str})
        
        if existing_df.empty:
            print(f"ℹ️ Existing temperature CSV '{csv_file}' was loaded but is empty. Will use new data only.")
            # combined_df is already new_df
        else:
            # Ensure 'Station ID' from existing_df is string.
            if 'Station ID' in existing_df.columns:
                existing_df['Station ID'] = existing_df['Station ID'].astype(str)
            else:
                print(f"⚠️ Warning: 'Station ID' column not found in existing temp CSV '{csv_file}'. Cannot reliably merge. Using new data only.")
                existing_df = pd.DataFrame() # Empty to prevent further processing

            if not existing_df.empty and 'Date' in existing_df.columns:
                existing_df['Date'] = pd.to_datetime(existing_df['Date'], errors='coerce')
                existing_df.dropna(subset=['Date'], inplace=True)
                
                if existing_df.empty:
                    print(f"ℹ️ Existing temp CSV '{csv_file}' became empty after date parsing/cleaning. Will use new data only.")
                    # combined_df is already new_df
                else:
                    print(f"✅ Successfully loaded and processed {len(existing_df)} rows from existing temperature data in '{csv_file}'. Merging with {len(new_df)} new rows.")
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    print(f"ℹ️ Combined temperature DataFrame has {len(combined_df)} rows before deduplication.")
            elif not existing_df.empty:
                 print(f"⚠️ Warning: 'Date' column not found in existing temp CSV '{csv_file}'. Cannot reliably merge. Using new data only.")
                 # combined_df is already new_df
                 
    except pd.errors.EmptyDataError:
        print(f"⚠️ Existing temp CSV '{csv_file}' is empty (caught EmptyDataError). Will use new data only.")
        # combined_df is already new_df
    except Exception as e:
        print(f"⚠️ Error reading or processing existing temp CSV '{csv_file}': {e}. Will use new data only.")
        # combined_df is already new_df
else:
    print(f"ℹ️ No existing temp CSV file found at '{csv_file}'. Starting with new temperature data only.")
    # combined_df is already new_df

# Step 7: Final processing on combined_df
if not combined_df.empty:
    # Ensure 'Date' column is datetime type
    if 'Date' in combined_df.columns:
        combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
        combined_df.dropna(subset=['Date'], inplace=True) 
    else:
        print("⚠️ Warning: 'Date' column missing in combined temperature DataFrame. Cannot perform date-based operations.")

    # Ensure 'Station ID' is string type before deduplication
    if 'Station ID' in combined_df.columns:
        combined_df['Station ID'] = combined_df['Station ID'].astype(str)
    else:
        print("⚠️ Warning: 'Station ID' column missing in combined temperature DataFrame. Cannot perform deduplication.")

    # Remove duplicates
    dedup_cols = ['Station ID', 'Date', 'Reported Station']
    if all(col in combined_df.columns for col in dedup_cols) and not combined_df.empty:
        initial_rows = len(combined_df)
        combined_df.drop_duplicates(subset=dedup_cols, keep='last', inplace=True)
        print(f"ℹ️ Deduplication removed {initial_rows - len(combined_df)} rows from temperature data. Combined DataFrame now has {len(combined_df)} rows.")
    elif not combined_df.empty:
        print(f"⚠️ Skipping deduplication for temperature data because one or more key columns ({dedup_cols}) are missing or DataFrame is empty.")

else:
    print("\n⚠️ Combined temperature DataFrame is empty before final processing. Nothing to save.")


# Step 8: Sort and save
if not combined_df.empty and 'Date' in combined_df.columns:
    sort_by_cols = ['Date', 'Station Name']
    if all(col in combined_df.columns for col in sort_by_cols):
        combined_df.sort_values(by=sort_by_cols, ascending=[False, True], inplace=True)
    elif 'Date' in combined_df.columns:
        print("ℹ️ 'Station Name' column missing in temp data, sorting by 'Date' only.")
        combined_df.sort_values(by=['Date'], ascending=False, inplace=True)
    
    combined_df['Date'] = combined_df['Date'].dt.strftime('%d %b, %Y')

    try:
        combined_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"\n✅ Max Temperature scraping and processing completed successfully!")
        print(f"📁 Updated temperature data saved to: {csv_file} ({len(combined_df)} rows)\n")
    except Exception as e:
        print(f"❌ Error saving temperature data to CSV '{csv_file}': {e}")

elif combined_df.empty:
    print("\n⚠️ Combined temperature data is empty after all processing. Nothing was saved.")
else:
    print(f"\n⚠️ Combined temperature data is not empty ({len(combined_df)} rows) but essential 'Date' column is missing. Cannot save in standard format.")
