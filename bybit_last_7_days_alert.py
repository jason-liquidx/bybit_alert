import requests
import os
import pandas as pd
from datetime import datetime, timedelta

# Base URL for the Bybit Spot data
base_url = "https://public.bybit.com/spot/MONUSDT/"

# Directory to store downloaded files
output_dir = 'bybit_spot_data/'
os.makedirs(output_dir, exist_ok=True)

# Output CSV file
output_csv = 'aggregated_by_day_bybit_data.csv'

# Function to download the file for a specific date
def download_file(date):
    url = f"{base_url}MONUSDT_{date}.csv.gz"
    response = requests.get(url)

    if response.status_code == 200:
        file_path = os.path.join(output_dir, f"MONUSDT_{date}.csv.gz")
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"✅ Downloaded {url}")
        return file_path
    elif response.status_code == 404:
        print(f"⏭️ File not found for {date}. Skipping.")
        return None
    else:
        print(f"❌ Error for {date}: Status Code {response.status_code}")
        return None

# Function to process and aggregate data
def process_and_aggregate(file_path):
    try:
        df = pd.read_csv(file_path)

        # Convert 'timestamp' to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Create 'date' column
        df['date'] = df['timestamp'].dt.date

        # Calculate buy/sell volumes
        df['buy_volume'] = df.apply(lambda x: float(x['volume']) if x['side'] == 'buy' else 0, axis=1)
        df['sell_volume'] = df.apply(lambda x: float(x['volume']) if x['side'] == 'sell' else 0, axis=1)

        # Extract minute-level activity
        if 'minute' not in df.columns:
            df['minute'] = df['timestamp'].dt.floor('min')

        # Count unique active minutes per day
        active_minutes_per_day = df.groupby('date')['minute'].nunique().reset_index(name='active_minutes')

        # Aggregate volume + price per day
        agg_df = df.groupby('date').agg(
            buy_volume_sum=('buy_volume', 'sum'),
            sell_volume_sum=('sell_volume', 'sum'),
            avg_price=('price', 'mean')
        ).reset_index()

        # Merge with active_minutes
        result = pd.merge(agg_df, active_minutes_per_day, on='date', how='left')

        # Calculate trading frequency (active minutes / total minutes in day)
        result['trading_frequency_pct'] = result['active_minutes'] / (24 * 60) * 100

        return result

    except Exception as e:
        print(f"⚠️ Error processing {file_path}: {e}")
        return pd.DataFrame()

# Get the past 7 days from today
end_date = datetime.now()
start_date = end_date - timedelta(days=6)  # Last 7 days = today and previous 6

# Final DataFrame
all_data = pd.DataFrame()

# Loop through each date
current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime('%Y-%m-%d')
    file_path = download_file(date_str)

    if file_path:
        day_data = process_and_aggregate(file_path)
        all_data = pd.concat([all_data, day_data], ignore_index=True)

    current_date += timedelta(days=1)

# Save to CSV
if not all_data.empty:
    all_data.to_csv(output_csv, index=False)
    print(f"✅ Aggregated data saved to {output_csv}")
else:
    print("⚠️ No data to save.")
