import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
TARGET_TIMESTAMP = "2024-01-19 10:27"

def find_target_file():
    response = requests.get(BASE_URL)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'lxml')
    rows = soup.find_all("tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 2:
            timestamp = cols[1].text.strip()
            if timestamp == TARGET_TIMESTAMP:
                filename = cols[0].text.strip()
                return filename

    raise Exception("File with timestamp 2024-01-19 10:27 not found.")

def download_file(filename):
    download_url = BASE_URL + filename
    local_path = os.path.join("downloads", filename)

    os.makedirs("downloads", exist_ok=True)

    response = requests.get(download_url)
    response.raise_for_status()

    with open(local_path, 'wb') as f:
        f.write(response.content)
    print(f"Downloaded file to {local_path}")
    return local_path

def analyze_file(filepath):
    df = pd.read_csv(filepath)

    if 'HourlyDryBulbTemperature' not in df.columns:
        raise Exception("'HourlyDryBulbTemperature' column not found in the file.")

    # Chuyển đổi nhiệt độ về kiểu số (nếu cần, vì có thể là string)
    df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')
    
    max_temp = df['HourlyDryBulbTemperature'].max()
    hottest_records = df[df['HourlyDryBulbTemperature'] == max_temp]

    print("\n🌡Records with the highest HourlyDryBulbTemperature:")
    print(hottest_records)

def main():
    try:
        print("Looking for file...")
        filename = find_target_file()

        print(f"Found file: {filename}")
        filepath = download_file(filename)

        print("Analyzing file...")
        analyze_file(filepath)

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
