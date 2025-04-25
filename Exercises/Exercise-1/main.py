import requests
import os
import zipfile
from pathlib import Path

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def main():
    # Tạo thư mục downloads nếu chưa tồn tại
    downloads_dir = Path("downloads")
    downloads_dir.mkdir(exist_ok=True)

    # Tải và xử lý từng file
    for uri in download_uris:
        try:
            # Lấy tên file từ URI
            filename = uri.split("/")[-1]
            zip_path = downloads_dir / filename

            # Tải file
            response = requests.get(uri, stream=True)
            response.raise_for_status()  # Kiểm tra nếu URI không hợp lệ

            # Lưu file zip
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # Giải nén file zip
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(downloads_dir)

            # Xóa file zip sau khi giải nén
            os.remove(zip_path)
            print(f"Successfully downloaded and extracted {filename}")

        except requests.exceptions.RequestException as e:
            print(f"Failed to download {uri}: {e}")
        except zipfile.BadZipFile:
            print(f"Failed to extract {filename}: Invalid zip file")

if __name__ == "__main__":
    main()
