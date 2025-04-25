import requests
import os
from urllib.parse import urlparse

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def download_file(url, download_dir):
    """Tải file từ URL và lưu vào thư mục download_dir"""
    try:
        # Lấy tên file từ URL
        file_name = os.path.basename(urlparse(url).path)
        file_path = os.path.join(download_dir, file_name)
        
        # Tạo thư mục download nếu chưa tồn tại
        os.makedirs(download_dir, exist_ok=True)
        
        print(f"Downloading {file_name} from {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Kiểm tra lỗi HTTP
        
        # Lưu file vào thư mục
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded {file_name} to {file_path}")
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def main():
    # Thư mục lưu file
    download_dir = os.path.join(os.getcwd(), "download")
    
    # Tải từng file từ danh sách URL
    for url in download_uris:
        success = download_file(url, download_dir)
        if not success:
            print(f"Failed to download {url}, continuing with next file...")
    
    print("Download process completed.")

if __name__ == "__main__":
    main()
