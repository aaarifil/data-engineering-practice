import requests
import pandas as pd
from bs4 import BeautifulSoup
import os

def scrape_file_url(base_url, target_timestamp):
    """Tìm tên file tương ứng với thời gian sửa đổi cuối cùng trên trang web"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()

        # Phân tích HTML với BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tìm bảng chứa danh sách file
        table = soup.find('table')
        if not table:
            raise ValueError("Không tìm thấy bảng danh sách file trên trang web")

        # Tìm tất cả các hàng trong bảng
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 3:
                continue
            # Cột thứ 2 chứa thời gian sửa đổi cuối cùng
            timestamp = cols[1].text.strip()
            # Cột thứ 1 chứa tên file
            filename = cols[0].text.strip()
            
            # So sánh thời gian sửa đổi
            if timestamp == target_timestamp:
                # Xây dựng URL đầy đủ
                return f"{base_url}{filename}"
        raise ValueError(f"Không tìm thấy file với thời gian sửa đổi {target_timestamp}")
    except Exception as e:
        print(f"Lỗi khi scrape trang web: {e}")
        return None

def download_file(url, output_path):
    """Tải file từ URL và lưu vào output_path"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, stream=True, headers=headers)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Tải file thành công: {output_path}")
        return True
    except Exception as e:
        print(f"Lỗi khi tải file: {e}")
        return False

def find_highest_temperature(file_path):
    """Đọc file CSV và tìm bản ghi có HourlyDryBulbTemperature cao nhất"""
    try:
        # Đọc file CSV với pandas
        df = pd.read_csv(file_path)
        
        # Kiểm tra cột HourlyDryBulbTemperature có tồn tại không
        if 'HourlyDryBulbTemperature' not in df.columns:
            raise ValueError("Không tìm thấy cột 'HourlyDryBulbTemperature' trong file CSV")

        # Chuyển đổi cột HourlyDryBulbTemperature thành số, bỏ qua giá trị không hợp lệ
        df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')
        
        # Tìm giá trị cao nhất của HourlyDryBulbTemperature
        max_temp = df['HourlyDryBulbTemperature'].max()
        
        # Lọc các bản ghi có giá trị cao nhất
        max_temp_records = df[df['HourlyDryBulbTemperature'] == max_temp]
        
        # In các bản ghi
        print("\nBản ghi có HourlyDryBulbTemperature cao nhất:")
        print(max_temp_records.to_string(index=False))
    except Exception as e:
        print(f"Lỗi khi phân tích file CSV: {e}")

def main():
    # URL cơ sở và thời gian sửa đổi cần tìm
    base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    target_timestamp = "2024-01-19 10:27"
    
    # Bước 1: Scrape trang web để tìm URL của file
    file_url = scrape_file_url(base_url, target_timestamp)
    if not file_url:
        print("Không thể tìm thấy URL của file. Thoát chương trình.")
        return
    
    # Bước 2: Tải file về
    output_path = os.path.join(os.getcwd(), "weather_data.csv")
    if not download_file(file_url, output_path):
        print("Không thể tải file. Thoát chương trình.")
        return
    
    # Bước 3: Phân tích file với Pandas và tìm nhiệt độ cao nhất
    find_highest_temperature(output_path)

if __name__ == "__main__":
    main()
