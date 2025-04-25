import requests
import gzip
from pathlib import Path

def main():
    # Bước 1: Tải file từ S3 bằng requests
    s3_url = "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    local_file = Path("wet.paths.gz")

    try:
        response = requests.get(s3_url, stream=True)
        response.raise_for_status()  # Kiểm tra nếu URL không hợp lệ

        with open(local_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Successfully downloaded {local_file}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {s3_url}: {e}")
        return

    # Bước 2: Giải nén và mở file (file là gzip, nội dung là text)
    try:
        with gzip.open(local_file, "rt", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Failed to extract or read {local_file}: {e}")
        return
    finally:
        # Xóa file sau khi xử lý
        if local_file.exists():
            local_file.unlink()

    if not lines:
        print("File is empty")
        return

    # Bước 3: Lấy URI từ dòng đầu tiên
    first_line = lines[0].strip()
    print(f"URI from the first line: {first_line}")

    # Bước 4: In từng dòng ra terminal
    print("\nAll lines in the file:")
    for line in lines:
        print(line.strip())

if __name__ == "__main__":
    main()
