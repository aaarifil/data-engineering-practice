import requests
import gzip

def main():
    url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-13/wet.paths.gz"

    print("Đang tải tệp gzip từ URL...")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        content = gzip.decompress(response.content)

        print("10 dòng đầu tiên của tệp:")
        for i, line in enumerate(content.decode("utf-8").splitlines()):
            print(line)

            # Tải file wet từ S3
            wet_url = f"https://data.commoncrawl.org/{line}"
            print(f"\n→ Đang tải từ: {wet_url}")

            try:
                wet_response = requests.get(wet_url, stream=True, timeout=10)
                wet_response.raise_for_status()

                # Đọc và giải nén 5 dòng đầu tiên từ file wet
                with gzip.GzipFile(fileobj=wet_response.raw) as f:
                    print("Nội dung file: ")
                    for j, wet_line in enumerate(f):
                        print(wet_line.decode("utf-8").strip())
                        if j == 4:  # Dừng lại sau khi đọc 5 dòng (0 -> 4)
                            break
            except Exception as e:
                print(f"Lỗi khi xử lý {wet_url}: {e}")

            if i == 9:  # Chỉ xử lý 10 dòng đầu tiên từ tệp wet.paths.gz
                break

    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi tải tệp từ URL: {e}")
    except Exception as e:
        print(f"Lỗi không xác định: {e}")

if __name__ == "__main__":
    main()
