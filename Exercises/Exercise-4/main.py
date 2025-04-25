import glob
import json
import csv
from pathlib import Path

def flatten_dict(d, parent_key='', sep='_'):
    """Flatten a nested dictionary into a single-level dictionary."""
    items = []
    for key, value in d.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, sep).items())
        elif isinstance(value, list):
            for i, val in enumerate(value):
                items.append((f"{new_key}{sep}{i}", val))
        else:
            items.append((new_key, value))
    return dict(items)

def main():
    # Bước 1: Tìm tất cả file JSON trong thư mục data
    data_dir = Path("data")
    json_files = glob.glob(str(data_dir / "**" / "*.json"), recursive=True)

    if not json_files:
        print("No JSON files found in the data directory")
        return

    # Bước 2 & 3: Đọc từng file JSON và flatten dữ liệu
    for json_path in json_files:
        try:
            # Đọc file JSON
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Nếu data là danh sách, xử lý từng phần tử; nếu không, đưa vào danh sách
            if not isinstance(data, list):
                data = [data]

            # Flatten từng phần tử
            flattened_data = [flatten_dict(item) for item in data]

            # Bước 4: Chuyển đổi sang CSV
            if not flattened_data:
                print(f"No data to process in {json_path}")
                continue

            # Lấy tất cả các key (header) từ dữ liệu đã flatten
            all_keys = set()
            for item in flattened_data:
                all_keys.update(item.keys())
            headers = sorted(all_keys)

            # Tạo tên file CSV từ tên file JSON
            csv_path = Path(json_path).with_suffix(".csv")

            # Ghi dữ liệu vào file CSV
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                for row in flattened_data:
                    writer.writerow(row)

            print(f"Successfully converted {json_path} to {csv_path}")

        except Exception as e:
            print(f"Failed to process {json_path}: {e}")

if __name__ == "__main__":
    main()
