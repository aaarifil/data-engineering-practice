import psycopg2
import csv

# Kết nối đến PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",  # Tên cơ sở dữ liệu
    user="postgres",    # Tên người dùng PostgreSQL
    password="postgres",# Mật khẩu PostgreSQL
    host="postgres",    # Máy chủ (PostgreSQL chạy trên localhost)
    port="5432"         # Cổng PostgreSQL (mặc định)
)

# Tạo một con trỏ để thực thi các câu lệnh SQL
with conn.cursor() as cur:
    # 1. Xóa tất cả các ràng buộc khóa ngoại nếu có
    remove_constraints = """
    ALTER TABLE IF EXISTS orders DROP CONSTRAINT IF EXISTS orders_product_id_fkey;
    """
    cur.execute(remove_constraints)
    conn.commit()

    # 2. Xóa các bảng nếu đã tồn tại
    drop_tables = """
    DROP TABLE IF EXISTS transactions, accounts, products CASCADE;
    """
    cur.execute(drop_tables)
    conn.commit()

    # 3. Tạo lại các bảng
    create_products_table = """
    CREATE TABLE IF NOT EXISTS products (
        product_id INT PRIMARY KEY,
        product_code VARCHAR(10),
        product_description VARCHAR(255)
    );
    """
    
    # Tạo bảng accounts
    create_accounts_table = """
    CREATE TABLE IF NOT EXISTS accounts (
        customer_id INT PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        address_1 VARCHAR(255),
        address_2 VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(50),
        zip_code VARCHAR(20),
        join_date DATE
    );
    """
    
    # Tạo bảng transactions (sau khi bảng products đã được tạo)
    create_transactions_table = """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR(50) PRIMARY KEY,
        transaction_date DATE,
        product_id INT,
        product_code VARCHAR(10),
        product_description VARCHAR(255),
        quantity INT,
        account_id INT
    );
    """
    
    # 4. Chạy các câu lệnh tạo bảng
    print("Creating products table...")
    cur.execute(create_products_table)  # Đảm bảo tạo bảng products trước
    print("Creating accounts table...")
    cur.execute(create_accounts_table)
    print("Creating transactions table...")
    cur.execute(create_transactions_table)

    # Commit changes
    conn.commit()

    # 5. Thêm khóa ngoại sau khi các bảng đã được tạo
    add_foreign_keys = """
    ALTER TABLE transactions
    ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_account_id FOREIGN KEY (account_id) REFERENCES accounts(customer_id) ON DELETE CASCADE;
    """

    print("Adding foreign key constraints...")
    cur.execute(add_foreign_keys)
    
    # Commit changes
    conn.commit()

    # 6. Chèn dữ liệu từ CSV vào các bảng
    def load_data_from_csv(csv_file, table_name, columns):
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Bỏ qua dòng tiêu đề (header row)
            
            for row in reader:
                # Tạo câu truy vấn để chèn dữ liệu
                query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
                cur.execute(query, row)
        
        conn.commit()

    # 7. Chèn dữ liệu cho từng bảng
    load_data_from_csv("data/accounts.csv", "accounts", ["customer_id", "first_name", "last_name", "address_1", "address_2", "city", "state", "zip_code", "join_date"])
    load_data_from_csv("data/products.csv", "products", ["product_id", "product_code", "product_description"])
    load_data_from_csv("data/transactions.csv", "transactions", ["transaction_id", "transaction_date", "product_id", "product_code", "product_description", "quantity", "account_id"])

# 8. Đóng kết nối
conn.close()

print("Data has been loaded successfully.")
