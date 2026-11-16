import os
import urllib
import pandas as pd

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.types import NVARCHAR


# ==========================================
# SQL SERVER CONFIGURATION
# ==========================================

server_name = r'DESKTOP-4RIU2UD\SQLEXPRESS'
database_name = 'SupplyChainDB'
driver = 'ODBC Driver 17 for SQL Server'

params = urllib.parse.quote_plus(
    f"DRIVER={{{driver}}};"
    f"SERVER={server_name};"
    f"DATABASE={database_name};"
    f"Trusted_Connection=yes"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True,
    pool_pre_ping=True
)


# ==========================================
# PATHS
# ==========================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
raw_data_dir = os.path.join(BASE_DIR, 'Data', 'Raw')


# ==========================================
# SOURCE FILES
# ==========================================

files = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv"
]

STRING_PRESERVE_DTYPES = {
    "customer_id": str,
    "customer_unique_id": str,
    "customer_zip_code_prefix": str,
    "geolocation_zip_code_prefix": str,
    "seller_id": str,
    "seller_zip_code_prefix": str,
    "order_id": str,
    "product_id": str,
    "review_id": str
}

LONG_TEXT_COLUMNS = {
    "review_comment_title",
    "review_comment_message"
}


def build_sql_dtype_map(df):
    dtype_map = {}
    for col in df.select_dtypes(include=['object', 'string']).columns:
        if col in LONG_TEXT_COLUMNS:
            dtype_map[col] = NVARCHAR(2000)
        else:
            dtype_map[col] = NVARCHAR(255)
    return dtype_map


# ==========================================
# CREATE BRONZE SCHEMA
# ==========================================

with engine.begin() as conn:

    conn.execute(text("""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.schemas
            WHERE name = 'bronze'
        )
        BEGIN
            EXEC('CREATE SCHEMA [bronze]');
        END
    """))


# ==========================================
# LOAD RAW CSV TABLES
# ==========================================

print("\nStarting Bronze Initial Load...\n")

inspector = inspect(engine)

for file in files:

    file_path = os.path.join(raw_data_dir, file)
    table_name = file.replace('.csv', '')

    if not os.path.exists(file_path):
        print(f"[WARNING] File not found: {file}")
        continue

    if inspector.has_table(table_name, schema='bronze'):
        print(f"[SKIPPED] {table_name} already exists.")
        continue

    print(f"[LOADING] {table_name}")

    df = pd.read_csv(
        file_path,
        dtype=STRING_PRESERVE_DTYPES
    )

    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].where(pd.notna(df[col]), None)

    sql_dtypes = build_sql_dtype_map(df)

    df.to_sql(
        name=table_name,
        con=engine,
        schema='bronze',
        if_exists='fail',
        index=False,
        chunksize=5000,
        dtype=sql_dtypes
    )

    print(f"[SUCCESS] Loaded {len(df)} rows into {table_name}")


# ==========================================
# INDEX CUSTOMERS TABLE FOR CONSUMER LOOKUP
# ==========================================

with engine.begin() as conn:

    if conn.execute(text("""
        SELECT OBJECT_ID('bronze.olist_customers_dataset', 'U')
    """)).scalar() is not None:

        col_max_len = conn.execute(text("""
            SELECT max_length
            FROM sys.columns
            WHERE object_id = OBJECT_ID('bronze.olist_customers_dataset')
            AND name = 'customer_id'
        """)).scalar()

        if col_max_len == -1:
            raise RuntimeError(
                "bronze.olist_customers_dataset.customer_id is MAX length (created by an older run). "
                "Drop bronze.olist_customers_dataset and re-run setup_bronze.py to enable indexing."
            )

        duplicate_customer = conn.execute(text("""
            SELECT TOP 1 customer_id
            FROM bronze.olist_customers_dataset
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        """)).first()

        if duplicate_customer:
            raise RuntimeError(
                f"Duplicate customer_id found: {duplicate_customer[0]}. "
                "Resolve duplicates before creating the unique index."
            )

        conn.execute(text("""
            IF NOT EXISTS (
                SELECT 1
                FROM sys.indexes
                WHERE name = 'UX_Bronze_Customers_customer_id'
                AND object_id = OBJECT_ID('bronze.olist_customers_dataset')
            )
            BEGIN
                CREATE UNIQUE NONCLUSTERED INDEX UX_Bronze_Customers_customer_id
                ON bronze.olist_customers_dataset(customer_id)
                WHERE customer_id IS NOT NULL;
            END
        """))


# ==========================================
# CREATE ORDERS TABLE
# ==========================================

with engine.begin() as conn:

    conn.execute(text("""
        IF OBJECT_ID('bronze.Orders', 'U') IS NULL
        BEGIN

            CREATE TABLE bronze.Orders (

                id BIGINT IDENTITY(1,1) PRIMARY KEY,

                ingested_at DATETIME2(3)
                    NOT NULL DEFAULT SYSUTCDATETIME(),

                order_id NVARCHAR(255) NOT NULL,

                customer_id NVARCHAR(255),

                order_status NVARCHAR(255),

                order_purchase_timestamp NVARCHAR(255),

                order_approved_at NVARCHAR(255),

                order_delivered_carrier_date NVARCHAR(255),

                order_delivered_customer_date NVARCHAR(255),

                order_estimated_delivery_date NVARCHAR(255)

            );

        END
    """))

    duplicate = conn.execute(text("""
        SELECT TOP 1 order_id
        FROM bronze.Orders
        WHERE order_id IS NOT NULL
        GROUP BY order_id
        HAVING COUNT(*) > 1
    """)).first()

    if duplicate:
        raise RuntimeError(
            f"Duplicate order_id found: {duplicate[0]}. "
            "Resolve duplicates before creating the unique index."
        )

    conn.execute(text("""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'UX_Bronze_Orders_order_id'
            AND object_id = OBJECT_ID('bronze.Orders')
        )
        BEGIN
            CREATE UNIQUE INDEX UX_Bronze_Orders_order_id
            ON bronze.Orders(order_id);
        END
    """))


# ==========================================
# CREATE DEAD LETTER TABLE
# ==========================================

with engine.begin() as conn:

    conn.execute(text("""
        IF OBJECT_ID('bronze.Dead_Letter', 'U') IS NULL
        BEGIN

            CREATE TABLE bronze.Dead_Letter (

                id BIGINT IDENTITY(1,1) PRIMARY KEY,

                topic NVARCHAR(255) NOT NULL,

                partition_id INT NOT NULL,

                message_offset BIGINT NOT NULL,

                raw_message NVARCHAR(MAX),

                error_message NVARCHAR(2000),

                ingested_at DATETIME2(3)
                    NOT NULL DEFAULT SYSUTCDATETIME()

            );

        END
    """))

    conn.execute(text("""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'UX_Bronze_Dead_Letter_Offset'
            AND object_id = OBJECT_ID('bronze.Dead_Letter')
        )
        BEGIN

            CREATE UNIQUE INDEX UX_Bronze_Dead_Letter_Offset
            ON bronze.Dead_Letter(
                topic,
                partition_id,
                message_offset
            );

        END
    """))


# ==========================================
# FINISH
# ==========================================

print("\n===================================")
print("BRONZE DATABASE SETUP COMPLETED")
print("===================================")

print("Schema: bronze")
print("Orders table: bronze.Orders")
print("Dead Letter: bronze.Dead_Letter")

engine.dispose()