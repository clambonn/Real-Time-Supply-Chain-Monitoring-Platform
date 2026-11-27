USE DATABASE SUPPLY_CHAIN_DWH;
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE DIM_CUSTOMERS (
    customer_id VARCHAR(100),
    customer_unique_id VARCHAR(100),
    customer_zip_code_prefix VARCHAR(50),
    customer_city VARCHAR(150),
    customer_state VARCHAR(50)
);

CREATE OR REPLACE TABLE DIM_PRODUCTS (
    product_id VARCHAR(100),
    product_category_name VARCHAR(200),
    product_category_name_english VARCHAR(200),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g DECIMAL(12, 2),
    product_length_cm DECIMAL(12, 2),
    product_height_cm DECIMAL(12, 2),
    product_width_cm DECIMAL(12, 2)
);

CREATE OR REPLACE TABLE DIM_SELLERS (
    seller_id VARCHAR(100),
    seller_zip_code_prefix VARCHAR(50),
    seller_city VARCHAR(150),
    seller_state VARCHAR(50)
);

CREATE OR REPLACE TABLE DIM_DATE (
    date_key INTEGER,
    full_date DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

CREATE OR REPLACE TABLE FACT_ORDERS (
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    purchase_date_key INTEGER,
    order_status VARCHAR(100),
    order_purchase_timestamp TIMESTAMP_NTZ,
    order_approved_at TIMESTAMP_NTZ,
    order_delivered_carrier_date TIMESTAMP_NTZ,
    order_delivered_customer_date TIMESTAMP_NTZ,
    order_estimated_delivery_date TIMESTAMP_NTZ,
    delay_days INTEGER,
    is_delayed BOOLEAN,
    ingested_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE FACT_ORDER_ITEMS (
    order_id VARCHAR(100),
    order_item_id INTEGER,
    product_id VARCHAR(100),
    seller_id VARCHAR(100),
    shipping_limit_date TIMESTAMP_NTZ,
    price DECIMAL(12, 2),
    freight_value DECIMAL(12, 2)
);

CREATE OR REPLACE TABLE FACT_PAYMENTS (
    order_id VARCHAR(100),
    payment_sequential INTEGER,
    payment_type VARCHAR(100),
    payment_installments INTEGER,
    payment_value DECIMAL(12, 2)
);

CREATE OR REPLACE TABLE FACT_REVIEWS (
    review_id VARCHAR(100),
    order_id VARCHAR(100),
    review_score INTEGER,
    review_comment_title VARCHAR(500),
    review_comment_message VARCHAR(5000),
    review_creation_date TIMESTAMP_NTZ,
    review_answer_timestamp TIMESTAMP_NTZ
);