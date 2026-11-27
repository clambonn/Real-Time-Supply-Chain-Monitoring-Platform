USE DATABASE SUPPLY_CHAIN_DWH;
USE SCHEMA BRONZE;

CREATE OR REPLACE TABLE STG_ORDERS (
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    order_status VARCHAR(100),
    order_purchase_timestamp VARCHAR(100),
    order_approved_at VARCHAR(100),
    order_delivered_carrier_date VARCHAR(100),
    order_delivered_customer_date VARCHAR(100),
    order_estimated_delivery_date VARCHAR(100),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE ETL_WATERMARK (
    source_table VARCHAR(100),
    last_watermark_timestamp VARCHAR(100),
    last_order_id VARCHAR(100),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CUSTOMERS (
    customer_id VARCHAR(100),
    customer_unique_id VARCHAR(100),
    customer_zip_code_prefix VARCHAR(50),
    customer_city VARCHAR(150),
    customer_state VARCHAR(50)
);

CREATE OR REPLACE TABLE ORDER_ITEMS (
    order_id VARCHAR(100),
    order_item_id VARCHAR(50),
    product_id VARCHAR(100),
    seller_id VARCHAR(100),
    shipping_limit_date VARCHAR(100),
    price VARCHAR(50),
    freight_value VARCHAR(50)
);

CREATE OR REPLACE TABLE ORDER_PAYMENTS (
    order_id VARCHAR(100),
    payment_sequential VARCHAR(50),
    payment_type VARCHAR(100),
    payment_installments VARCHAR(50),
    payment_value VARCHAR(50)
);

CREATE OR REPLACE TABLE ORDER_REVIEWS (
    review_id VARCHAR(100),
    order_id VARCHAR(100),
    review_score VARCHAR(50),
    review_comment_title VARCHAR(500),
    review_comment_message VARCHAR(5000),
    review_creation_date VARCHAR(100),
    review_answer_timestamp VARCHAR(100)
);

CREATE OR REPLACE TABLE PRODUCTS (
    product_id VARCHAR(100),
    product_category_name VARCHAR(200),
    product_name_lenght VARCHAR(50),
    product_description_lenght VARCHAR(50),
    product_photos_qty VARCHAR(50),
    product_weight_g VARCHAR(50),
    product_length_cm VARCHAR(50),
    product_height_cm VARCHAR(50),
    product_width_cm VARCHAR(50)
);

CREATE OR REPLACE TABLE SELLERS (
    seller_id VARCHAR(100),
    seller_zip_code_prefix VARCHAR(50),
    seller_city VARCHAR(150),
    seller_state VARCHAR(50)
);

CREATE OR REPLACE TABLE GEOLOCATION (
    geolocation_zip_code_prefix VARCHAR(50),
    geolocation_lat VARCHAR(50),
    geolocation_lng VARCHAR(50),
    geolocation_city VARCHAR(150),
    geolocation_state VARCHAR(50)
);

CREATE OR REPLACE TABLE PRODUCT_CATEGORY_NAME_TRANSLATION (
    product_category_name VARCHAR(200),
    product_category_name_english VARCHAR(200)
);
