USE DATABASE SUPPLY_CHAIN_DWH;
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE DIM_CUSTOMERS AS
SELECT DISTINCT
    TRIM(customer_id) AS customer_id,
    TRIM(customer_unique_id) AS customer_unique_id,
    TRIM(customer_zip_code_prefix) AS customer_zip_code_prefix,
    LOWER(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state
FROM BRONZE.CUSTOMERS
WHERE customer_id IS NOT NULL
  AND TRIM(customer_id) <> '';

CREATE OR REPLACE TABLE DIM_PRODUCTS AS
SELECT DISTINCT
    TRIM(p.product_id) AS product_id,
    NULLIF(TRIM(p.product_category_name), '') AS product_category_name,
    COALESCE(
        NULLIF(TRIM(t.product_category_name_english), ''),
        NULLIF(TRIM(p.product_category_name), ''),
        'unknown'
    ) AS product_category_name_english,
    TRY_TO_NUMBER(TRIM(p.product_name_lenght))::INTEGER AS product_name_lenght,
    TRY_TO_NUMBER(TRIM(p.product_description_lenght))::INTEGER AS product_description_lenght,
    TRY_TO_NUMBER(TRIM(p.product_photos_qty))::INTEGER AS product_photos_qty,
    TRY_TO_DECIMAL(TRIM(p.product_weight_g), 12, 2) AS product_weight_g,
    TRY_TO_DECIMAL(TRIM(p.product_length_cm), 12, 2) AS product_length_cm,
    TRY_TO_DECIMAL(TRIM(p.product_height_cm), 12, 2) AS product_height_cm,
    TRY_TO_DECIMAL(TRIM(p.product_width_cm), 12, 2) AS product_width_cm
FROM BRONZE.PRODUCTS p
LEFT JOIN BRONZE.PRODUCT_CATEGORY_NAME_TRANSLATION t
    ON LOWER(TRIM(p.product_category_name)) = LOWER(TRIM(t.product_category_name))
WHERE p.product_id IS NOT NULL
  AND TRIM(p.product_id) <> '';

CREATE OR REPLACE TABLE DIM_SELLERS AS
SELECT DISTINCT
    TRIM(seller_id) AS seller_id,
    TRIM(seller_zip_code_prefix) AS seller_zip_code_prefix,
    LOWER(TRIM(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state
FROM BRONZE.SELLERS
WHERE seller_id IS NOT NULL
  AND TRIM(seller_id) <> '';

CREATE OR REPLACE TABLE DIM_DATE AS
WITH date_spine AS (
    SELECT
        DATEADD(DAY, SEQ4(), '2016-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))
)
SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_key,
    full_date,
    YEAR(full_date) AS year,
    QUARTER(full_date) AS quarter,
    MONTH(full_date) AS month,
    MONTHNAME(full_date) AS month_name,
    DAY(full_date) AS day,
    DAYOFWEEKISO(full_date) AS day_of_week,
    DAYNAME(full_date) AS day_name,
    CASE WHEN DAYOFWEEKISO(full_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine;

CREATE OR REPLACE TABLE FACT_ORDERS AS
WITH cleaned_orders AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(customer_id) AS customer_id,
        LOWER(TRIM(order_status)) AS order_status,
        TRY_TO_TIMESTAMP_NTZ(TRIM(order_purchase_timestamp)) AS order_purchase_timestamp,
        TRY_TO_TIMESTAMP_NTZ(TRIM(order_approved_at)) AS order_approved_at,
        TRY_TO_TIMESTAMP_NTZ(TRIM(order_delivered_carrier_date)) AS order_delivered_carrier_date,
        TRY_TO_TIMESTAMP_NTZ(TRIM(order_delivered_customer_date)) AS order_delivered_customer_date,
        TRY_TO_TIMESTAMP_NTZ(TRIM(order_estimated_delivery_date)) AS order_estimated_delivery_date,
        ingested_at,
        ROW_NUMBER() OVER (PARTITION BY TRIM(order_id) ORDER BY ingested_at DESC) AS rn
    FROM BRONZE.STG_ORDERS
    WHERE order_id IS NOT NULL
      AND TRIM(order_id) <> ''
      AND customer_id IS NOT NULL
      AND TRIM(customer_id) <> ''
      AND order_status IS NOT NULL
      AND TRIM(order_status) <> ''
      AND TRY_TO_TIMESTAMP_NTZ(TRIM(order_purchase_timestamp)) IS NOT NULL
)
SELECT
    order_id,
    customer_id,
    TO_NUMBER(TO_CHAR(order_purchase_timestamp::DATE, 'YYYYMMDD')) AS purchase_date_key,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    CASE
        WHEN order_delivered_customer_date IS NOT NULL
         AND order_estimated_delivery_date IS NOT NULL
        THEN DATEDIFF('DAY', order_estimated_delivery_date::DATE, order_delivered_customer_date::DATE)
        ELSE NULL
    END AS delay_days,
    CASE
        WHEN order_delivered_customer_date IS NOT NULL
         AND order_estimated_delivery_date IS NOT NULL
         AND order_delivered_customer_date::DATE > order_estimated_delivery_date::DATE
        THEN TRUE
        ELSE FALSE
    END AS is_delayed,
    ingested_at
FROM cleaned_orders
WHERE rn = 1;

CREATE OR REPLACE TABLE FACT_ORDER_ITEMS AS
SELECT DISTINCT
    TRIM(order_id) AS order_id,
    TRY_TO_NUMBER(TRIM(order_item_id))::INTEGER AS order_item_id,
    TRIM(product_id) AS product_id,
    TRIM(seller_id) AS seller_id,
    TRY_TO_TIMESTAMP_NTZ(TRIM(shipping_limit_date)) AS shipping_limit_date,
    TRY_TO_DECIMAL(TRIM(price), 12, 2) AS price,
    TRY_TO_DECIMAL(TRIM(freight_value), 12, 2) AS freight_value
FROM BRONZE.ORDER_ITEMS
WHERE order_id IS NOT NULL
  AND TRIM(order_id) <> ''
  AND product_id IS NOT NULL
  AND TRIM(product_id) <> ''
  AND seller_id IS NOT NULL
  AND TRIM(seller_id) <> ''
  AND TRY_TO_DECIMAL(TRIM(price), 12, 2) > 0
  AND TRY_TO_DECIMAL(TRIM(freight_value), 12, 2) >= 0;

CREATE OR REPLACE TABLE FACT_PAYMENTS AS
SELECT DISTINCT
    TRIM(order_id) AS order_id,
    TRY_TO_NUMBER(TRIM(payment_sequential))::INTEGER AS payment_sequential,
    COALESCE(NULLIF(LOWER(TRIM(payment_type)), ''), 'unknown') AS payment_type,
    TRY_TO_NUMBER(TRIM(payment_installments))::INTEGER AS payment_installments,
    TRY_TO_DECIMAL(TRIM(payment_value), 12, 2) AS payment_value
FROM BRONZE.ORDER_PAYMENTS
WHERE order_id IS NOT NULL
  AND TRIM(order_id) <> ''
  AND TRY_TO_DECIMAL(TRIM(payment_value), 12, 2) > 0
  AND TRY_TO_NUMBER(TRIM(payment_installments))::INTEGER >= 0;

CREATE OR REPLACE TABLE FACT_REVIEWS AS
SELECT DISTINCT
    TRIM(review_id) AS review_id,
    TRIM(order_id) AS order_id,
    TRY_TO_NUMBER(TRIM(review_score))::INTEGER AS review_score,
    NULLIF(TRIM(review_comment_title), '') AS review_comment_title,
    NULLIF(TRIM(review_comment_message), '') AS review_comment_message,
    TRY_TO_TIMESTAMP_NTZ(TRIM(review_creation_date)) AS review_creation_date,
    TRY_TO_TIMESTAMP_NTZ(TRIM(review_answer_timestamp)) AS review_answer_timestamp
FROM BRONZE.ORDER_REVIEWS
WHERE review_id IS NOT NULL
  AND TRIM(review_id) <> ''
  AND order_id IS NOT NULL
  AND TRIM(order_id) <> ''
  AND TRY_TO_NUMBER(TRIM(review_score))::INTEGER BETWEEN 1 AND 5;

