USE DATABASE SUPPLY_CHAIN_DWH;
USE SCHEMA BRONZE;

COPY INTO CUSTOMERS (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
FROM @RAW_STAGE/olist_customers_dataset.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO ORDER_ITEMS (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
)
FROM @RAW_STAGE/olist_order_items_dataset.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO ORDER_PAYMENTS (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
)
FROM @RAW_STAGE/olist_order_payments_dataset.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO ORDER_REVIEWS (
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
)
FROM @RAW_STAGE/olist_order_reviews_dataset.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO PRODUCTS (
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
FROM @RAW_STAGE/olist_products_dataset.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO SELLERS (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
FROM @RAW_STAGE/olist_sellers_dataset.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO GEOLOCATION (
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
)
FROM @RAW_STAGE/olist_geolocation_dataset.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO PRODUCT_CATEGORY_NAME_TRANSLATION (
    product_category_name,
    product_category_name_english
)
FROM @RAW_STAGE/product_category_name_translation.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO STG_ORDERS (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8
    FROM @RAW_STAGE/olist_orders_dataset.csv
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';