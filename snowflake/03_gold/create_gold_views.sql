USE DATABASE SUPPLY_CHAIN_DWH;
USE SCHEMA GOLD;

CREATE OR REPLACE VIEW VW_DELIVERY_PERFORMANCE_MONTHLY AS
SELECT
    YEAR(o.order_purchase_timestamp) AS order_year,
    MONTH(o.order_purchase_timestamp) AS order_month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN o.order_id END) AS delivered_orders,
    AVG(
        CASE
            WHEN o.order_delivered_customer_date IS NOT NULL
            THEN DATEDIFF('DAY', o.order_purchase_timestamp::DATE, o.order_delivered_customer_date::DATE)
        END
    )::DECIMAL(10, 2) AS avg_delivery_days,
    AVG(
        CASE
            WHEN o.order_delivered_customer_date IS NOT NULL
             AND o.order_estimated_delivery_date IS NOT NULL
            THEN DATEDIFF('DAY', o.order_purchase_timestamp::DATE, o.order_estimated_delivery_date::DATE)
        END
    )::DECIMAL(10, 2) AS avg_estimated_days,
    COUNT(DISTINCT CASE WHEN o.is_delayed = TRUE THEN o.order_id END) AS late_orders,
    (
        COUNT(DISTINCT CASE WHEN o.is_delayed = TRUE THEN o.order_id END)::DECIMAL(12, 2)
        / NULLIF(COUNT(DISTINCT CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN o.order_id END), 0)
        * 100
    )::DECIMAL(10, 2) AS late_percentage,
    AVG(
        CASE
            WHEN o.is_delayed = TRUE THEN o.delay_days
        END
    )::DECIMAL(10, 2) AS avg_delay_days
FROM SILVER.FACT_ORDERS o
GROUP BY
    YEAR(o.order_purchase_timestamp),
    MONTH(o.order_purchase_timestamp);

CREATE OR REPLACE VIEW VW_DELIVERY_PERFORMANCE_STATE AS
SELECT
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN o.order_id END) AS delivered_orders,
    AVG(
        CASE
            WHEN o.order_delivered_customer_date IS NOT NULL
            THEN DATEDIFF('DAY', o.order_purchase_timestamp::DATE, o.order_delivered_customer_date::DATE)
        END
    )::DECIMAL(10, 2) AS avg_delivery_days,
    COUNT(DISTINCT CASE WHEN o.is_delayed = TRUE THEN o.order_id END) AS late_orders,
    (
        COUNT(DISTINCT CASE WHEN o.is_delayed = TRUE THEN o.order_id END)::DECIMAL(12, 2)
        / NULLIF(COUNT(DISTINCT CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN o.order_id END), 0)
        * 100
    )::DECIMAL(10, 2) AS late_percentage,
    AVG(
        CASE
            WHEN o.is_delayed = TRUE THEN o.delay_days
        END
    )::DECIMAL(10, 2) AS avg_delay_days
FROM SILVER.FACT_ORDERS o
LEFT JOIN SILVER.DIM_CUSTOMERS c
    ON o.customer_id = c.customer_id
GROUP BY
    c.customer_state;

CREATE OR REPLACE VIEW VW_SELLER_PERFORMANCE AS
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(*) AS total_items_sold,
    SUM(oi.price)::DECIMAL(14, 2) AS total_sales,
    AVG(oi.price)::DECIMAL(12, 2) AS avg_item_price,
    SUM(oi.freight_value)::DECIMAL(14, 2) AS total_freight,
    COUNT(DISTINCT CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN o.order_id END) AS delivered_orders,
    COUNT(DISTINCT CASE WHEN o.is_delayed = TRUE THEN o.order_id END) AS late_orders,
    (
        COUNT(DISTINCT CASE WHEN o.is_delayed = TRUE THEN o.order_id END)::DECIMAL(12, 2)
        / NULLIF(COUNT(DISTINCT CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN o.order_id END), 0)
        * 100
    )::DECIMAL(10, 2) AS late_percentage
FROM SILVER.FACT_ORDER_ITEMS oi
LEFT JOIN SILVER.DIM_SELLERS s
    ON oi.seller_id = s.seller_id
LEFT JOIN SILVER.FACT_ORDERS o
    ON oi.order_id = o.order_id
GROUP BY
    s.seller_id,
    s.seller_city,
    s.seller_state;

CREATE OR REPLACE VIEW VW_PRODUCT_CATEGORY_PERFORMANCE AS
SELECT
    p.product_category_name_english,
    p.product_category_name,
    COUNT(*) AS total_items_sold,
    COUNT(DISTINCT oi.product_id) AS unique_products,
    SUM(oi.price)::DECIMAL(14, 2) AS total_sales,
    SUM(oi.freight_value)::DECIMAL(14, 2) AS total_freight,
    AVG(oi.price)::DECIMAL(12, 2) AS avg_price
FROM SILVER.FACT_ORDER_ITEMS oi
LEFT JOIN SILVER.DIM_PRODUCTS p
    ON oi.product_id = p.product_id
GROUP BY
    p.product_category_name_english,
    p.product_category_name;

CREATE OR REPLACE VIEW VW_PAYMENT_ANALYSIS AS
WITH order_payments AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment
    FROM SILVER.FACT_PAYMENTS
    GROUP BY order_id
),
order_items AS (
    SELECT
        order_id,
        SUM(COALESCE(price, 0) + COALESCE(freight_value, 0)) AS total_item_price
    FROM SILVER.FACT_ORDER_ITEMS
    GROUP BY order_id
),
order_payment_diff AS (
    SELECT
        op.order_id,
        (COALESCE(op.total_payment, 0) - COALESCE(oi.total_item_price, 0)) AS order_diff
    FROM order_payments op
    LEFT JOIN order_items oi
        ON op.order_id = oi.order_id
),
payment_type_order_diff AS (
    SELECT
        p.payment_type,
        p.order_id,
        MAX(opd.order_diff) AS order_diff
    FROM SILVER.FACT_PAYMENTS p
    LEFT JOIN order_payment_diff opd
        ON p.order_id = opd.order_id
    GROUP BY
        p.payment_type,
        p.order_id
)
SELECT
    p.payment_type,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT p.order_id) AS total_orders,
    SUM(p.payment_value)::DECIMAL(14, 2) AS total_payment,
    AVG(p.payment_value)::DECIMAL(12, 2) AS avg_payment,
    AVG(p.payment_installments)::DECIMAL(10, 2) AS avg_installments,
    COALESCE(ptod.type_payment_difference, 0)::DECIMAL(14, 2) AS payment_difference
FROM SILVER.FACT_PAYMENTS p
LEFT JOIN (
    SELECT
        payment_type,
        SUM(order_diff) AS type_payment_difference
    FROM payment_type_order_diff
    GROUP BY payment_type
) ptod
    ON p.payment_type = ptod.payment_type
GROUP BY
    p.payment_type,
    ptod.type_payment_difference;

CREATE OR REPLACE VIEW VW_CALENDAR_ORDER_TREND AS
SELECT
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.quarter,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN o.order_id END) AS delivered_orders,
    COUNT(DISTINCT CASE WHEN o.is_delayed = TRUE THEN o.order_id END) AS late_orders
FROM SILVER.DIM_DATE d
LEFT JOIN SILVER.FACT_ORDERS o
    ON d.full_date = o.order_purchase_timestamp::DATE
GROUP BY
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.quarter;

CREATE OR REPLACE VIEW VW_GEO_DISTRIBUTION AS
WITH geo_summary AS (
    SELECT
        TRIM(geolocation_zip_code_prefix) AS zip_code_prefix,
        AVG(TRY_TO_DECIMAL(TRIM(geolocation_lat), 12, 6)) AS avg_lat,
        AVG(TRY_TO_DECIMAL(TRIM(geolocation_lng), 12, 6)) AS avg_lng
    FROM BRONZE.GEOLOCATION
    GROUP BY
        TRIM(geolocation_zip_code_prefix)
)
SELECT
    c.customer_state,
    c.customer_city,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    AVG(g.avg_lat)::DECIMAL(12, 6) AS avg_latitude,
    AVG(g.avg_lng)::DECIMAL(12, 6) AS avg_longitude
FROM SILVER.DIM_CUSTOMERS c
LEFT JOIN geo_summary g
    ON TRIM(c.customer_zip_code_prefix) = g.zip_code_prefix
GROUP BY
    c.customer_state,
    c.customer_city;

CREATE OR REPLACE VIEW VW_REVIEW_PERFORMANCE AS
SELECT
    r.review_score,
    CASE
        WHEN o.order_delivered_customer_date IS NULL THEN 'undelivered'
        WHEN o.is_delayed = TRUE THEN 'delayed'
        ELSE 'on_time'
    END AS delivery_status,
    COUNT(DISTINCT r.review_id) AS total_reviews,
    COUNT(DISTINCT r.order_id) AS reviewed_orders,
    AVG(r.review_score)::DECIMAL(10, 2) AS avg_review_score,
    AVG(
        CASE
            WHEN o.order_delivered_customer_date IS NOT NULL
            THEN DATEDIFF('DAY', o.order_purchase_timestamp::DATE, o.order_delivered_customer_date::DATE)
        END
    )::DECIMAL(10, 2) AS avg_delivery_days
FROM SILVER.FACT_REVIEWS r
LEFT JOIN SILVER.FACT_ORDERS o
    ON r.order_id = o.order_id
GROUP BY
    r.review_score,
    CASE
        WHEN o.order_delivered_customer_date IS NULL THEN 'undelivered'
        WHEN o.is_delayed = TRUE THEN 'delayed'
        ELSE 'on_time'
    END;