# Snowflake

Snowflake SQL scripts for `SUPPLY_CHAIN_DWH` structured around a Medallion architecture.

## Execution Order

1. `setup/setup.sql`
2. Upload CSV files to `@BRONZE.RAW_STAGE`
3. `01_bronze/ddl_bronze_tables.sql`
4. `01_bronze/load_bronze_batch.sql`
5. `02_silver/ddl_silver_tables.sql`
6. `02_silver/transform_silver.sql`
7. `03_gold/create_gold_views.sql`

## Stage Files

- `olist_customers_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`
- `olist_orders_dataset.csv`