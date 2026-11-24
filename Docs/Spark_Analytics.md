
# Spark Analytics

## Overview
The project uses Apache Spark with PySpark to process Olist datasets stored in HDFS and generate analytical outputs.

All analytical outputs are stored in Parquet format.

## Analytics Jobs

### 1. Delivery Performance
- Delivery duration
- Estimated vs actual delivery time
- Late orders and late percentage
- Monthly delivery performance
- Delivery performance by state

Output:
`/supply-chain/analytics/delivery_performance/`

### 2. Seller Performance
- Total orders per seller
- Items sold
- Total sales
- Average item price
- Total freight
- Delivered orders
- Late orders and late percentage
- Seller location

Output:
`/supply-chain/analytics/seller_performance/`

### 3. Product & Category Performance
- Items sold per product
- Product revenue
- Average product price
- Freight
- Category sales
- Number of unique products per category

Output:
`/supply-chain/analytics/product_category/`

### 4. Payment Analysis
- Transactions per payment type
- Number of orders per payment type
- Total and average payment
- Average installments
- Payment totals compared with item prices

Output:
`/supply-chain/analytics/payment_analysis/`

## Technology Stack
- Apache Spark
- PySpark
- Hadoop HDFS
- Docker
- Parquet

## Execution

Jobs are executed inside the Docker Spark environment.

Example:

```bash
docker compose -f docker-compose.hadoop.yml run --rm -e HADOOP_USER_NAME=hadoop spark-runner python delivery_performance.py
```

Replace the Python filename with the required analytics job.

## Storage

Raw and cleaned datasets are stored in HDFS.

Analytics outputs are stored under:

`/supply-chain/analytics/`

The outputs use Parquet columnar storage.
