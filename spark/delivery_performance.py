from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum,
    when, datediff, round, date_format
)

spark = (
    SparkSession.builder
    .appName("DeliveryPerformance")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BASE = "hdfs://namenode:9000"
OUTPUT = f"{BASE}/supply-chain/analytics/delivery_performance"

# Find files recursively in HDFS
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)
Path = spark._jvm.org.apache.hadoop.fs.Path

def find_file(filename):
    for root in ["/supply-chain/cleaned", "/supply-chain/raw"]:
        root_path = Path(root)

        if not fs.exists(root_path):
            continue

        stack = [root_path]

        while stack:
            current = stack.pop()
            for item in fs.listStatus(current):
                if item.isDirectory():
                    stack.append(item.getPath())
                elif item.getPath().getName() == filename:
                    return item.getPath().toString()

    raise FileNotFoundError(f"File not found in HDFS: {filename}")

orders_path = find_file("olist_orders_cleaned.csv")
items_path = find_file("olist_order_items_dataset.csv")
customers_path = find_file("olist_customers_dataset.csv")

print("Reading HDFS datasets...")

orders = spark.read.option("header", True).option(
    "inferSchema", True
).csv(orders_path)

items = spark.read.option("header", True).option(
    "inferSchema", True
).csv(items_path)

customers = spark.read.option("header", True).option(
    "inferSchema", True
).csv(customers_path)

# Aggregate order items before joining
items_agg = (
    items.groupBy("order_id")
    .agg(
        count("order_item_id").alias("total_items"),
        round(spark_sum("price"), 2).alias("order_value"),
        round(spark_sum("freight_value"), 2).alias("freight_value")
    )
)

# Join datasets
delivery = (
    orders.join(items_agg, "order_id", "left")
    .join(
        customers.select("customer_id", "customer_state"),
        "customer_id",
        "left"
    )
)

# Keep orders with actual delivery dates
delivery = delivery.filter(
    col("order_delivered_customer_date").isNotNull()
)

# Calculate delivery metrics
delivery = (
    delivery
    .withColumn(
        "actual_delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    )
    .withColumn(
        "estimated_delivery_days",
        datediff(
            col("order_estimated_delivery_date"),
            col("order_purchase_timestamp")
        )
    )
    .withColumn(
        "delay_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date")
        )
    )
    .withColumn(
        "is_late",
        when(col("delay_days") > 0, 1).otherwise(0)
    )
    .withColumn(
        "purchase_month",
        date_format(col("order_purchase_timestamp"), "yyyy-MM")
    )
)

# 1. Detailed delivery dataset
detail = delivery.select(
    "order_id",
    "customer_id",
    "customer_state",
    "purchase_month",
    "total_items",
    "order_value",
    "freight_value",
    "actual_delivery_days",
    "estimated_delivery_days",
    "delay_days",
    "is_late"
)

# 2. Monthly performance
monthly = (
    delivery.groupBy("purchase_month")
    .agg(
        count("order_id").alias("total_orders"),
        round(avg("actual_delivery_days"), 2).alias("avg_delivery_days"),
        round(avg("estimated_delivery_days"), 2).alias("avg_estimated_days"),
        spark_sum("is_late").alias("late_orders"),
        round(avg("is_late") * 100, 2).alias("late_percentage"),
        round(avg("delay_days"), 2).alias("avg_delay_days")
    )
    .orderBy("purchase_month")
)

# 3. State performance
state = (
    delivery.groupBy("customer_state")
    .agg(
        count("order_id").alias("total_orders"),
        round(avg("actual_delivery_days"), 2).alias("avg_delivery_days"),
        spark_sum("is_late").alias("late_orders"),
        round(avg("is_late") * 100, 2).alias("late_percentage"),
        round(avg("delay_days"), 2).alias("avg_delay_days")
    )
    .orderBy(col("late_percentage").desc())
)

# Save outputs as Parquet
detail.write.mode("overwrite").parquet(f"{OUTPUT}/detail")
monthly.write.mode("overwrite").parquet(f"{OUTPUT}/monthly")
state.write.mode("overwrite").parquet(f"{OUTPUT}/state")

print("\nDelivery Performance Job completed.")
print(f"Delivered orders analyzed: {delivery.count():,}")

print("\nMonthly summary:")
monthly.show()

print("\nState summary:")
state.show()

spark.stop()