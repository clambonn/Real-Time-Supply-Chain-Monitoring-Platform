
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    avg, round, when
)

spark = (
    SparkSession.builder
    .appName("SellerPerformance")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BASE = "hdfs://namenode:9000/supply-chain"
OUTPUT = f"{BASE}/analytics/seller_performance"

# Locate datasets in HDFS
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

    raise FileNotFoundError(filename)

orders = spark.read.option("header", True).option(
    "inferSchema", True
).csv(find_file("olist_orders_cleaned.csv"))

items = spark.read.option("header", True).option(
    "inferSchema", True
).csv(find_file("olist_order_items_dataset.csv"))

sellers = spark.read.option("header", True).option(
    "inferSchema", True
).csv(find_file("olist_sellers_dataset.csv"))

# Join order items with orders
joined = items.join(
    orders.select(
        "order_id",
        "order_status",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ),
    "order_id",
    "left"
)

# Calculate late delivery
joined = joined.withColumn(
    "is_late",
    when(
        col("order_delivered_customer_date").isNotNull()
        & (col("order_delivered_customer_date")
           > col("order_estimated_delivery_date")),
        1
    ).otherwise(0)
)

# Seller-level aggregation
seller_metrics = joined.groupBy("seller_id").agg(
    countDistinct("order_id").alias("total_orders"),
    count("order_item_id").alias("total_items_sold"),
    round(spark_sum("price"), 2).alias("total_sales"),
    round(avg("price"), 2).alias("avg_item_price"),
    round(spark_sum("freight_value"), 2).alias("total_freight"),
    countDistinct(
        when(
            col("order_delivered_customer_date").isNotNull(),
            col("order_id")
        )
    ).alias("delivered_orders"),
    countDistinct(
        when(col("is_late") == 1, col("order_id"))
    ).alias("late_orders")
)

seller_metrics = seller_metrics.withColumn(
    "late_percentage",
    round(
        when(
            col("delivered_orders") > 0,
            col("late_orders") / col("delivered_orders") * 100
        ),
        2
    )
)

# Add seller location
seller_metrics = seller_metrics.join(
    sellers.select(
        "seller_id",
        "seller_state",
        "seller_city"
    ),
    "seller_id",
    "left"
)

# Detailed item-level data
detail = joined.select(
    "order_id",
    "order_item_id",
    "seller_id",
    "price",
    "freight_value",
    "order_status",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "is_late"
)

# Save results
detail.write.mode("overwrite").parquet(f"{OUTPUT}/detail")
seller_metrics.write.mode("overwrite").parquet(f"{OUTPUT}/summary")

print("Seller Performance Job completed.")
print(f"Sellers analyzed: {seller_metrics.count():,}")

seller_metrics.orderBy(
    col("total_sales").desc()
).show(20, truncate=False)

spark.stop()
