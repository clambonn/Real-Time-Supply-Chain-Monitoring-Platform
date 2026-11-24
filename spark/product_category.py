
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct,
    sum as spark_sum, avg, round
)

spark = (
    SparkSession.builder
    .appName("ProductCategoryPerformance")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BASE = "hdfs://namenode:9000/supply-chain"
OUTPUT = f"{BASE}/analytics/product_category"

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


# Read datasets
items = spark.read.option("header", True).option(
    "inferSchema", True
).csv(find_file("olist_order_items_dataset.csv"))

products = spark.read.option("header", True).option(
    "inferSchema", True
).csv(find_file("olist_products_dataset.csv"))

translation = spark.read.option("header", True).option(
    "inferSchema", True
).csv(find_file("product_category_name_translation.csv"))

# Join product information
product_data = products.join(
    translation,
    "product_category_name",
    "left"
)

items_joined = items.join(
    product_data,
    "product_id",
    "left"
)

# Product-level metrics
product_metrics = items_joined.groupBy(
    "product_id",
    "product_category_name",
    "product_category_name_english"
).agg(
    count("order_id").alias("total_items_sold"),
    round(spark_sum("price"), 2).alias("total_sales"),
    round(spark_sum("freight_value"), 2).alias("total_freight"),
    round(avg("price"), 2).alias("avg_price")
)

# Category-level metrics
category_metrics = items_joined.groupBy(
    "product_category_name",
    "product_category_name_english"
).agg(
    count("order_id").alias("total_items_sold"),
    countDistinct("product_id").alias("unique_products"),
    round(spark_sum("price"), 2).alias("total_sales"),
    round(spark_sum("freight_value"), 2).alias("total_freight"),
    round(avg("price"), 2).alias("avg_price")
)

# Save Parquet outputs to HDFS
product_metrics.write.mode("overwrite").parquet(
    f"{OUTPUT}/products"
)

category_metrics.write.mode("overwrite").parquet(
    f"{OUTPUT}/categories"
)

print("Product & Category Performance Job completed.")
print(f"Products analyzed: {product_metrics.count():,}")
print(f"Categories analyzed: {category_metrics.count():,}")

print("Top 10 Categories by Sales")
category_metrics.orderBy(
    col("total_sales").desc()
).show(10, truncate=False)

print("Top 10 Products by Sales")
product_metrics.orderBy(
    col("total_sales").desc()
).show(10, truncate=False)

spark.stop()
