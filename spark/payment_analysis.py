
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    avg, round
)

spark = (
    SparkSession.builder
    .appName("PaymentAnalysis")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BASE = "hdfs://namenode:9000/supply-chain"
OUTPUT = f"{BASE}/analytics/payment_analysis"

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
payments = spark.read.option("header", True).option(
    "inferSchema", True
).csv(find_file("olist_order_payments_cleaned.csv"))

items = spark.read.option("header", True).option(
    "inferSchema", True
).csv(find_file("olist_order_items_dataset.csv"))

# Payment type metrics
payment_metrics = payments.groupBy(
    "payment_type"
).agg(
    count("*").alias("total_transactions"),
    countDistinct("order_id").alias("total_orders"),
    round(spark_sum("payment_value"), 2).alias("total_payment"),
    round(avg("payment_value"), 2).alias("avg_payment"),
    round(avg("payment_installments"), 2).alias("avg_installments")
)

# Payment total per order
order_payments = payments.groupBy("order_id").agg(
    round(spark_sum("payment_value"), 2).alias("total_payment")
)

# Product total per order
order_items = items.groupBy("order_id").agg(
    round(spark_sum("price"), 2).alias("total_item_price"),
    round(spark_sum("freight_value"), 2).alias("total_freight")
)

# Compare payments with item prices
order_comparison = order_payments.join(
    order_items,
    "order_id",
    "inner"
).withColumn(
    "payment_difference",
    round(col("total_payment") - col("total_item_price"), 2)
)

# Save Parquet outputs
payment_metrics.write.mode("overwrite").parquet(
    f"{OUTPUT}/payment_type"
)

order_comparison.write.mode("overwrite").parquet(
    f"{OUTPUT}/order_comparison"
)

print("Payment Analysis Job completed.")
print(f"Payment types analyzed: {payment_metrics.count()}")
print(f"Orders compared: {order_comparison.count():,}")

print("Payment Summary")
payment_metrics.orderBy(
    col("total_payment").desc()
).show(truncate=False)

print("Payment vs Item Price")
order_comparison.show(10, truncate=False)

spark.stop()
