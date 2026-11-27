"""Shared configuration, readers and helpers for all Spark jobs.

Every job imports this module so paths, schemas and business rules are defined
exactly once.

Path configuration
------------------
SUPPLY_CHAIN_BASE  Root of the data lake. Default: hdfs://namenode:9000/supply-chain
                   (inside the Docker network). Use file:///some/dir for local runs
                   without HDFS; the directory must contain raw/ and cleaned/.
"""
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

BASE = os.environ.get("SUPPLY_CHAIN_BASE", "hdfs://namenode:9000/supply-chain").rstrip("/")
RAW = f"{BASE}/raw"          # Bronze
CLEANED = f"{BASE}/cleaned"  # Silver
ANALYTICS = f"{BASE}/analytics"  # Gold
REPORT_DIR = os.environ.get("SPARK_REPORT_DIR", "reports")  # local dir for QA reports

# Fixed, explicit file locations (no recursive searching).
# Silver only exists for orders, payments and reviews; the other datasets were
# not changed by the cleaning phase, so Bronze is their authoritative source.
SOURCES = {
    "orders":      f"{CLEANED}/olist_orders_cleaned.csv",
    "payments":    f"{CLEANED}/olist_order_payments_cleaned.csv",
    "reviews":     f"{CLEANED}/olist_order_reviews_cleaned.csv",
    "items":       f"{RAW}/olist_order_items_dataset.csv",
    "customers":   f"{RAW}/olist_customers_dataset.csv",
    "sellers":     f"{RAW}/olist_sellers_dataset.csv",
    "products":    f"{RAW}/olist_products_dataset.csv",
    "translation": f"{RAW}/product_category_name_translation.csv",
    "geolocation": f"{RAW}/olist_geolocation_dataset.csv",
}

GOLD = {
    "delivery_detail":  f"{ANALYTICS}/delivery_performance/detail",
    "delivery_monthly": f"{ANALYTICS}/delivery_performance/monthly",
    "delivery_state":   f"{ANALYTICS}/delivery_performance/state",
    "seller_summary":   f"{ANALYTICS}/seller_performance/summary",
    "seller_detail":    f"{ANALYTICS}/seller_performance/detail",
    "product_products":   f"{ANALYTICS}/product_category/products",
    "product_categories": f"{ANALYTICS}/product_category/categories",
    "payment_type":       f"{ANALYTICS}/payment_analysis/payment_type",
    "payment_order":      f"{ANALYTICS}/payment_analysis/order_comparison",
}

# All source columns are read as strings and converted explicitly, so that a
# malformed value becomes NULL (and is counted by validation) instead of making
# Spark guess a wrong type with inferSchema.
COLUMNS = {
    "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
               "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"],
    "payments": ["order_id", "payment_sequential", "payment_type",
                 "payment_installments", "payment_value"],
    "reviews": ["review_id", "order_id", "review_score", "review_comment_title",
                "review_comment_message", "review_creation_date", "review_answer_timestamp"],
    "items": ["order_id", "order_item_id", "product_id", "seller_id",
              "shipping_limit_date", "price", "freight_value"],
    "customers": ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                  "customer_city", "customer_state"],
    "sellers": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    "products": ["product_id", "product_category_name", "product_name_lenght",
                 "product_description_lenght", "product_photos_qty", "product_weight_g",
                 "product_length_cm", "product_height_cm", "product_width_cm"],
    "translation": ["product_category_name", "product_category_name_english"],
    "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
                    "geolocation_city", "geolocation_state"],
}
TIMESTAMP_COLS = {
    "orders": ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"],
    "reviews": ["review_creation_date", "review_answer_timestamp"],
    "items": ["shipping_limit_date"],
}
DOUBLE_COLS = {
    "payments": ["payment_value"],
    "items": ["price", "freight_value"],
    "geolocation": ["geolocation_lat", "geolocation_lng"],
    "products": ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"],
}
INT_COLS = {
    "payments": ["payment_sequential", "payment_installments"],
    "reviews": ["review_score"],
    "items": ["order_item_id"],
    "products": ["product_name_lenght", "product_description_lenght", "product_photos_qty"],
}
# Datasets that contain quoted multi-line free text (review comments).
MULTILINE = {"reviews"}


def get_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")  # small dataset: avoid 200 tiny tasks
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw_strings(spark: SparkSession, name: str) -> DataFrame:
    """Read a source CSV with every column as a string (used by validation)."""
    schema = StructType([StructField(c, StringType(), True) for c in COLUMNS[name]])
    reader = (spark.read.schema(schema).option("header", True)
              .option("mode", "PERMISSIVE")
              .option("encoding", "UTF-8")
              .option("quote", '"').option("escape", '"')
              .option("emptyValue", "")
              .option("multiLine", name in MULTILINE))
    df = reader.csv(SOURCES[name])
    return df  # schema is positional, so header names (e.g. a UTF-8 BOM) cannot break it


def read_dataset(spark: SparkSession, name: str) -> DataFrame:
    """Read a source dataset with explicit types. Unparseable values become NULL."""
    df = read_raw_strings(spark, name)
    for c in df.columns:  # empty strings are missing values
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))))
    for c in TIMESTAMP_COLS.get(name, []):
        df = df.withColumn(c, F.coalesce(
            F.try_to_timestamp(F.col(c), F.lit("yyyy-MM-dd HH:mm:ss")),
            F.try_to_timestamp(F.col(c), F.lit("yyyy-MM-dd"))))
    for c in DOUBLE_COLS.get(name, []):
        df = df.withColumn(c, F.col(c).try_cast("double"))
    for c in INT_COLS.get(name, []):
        df = df.withColumn(c, F.col(c).try_cast("int"))
    return df


def day_diff(later, earlier):
    """Whole calendar days between two timestamp columns (date part only)."""
    return F.datediff(F.to_date(later), F.to_date(earlier))


def with_delivery_flags(orders: DataFrame) -> DataFrame:
    """Add the single, shared delivery business rules (used by delivery + seller jobs).

    evaluable_delivery  : order was delivered (status 'delivered'), has an actual delivery
                          date AND an estimated date. Only these orders can be judged
                          on time / late. Orders with missing dates are NOT on time.
    delay_days          : calendar days between delivery and estimate (>0 = late).
    is_late             : 1 / 0 for evaluable orders, NULL when it cannot be determined.
    Date comparison is at calendar-day granularity because the estimated delivery date
    carries no time of day in the source (a delivery any time on the estimated day is on time).
    """
    delivered = F.col("order_delivered_customer_date")
    estimated = F.col("order_estimated_delivery_date")
    evaluable = (F.col("order_status") == "delivered") & delivered.isNotNull() & estimated.isNotNull()
    return (
        orders
        .withColumn("evaluable_delivery", evaluable)
        .withColumn("delay_days", F.when(evaluable, day_diff(delivered, estimated)))
        .withColumn("is_late", F.when(evaluable, F.when(F.col("delay_days") > 0, 1).otherwise(0)))
    )


def write_parquet(df: DataFrame, path: str) -> None:
    """Overwrite a Gold dataset. Callers only invoke this after all inputs were read OK."""
    df.write.mode("overwrite").parquet(path)


def read_gold(spark: SparkSession, key: str) -> DataFrame:
    return spark.read.parquet(GOLD[key])


def hadoop_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(p)
