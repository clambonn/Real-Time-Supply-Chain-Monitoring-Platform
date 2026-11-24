from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when


# =========================
# 1. Create Spark Session
# =========================

spark = (
    SparkSession.builder
    .appName("SupplyChainDataValidation")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================
# 2. Dataset paths
# =========================

BASE_PATH = "hdfs://namenode:9000/supply-chain/cleaned"

datasets = {
    "orders": f"{BASE_PATH}/olist_orders_cleaned.csv",
    "payments": f"{BASE_PATH}/olist_order_payments_cleaned.csv",
    "reviews": f"{BASE_PATH}/olist_order_reviews_cleaned.csv",
}


# =========================
# 3. Read CSV datasets
# =========================

dataframes = {}

for name, path in datasets.items():

    print(f"\n{'=' * 60}")
    print(f"Reading dataset: {name}")
    print(f"{'=' * 60}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

    dataframes[name] = df

    print(f"Dataset: {name}")
    print(f"Rows: {df.count():,}")
    print(f"Columns: {len(df.columns)}")

    print("\nSchema:")
    df.printSchema()

    print("\nSample:")
    df.show(5, truncate=False)


# =========================
# 4. Validation
# =========================

validation_results = []

for name, df in dataframes.items():

    row_count = df.count()
    column_count = len(df.columns)

    # Total null values across the dataset
    null_counts = df.select([
        count(when(col(c).isNull(), 1)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()

    total_nulls = sum(null_counts.values())

    # Duplicate checks for dataset primary key
    key_map = {
        "orders": "order_id",
        "payments": None,
        "reviews": "review_id",
    }

    key = key_map[name]

    if key and key in df.columns:

        duplicate_count = (
            df.groupBy(key)
            .count()
            .filter(col("count") > 1)
            .count()
        )

    else:
        duplicate_count = "N/A"

    result = {
        "dataset": name,
        "rows": row_count,
        "columns": column_count,
        "total_nulls": total_nulls,
        "duplicate_keys": duplicate_count,
    }

    validation_results.append(result)

    print(f"\n{'=' * 60}")
    print(f"VALIDATION: {name}")
    print(f"{'=' * 60}")

    print(f"Rows: {row_count:,}")
    print(f"Columns: {column_count}")
    print(f"Total nulls: {total_nulls:,}")
    print(f"Duplicate keys: {duplicate_count}")

    print("\nNulls per column:")

    for column_name, null_count in null_counts.items():

        if null_count > 0:
            print(f"{column_name}: {null_count:,}")


# =========================
# 5. Summary report
# =========================

print("\n\n" + "=" * 70)
print("FINAL VALIDATION SUMMARY")
print("=" * 70)

for result in validation_results:

    print(
        f"{result['dataset']:10} | "
        f"Rows: {result['rows']:>10,} | "
        f"Columns: {result['columns']:>3} | "
        f"Nulls: {result['total_nulls']:>8,} | "
        f"Duplicates: {result['duplicate_keys']}"
    )


# =========================
# 6. Stop Spark
# =========================

spark.stop()

print("\nSpark validation completed successfully.")