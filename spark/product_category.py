"""Gold job 3 - Product and category performance.

Inputs : Bronze items, products, product_category_name_translation
Outputs: analytics/product_category/{products,categories}  (Parquet)

Anti-duplication design
-----------------------
* items are aggregated per product_id FIRST (grain: product), and only then joined
  to the product attributes. products.product_id and translation.product_category_name
  are verified unique before joining, so sales/item counts cannot be multiplied.
* Category totals are the sum of product totals; the job asserts that the grand total
  of both outputs equals SUM(price) of the items input.
* Products without a category are reported as category 'unknown'. Categories missing
  from the translation table fall back to the Portuguese name (flagged
  has_translation = false) instead of NULL.
"""
from pyspark.sql import functions as F

import common as C

spark = C.get_spark("ProductCategoryPerformance")

items = C.read_dataset(spark, "items")
products = C.read_dataset(spark, "products")
translation = C.read_dataset(spark, "translation")

# Key uniqueness checks (fail the job instead of silently multiplying sales).
for name, df, key in [("products", products, "product_id"),
                      ("translation", translation, "product_category_name")]:
    dup = df.groupBy(key).count().filter("count > 1").count()
    if dup:
        raise ValueError(f"{name}.{key} has {dup} duplicated keys - cannot join safely")

product_attrs = (
    products.select("product_id", "product_category_name")
    .join(F.broadcast(translation), "product_category_name", "left")
    .withColumn("has_translation", F.col("product_category_name_english").isNotNull())
    .withColumn("product_category_name",
                F.coalesce("product_category_name", F.lit("unknown")))
    .withColumn("product_category_name_english",
                F.coalesce("product_category_name_english", F.col("product_category_name")))
)

product_sales = items.groupBy("product_id").agg(
    F.count("*").alias("total_items_sold"),
    F.countDistinct("order_id").alias("total_orders"),
    F.sum("price").alias("sales_raw"),
    F.sum("freight_value").alias("freight_raw"),
)

product_metrics = (
    product_sales.join(product_attrs, "product_id", "left")
    .fillna({"product_category_name": "unknown", "product_category_name_english": "unknown",
             "has_translation": False})
    .withColumn("total_sales", F.round("sales_raw", 2))
    .withColumn("total_freight", F.round("freight_raw", 2))
    .withColumn("avg_price", F.round(F.col("sales_raw") / F.col("total_items_sold"), 2))
    .cache()
)

products_out = product_metrics.select(
    "product_id", "product_category_name", "product_category_name_english", "has_translation",
    "total_items_sold", "total_orders", "total_sales", "total_freight", "avg_price",
)

categories_out = (
    product_metrics.groupBy("product_category_name", "product_category_name_english", "has_translation")
    .agg(
        F.sum("total_items_sold").alias("total_items_sold"),
        F.countDistinct("product_id").alias("unique_products"),
        F.round(F.sum("sales_raw"), 2).alias("total_sales"),
        F.round(F.sum("freight_raw"), 2).alias("total_freight"),
        F.round(F.sum("sales_raw") / F.sum("total_items_sold"), 2).alias("avg_price"),
    )
)

# Reconciliation: output totals must equal the item input totals.
src = items.agg(F.count("*").alias("n"), F.round(F.sum("price"), 2).alias("s")).first()
for label, df in [("products", products_out), ("categories", categories_out)]:
    out = df.agg(F.sum("total_items_sold").alias("n"), F.round(F.sum("total_sales"), 2).alias("s")).first()
    if out["n"] != src["n"] or abs(out["s"] - src["s"]) > 1.0:
        raise ValueError(f"{label} totals ({out['n']}, {out['s']}) do not match items ({src['n']}, {src['s']})")

C.write_parquet(products_out, C.GOLD["product_products"])
C.write_parquet(categories_out, C.GOLD["product_categories"])

print("\nProduct & Category job completed.")
print(f"Items: {src['n']:,}  Total sales: {src['s']:,.2f}")
print(f"Products: {products_out.count():,}  Categories: {categories_out.count():,}")
categories_out.orderBy(F.col("total_sales").desc()).show(10, truncate=False)
products_out.orderBy(F.col("total_sales").desc()).show(10, truncate=False)
spark.stop()
