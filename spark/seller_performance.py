"""Gold job 2 - Seller performance.

Inputs : Silver orders, Bronze items, Bronze sellers
Outputs: analytics/seller_performance/{summary,detail}  (Parquet)

Grain of `detail`: one row per order item (order_id, order_item_id).
Grain of `summary`: one row per seller_id.
Orders are unique (validated), so joining items -> orders cannot multiply rows.
Sales metrics cover all order items; delivery metrics use only evaluable orders
(delivered + actual date + estimated date), with the same rule as delivery_performance.
late_percentage = late_orders / delivered_orders * 100 (NULL if no delivered order).
"""
from pyspark.sql import functions as F

import common as C

spark = C.get_spark("SellerPerformance")

orders = C.read_dataset(spark, "orders")
items = C.read_dataset(spark, "items")
sellers = C.read_dataset(spark, "sellers")

for name, df, key in [("orders", orders, "order_id"), ("sellers", sellers, "seller_id")]:
    dup = df.groupBy(key).count().filter("count > 1").count()
    if dup:
        raise ValueError(f"{name}.{key} has {dup} duplicated keys - run read_and_validate.py")

order_flags = C.with_delivery_flags(orders).select(
    "order_id", "order_status", "order_delivered_customer_date",
    "order_estimated_delivery_date", "evaluable_delivery", "is_late",
)

joined = items.join(order_flags, "order_id", "left").cache()

summary = (
    joined.groupBy("seller_id").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.count("*").alias("total_items_sold"),
        F.round(F.sum("price"), 2).alias("total_sales"),
        F.round(F.avg("price"), 2).alias("avg_item_price"),
        F.round(F.sum("freight_value"), 2).alias("total_freight"),
        F.countDistinct(F.when(F.col("evaluable_delivery"), F.col("order_id"))).alias("delivered_orders"),
        F.countDistinct(F.when(F.col("is_late") == 1, F.col("order_id"))).alias("late_orders"),
    )
    .withColumn("late_percentage",
                F.round(F.when(F.col("delivered_orders") > 0,
                               F.col("late_orders") / F.col("delivered_orders") * 100), 2))
    .join(sellers.select("seller_id", "seller_state", "seller_city"), "seller_id", "left")
)

detail = joined.select(
    "order_id", "order_item_id", "seller_id", "price", "freight_value", "order_status",
    "order_delivered_customer_date", "order_estimated_delivery_date", "is_late",
)

C.write_parquet(detail, C.GOLD["seller_detail"])
C.write_parquet(summary, C.GOLD["seller_summary"])

print("\nSeller Performance job completed.")
print(f"Order items: {joined.count():,}   Sellers: {summary.count():,}")
summary.orderBy(F.col("total_sales").desc()).show(20, truncate=False)
spark.stop()
