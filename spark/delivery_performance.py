"""Gold job 1 - Delivery performance.

Inputs : Silver orders (olist_orders_cleaned.csv), Bronze items, Bronze customers
Outputs: analytics/delivery_performance/{detail,monthly,state}  (Parquet)

Grain of `detail`: one row per order (order_id unique). Items are aggregated to
order level BEFORE joining, so the join cannot multiply rows.
Only orders that can be evaluated (delivered + actual date + estimated date) are
included. Orders with missing dates are never counted as on time; the number
excluded is printed. Date anomalies are flagged (has_date_anomaly), never dropped.
"""
from pyspark.sql import functions as F

import common as C

spark = C.get_spark("DeliveryPerformance")

orders = C.read_dataset(spark, "orders")
items = C.read_dataset(spark, "items")
customers = C.read_dataset(spark, "customers")

# Fail early on an invalid key: a duplicated key would silently multiply rows in joins.
for name, df, key in [("orders", orders, "order_id"), ("customers", customers, "customer_id")]:
    dup = df.groupBy(key).count().filter("count > 1").count()
    if dup:
        raise ValueError(f"{name}.{key} has {dup} duplicated keys - run read_and_validate.py")

items_agg = items.groupBy("order_id").agg(
    F.count("*").alias("total_items"),
    F.round(F.sum("price"), 2).alias("order_value"),
    F.round(F.sum("freight_value"), 2).alias("freight_value"),
)

flagged = (
    C.with_delivery_flags(orders)
    .join(items_agg, "order_id", "left")
    .join(customers.select("customer_id", "customer_state"), "customer_id", "left")
)

delivery = (
    flagged.filter("evaluable_delivery")
    .withColumn("actual_delivery_days",
                C.day_diff(F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp")))
    .withColumn("estimated_delivery_days",
                C.day_diff(F.col("order_estimated_delivery_date"), F.col("order_purchase_timestamp")))
    .withColumn("purchase_month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
    # Anomaly = customer delivery before carrier pick-up, or carrier pick-up before approval.
    .withColumn("has_date_anomaly",
                (F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date"))
                | (F.col("order_delivered_carrier_date") < F.col("order_approved_at")))
    .fillna({"total_items": 0, "order_value": 0.0, "freight_value": 0.0})
    .withColumn("has_date_anomaly", F.coalesce("has_date_anomaly", F.lit(False)))
    .cache()
)

detail = delivery.select(
    "order_id", "customer_id", "customer_state", "purchase_month", "total_items",
    "order_value", "freight_value", "actual_delivery_days", "estimated_delivery_days",
    "delay_days", "is_late", "has_date_anomaly",
)


def summarise(df, key):
    return df.groupBy(key).agg(
        F.count("*").alias("total_orders"),
        F.round(F.avg("actual_delivery_days"), 2).alias("avg_delivery_days"),
        F.round(F.avg("estimated_delivery_days"), 2).alias("avg_estimated_days"),
        F.sum("is_late").alias("late_orders"),
        F.round(F.avg("is_late") * 100, 2).alias("late_percentage"),
        F.round(F.avg("delay_days"), 2).alias("avg_delay_days"),
    )


monthly = summarise(delivery, "purchase_month").orderBy("purchase_month")
state = summarise(delivery.filter(F.col("customer_state").isNotNull()), "customer_state") \
    .orderBy(F.col("late_percentage").desc())

total_orders = orders.count()
analysed = delivery.count()
C.write_parquet(detail, C.GOLD["delivery_detail"])
C.write_parquet(monthly, C.GOLD["delivery_monthly"])
C.write_parquet(state, C.GOLD["delivery_state"])

print("\nDelivery Performance job completed.")
print(f"Orders in Silver: {total_orders:,}")
print(f"Orders analysed (delivered with actual + estimated date): {analysed:,}")
print(f"Orders excluded (not delivered / missing dates): {total_orders - analysed:,}")
monthly.show(30)
state.show(30)
spark.stop()
