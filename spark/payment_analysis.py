"""Gold job 4 - Payment analysis.

Inputs : Silver payments, Bronze items
Outputs: analytics/payment_analysis/{payment_type,order_comparison}  (Parquet)

payment_type    : one row per payment_type (transactions, orders, totals, installments).
order_comparison: one row per order_id (payments aggregated to order level, items
                  aggregated to order level, then FULL OUTER joined so orders that exist
                  on only one side are kept and flagged rather than dropped).

Chosen reconciliation
---------------------
A customer pays for item price + freight, so the expected payment is
    expected_payment = total_item_price + total_freight
and payment_difference = total_payment - expected_payment.
The difference against item price alone is also exposed (diff_vs_item_price,
equal to freight when the order is fully paid) for transparency. An order is flagged
payment_mismatch when |payment_difference| > 0.01 (rounding tolerance).
Uniqueness of payments is the composite key (order_id, payment_sequential), which
is checked before aggregating.
"""
from pyspark.sql import functions as F

import common as C

TOLERANCE = 0.01

spark = C.get_spark("PaymentAnalysis")

payments = C.read_dataset(spark, "payments")
items = C.read_dataset(spark, "items")

dup = payments.groupBy("order_id", "payment_sequential").count().filter("count > 1").count()
if dup:
    raise ValueError(f"payments has {dup} duplicated (order_id, payment_sequential) keys")

payment_metrics = payments.groupBy("payment_type").agg(
    F.count("*").alias("total_transactions"),
    F.countDistinct("order_id").alias("total_orders"),
    F.round(F.sum("payment_value"), 2).alias("total_payment"),
    F.round(F.avg("payment_value"), 2).alias("avg_payment"),
    F.round(F.avg("payment_installments"), 2).alias("avg_installments"),
)

order_payments = payments.groupBy("order_id").agg(
    F.round(F.sum("payment_value"), 2).alias("total_payment"),
    F.count("*").alias("payment_transactions"),
    F.max("payment_installments").alias("max_installments"),
)
order_items = items.groupBy("order_id").agg(
    F.round(F.sum("price"), 2).alias("total_item_price"),
    F.round(F.sum("freight_value"), 2).alias("total_freight"),
)

comparison = (
    order_payments.join(order_items, "order_id", "full_outer")
    .withColumn("has_payment", F.col("total_payment").isNotNull())
    .withColumn("has_items", F.col("total_item_price").isNotNull())
    .withColumn("expected_payment", F.round(F.col("total_item_price") + F.col("total_freight"), 2))
    .withColumn("payment_difference", F.round(F.col("total_payment") - F.col("expected_payment"), 2))
    .withColumn("diff_vs_item_price", F.round(F.col("total_payment") - F.col("total_item_price"), 2))
    .withColumn("payment_mismatch", F.abs(F.col("payment_difference")) > TOLERANCE)
    .select("order_id", "has_payment", "has_items", "payment_transactions", "max_installments",
            "total_payment", "total_item_price", "total_freight", "expected_payment",
            "payment_difference", "diff_vs_item_price", "payment_mismatch")
    .cache()
)

C.write_parquet(payment_metrics, C.GOLD["payment_type"])
C.write_parquet(comparison, C.GOLD["payment_order"])

stats = comparison.agg(
    F.count("*").alias("orders"),
    F.sum(F.when(F.col("has_payment") & F.col("has_items"), 1).otherwise(0)).alias("both"),
    F.sum(F.col("payment_mismatch").cast("int")).alias("mismatch"),
    F.sum((~F.col("has_payment")).cast("int")).alias("no_payment"),
    F.sum((~F.col("has_items")).cast("int")).alias("no_items"),
).first()

print("\nPayment Analysis job completed.")
print(f"Orders in comparison: {stats['orders']:,} (with payment+items: {stats['both']:,})")
print(f"Orders without items: {stats['no_items']:,}   without payments: {stats['no_payment']:,}")
print(f"Orders where payment != price + freight (> {TOLERANCE}): {stats['mismatch']:,}")
payment_metrics.orderBy(F.col("total_payment").desc()).show(truncate=False)
comparison.filter("payment_mismatch").show(10, truncate=False)
spark.stop()
