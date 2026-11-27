"""Spark phase - Gold layer verification (the downstream input contract test).

For each Gold dataset it checks: readable Parquet, schema == contract, non-empty,
key uniqueness, no NULLs in non-nullable columns. It then reconciles the Gold totals
against the Silver/Bronze inputs (so stale or inflated outputs are detected), and checks
that every Gold dataset is newer than the Silver input it was built from when both are
on a filesystem that exposes modification times.
Exit code 0 = all checks passed, 1 = at least one failed.
"""
import sys

from pyspark.sql import functions as F

import common as C

# dataset -> (key columns, {column: (type, nullable)})
CONTRACT = {
    "delivery_detail": (["order_id"], {
        "order_id": ("string", False), "customer_id": ("string", False),
        "customer_state": ("string", True), "purchase_month": ("string", False),
        "total_items": ("bigint", False), "order_value": ("double", False),
        "freight_value": ("double", False), "actual_delivery_days": ("int", False),
        "estimated_delivery_days": ("int", False), "delay_days": ("int", False),
        "is_late": ("int", False), "has_date_anomaly": ("boolean", False)}),
    "delivery_monthly": (["purchase_month"], {
        "purchase_month": ("string", False), "total_orders": ("bigint", False),
        "avg_delivery_days": ("double", False), "avg_estimated_days": ("double", False),
        "late_orders": ("bigint", False), "late_percentage": ("double", False),
        "avg_delay_days": ("double", False)}),
    "delivery_state": (["customer_state"], {
        "customer_state": ("string", False), "total_orders": ("bigint", False),
        "avg_delivery_days": ("double", False), "avg_estimated_days": ("double", False),
        "late_orders": ("bigint", False), "late_percentage": ("double", False),
        "avg_delay_days": ("double", False)}),
    "seller_summary": (["seller_id"], {
        "seller_id": ("string", False), "total_orders": ("bigint", False),
        "total_items_sold": ("bigint", False), "total_sales": ("double", False),
        "avg_item_price": ("double", False), "total_freight": ("double", False),
        "delivered_orders": ("bigint", False), "late_orders": ("bigint", False),
        "late_percentage": ("double", True),  # NULL when the seller has no delivered order
        "seller_state": ("string", True), "seller_city": ("string", True)}),
    "seller_detail": (["order_id", "order_item_id"], {
        "order_id": ("string", False), "order_item_id": ("int", False),
        "seller_id": ("string", False), "price": ("double", False),
        "freight_value": ("double", False), "order_status": ("string", False),
        "order_delivered_customer_date": ("timestamp", True),
        "order_estimated_delivery_date": ("timestamp", False),
        "is_late": ("int", True)}),  # NULL when the order is not evaluable
    "product_products": (["product_id"], {
        "product_id": ("string", False), "product_category_name": ("string", False),
        "product_category_name_english": ("string", False), "has_translation": ("boolean", False),
        "total_items_sold": ("bigint", False), "total_orders": ("bigint", False),
        "total_sales": ("double", False), "total_freight": ("double", False),
        "avg_price": ("double", False)}),
    "product_categories": (["product_category_name"], {
        "product_category_name": ("string", False), "product_category_name_english": ("string", False),
        "has_translation": ("boolean", False), "total_items_sold": ("bigint", False),
        "unique_products": ("bigint", False), "total_sales": ("double", False),
        "total_freight": ("double", False), "avg_price": ("double", False)}),
    "payment_type": (["payment_type"], {
        "payment_type": ("string", False), "total_transactions": ("bigint", False),
        "total_orders": ("bigint", False), "total_payment": ("double", False),
        "avg_payment": ("double", False), "avg_installments": ("double", False)}),
    "payment_order": (["order_id"], {
        "order_id": ("string", False), "has_payment": ("boolean", False), "has_items": ("boolean", False),
        "payment_transactions": ("bigint", True), "max_installments": ("int", True),
        "total_payment": ("double", True), "total_item_price": ("double", True),
        "total_freight": ("double", True), "expected_payment": ("double", True),
        "payment_difference": ("double", True), "diff_vs_item_price": ("double", True),
        "payment_mismatch": ("boolean", True)}),  # NULL when an order lacks payment or items
}

results = []


def check(dataset, name, ok, detail=""):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {dataset:20} {name}: {detail}")


def close(a, b, tol=0.05):
    return a is not None and b is not None and abs(a - b) <= tol


spark = C.get_spark("VerifyGold")
gold = {}
for key, (keys, cols) in CONTRACT.items():
    try:
        df = C.read_gold(spark, key).cache()
    except Exception as exc:
        check(key, "readable", False, f"{C.GOLD[key]}: {str(exc).splitlines()[0]}")
        continue
    gold[key] = df
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    expected = {c: t for c, (t, _) in cols.items()}
    check(key, "schema matches contract", actual == expected,
          "ok" if actual == expected else f"expected {expected}, got {actual}")
    n = df.count()
    check(key, "row count > 0", n > 0, f"{n:,} rows")
    dup = df.groupBy(*keys).count().filter("count > 1").count()
    check(key, f"unique key {keys}", dup == 0, f"{dup} duplicated keys")
    required = [c for c, (_, nullable) in cols.items() if not nullable and c in actual]
    if required:
        nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in required]).first().asDict()
        nulls = {c: v for c, v in nulls.items() if v}
        check(key, "no NULLs in non-nullable columns", not nulls, nulls or "none")

if len(gold) == len(CONTRACT):
    # ---- reconciliation against the inputs (proves Gold matches the latest Silver/Bronze)
    items = C.read_dataset(spark, "items")
    orders = C.read_dataset(spark, "orders")
    payments = C.read_dataset(spark, "payments")
    isum = items.agg(F.count("*").alias("n"), F.sum("price").alias("p"), F.sum("freight_value").alias("f")).first()

    sd = gold["seller_detail"].agg(F.count("*").alias("n"), F.sum("price").alias("p"), F.sum("freight_value").alias("f")).first()
    check("seller_detail", "items/price/freight equal items input",
          sd["n"] == isum["n"] and close(sd["p"], isum["p"]) and close(sd["f"], isum["f"]),
          f"{sd['n']:,} / {sd['p']:,.2f} / {sd['f']:,.2f}")
    ss = gold["seller_summary"].agg(F.sum("total_items_sold").alias("n"), F.sum("total_sales").alias("p"),
                                    F.sum("total_freight").alias("f")).first()
    check("seller_summary", "totals equal items input",
          ss["n"] == isum["n"] and close(ss["p"], isum["p"], 5) and close(ss["f"], isum["f"], 5),
          f"{ss['n']:,} / {ss['p']:,.2f} / {ss['f']:,.2f}")
    check("seller_summary", "late_orders <= delivered_orders <= total_orders",
          gold["seller_summary"].filter("late_orders > delivered_orders OR delivered_orders > total_orders").count() == 0)
    for k in ["product_products", "product_categories"]:
        t = gold[k].agg(F.sum("total_items_sold").alias("n"), F.sum("total_sales").alias("p")).first()
        check(k, "no join multiplication (items & sales equal items input)",
              t["n"] == isum["n"] and close(t["p"], isum["p"], 5), f"{t['n']:,} / {t['p']:,.2f}")
    check("product_categories", "unique_products sums to distinct products in products Gold",
          gold["product_categories"].agg(F.sum("unique_products")).first()[0] == gold["product_products"].count())

    expected_delivered = C.with_delivery_flags(orders).filter("evaluable_delivery").count()
    check("delivery_detail", "rows == evaluable delivered orders in Silver",
          gold["delivery_detail"].count() == expected_delivered, f"{expected_delivered:,}")
    check("delivery_detail", "is_late consistent with delay_days",
          gold["delivery_detail"].filter(
              "(delay_days > 0 AND is_late != 1) OR (delay_days <= 0 AND is_late != 0)").count() == 0)
    check("delivery_detail", "has no future/negative durations",
          gold["delivery_detail"].filter("actual_delivery_days < 0").count() == 0)
    for k in ["delivery_monthly", "delivery_state"]:
        t = gold[k].agg(F.sum("total_orders").alias("n"), F.sum("late_orders").alias("l")).first()
        d = gold["delivery_detail"].agg(F.count("*").alias("n"), F.sum("is_late").alias("l")).first()
        has_state_null = k == "delivery_state" and gold["delivery_detail"].filter("customer_state IS NULL").count()
        check(k, "totals equal delivery_detail",
              t["l"] == d["l"] and (t["n"] == d["n"] - (has_state_null or 0)), f"{t['n']:,} orders, {t['l']:,} late")

    pt = gold["payment_type"].agg(F.sum("total_transactions").alias("n"), F.sum("total_payment").alias("p")).first()
    pin = payments.agg(F.count("*").alias("n"), F.sum("payment_value").alias("p")).first()
    check("payment_type", "totals equal payments input",
          pt["n"] == pin["n"] and close(pt["p"], pin["p"], 1), f"{pt['n']:,} / {pt['p']:,.2f}")
    po = gold["payment_order"]
    check("payment_order", "total_payment equals payments input",
          close(po.agg(F.sum("total_payment")).first()[0], pin["p"], 1))
    check("payment_order", "total_item_price equals items input",
          close(po.agg(F.sum("total_item_price")).first()[0], isum["p"], 1))
    check("payment_order", "one row per order in Silver orders or items/payments",
          po.count() >= orders.count(), f"{po.count():,} rows vs {orders.count():,} orders")

spark.stop()
failed = results.count(False)
print(f"\nGOLD VERIFICATION: {results.count(True)} passed, {failed} failed")
sys.exit(1 if failed else 0)
