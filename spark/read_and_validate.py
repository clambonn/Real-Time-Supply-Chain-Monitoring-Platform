"""Spark phase - data validation / QA report for all 9 source datasets.

Reads Silver (orders, payments, reviews) and Bronze (the other six) from the fixed
paths in common.py, runs the checks below and writes
    <SPARK_REPORT_DIR>/qa_report.md   human readable (PASS / WARNING / FAIL)
    <SPARK_REPORT_DIR>/qa_results.csv machine readable
Exit code: 0 when there is no FAIL, 1 otherwise. Nothing is deleted or modified.

Status meaning
  PASS    rule satisfied
  INFO    expected characteristic of the data (e.g. missing delivery date for an order
          that was not delivered, missing review comments) - not a quality problem
  WARNING anomaly that is reported and must be understood, but records are kept
  FAIL    rule violation that makes downstream results unreliable
"""
import csv
import os
import sys
from datetime import datetime

from pyspark.sql import functions as F

import common as C

LOCAL_CLEANED_DIR = os.environ.get("LOCAL_CLEANED_DIR", "/data/cleaned")  # mounted Data/cleaned
SILVER_NAMES = {"orders": "olist_orders_cleaned.csv", "payments": "olist_order_payments_cleaned.csv",
                "reviews": "olist_order_reviews_cleaned.csv"}
ORDER_STATUSES = ["delivered", "shipped", "canceled", "unavailable", "invoiced",
                  "processing", "created", "approved"]
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]  # 'not_defined' handled as WARNING
PRIMARY_KEYS = {
    "orders": ["order_id"], "customers": ["customer_id"], "sellers": ["seller_id"],
    "products": ["product_id"], "translation": ["product_category_name"],
    "items": ["order_id", "order_item_id"], "payments": ["order_id", "payment_sequential"],
    "reviews": ["review_id", "order_id"],  # a review_id may legitimately cover several orders
}
# Columns that must never be NULL; any other column may be empty (nulls are reported as INFO).
REQUIRED = {
    "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
               "order_estimated_delivery_date"],
    "payments": C.COLUMNS["payments"], "reviews": ["review_id", "order_id", "review_score",
                                                    "review_creation_date", "review_answer_timestamp"],
    "items": C.COLUMNS["items"], "customers": C.COLUMNS["customers"], "sellers": C.COLUMNS["sellers"],
    "products": ["product_id"], "translation": C.COLUMNS["translation"],
    "geolocation": C.COLUMNS["geolocation"],
}

results = []  # (dataset, check, status, detail)


def add(dataset, check, status, detail=""):
    results.append((dataset, check, status, str(detail)))
    print(f"[{status:7}] {dataset:12} {check}: {detail}")


def rule(dataset, check, bad_count, fail_status="FAIL", detail_ok="0 violations", what="records"):
    """PASS when bad_count == 0, otherwise fail_status."""
    add(dataset, check, "PASS" if bad_count == 0 else fail_status,
        detail_ok if bad_count == 0 else f"{bad_count:,} {what}")


spark = C.get_spark("SupplyChainValidation")
raw, typed = {}, {}
for name in C.SOURCES:
    try:
        raw[name] = C.read_raw_strings(spark, name)
        typed[name] = C.read_dataset(spark, name).cache()
    except Exception as exc:  # unreadable source = hard failure, reported explicitly
        add(name, "source readable", "FAIL", f"{C.SOURCES[name]}: {exc}")
if any(r[2] == "FAIL" for r in results):
    sys.exit(1)

# ------------------------------------------------------------------ per dataset
for name, df in typed.items():
    rows = df.count()
    add(name, "row count", "PASS" if rows > 0 else "FAIL", f"{rows:,} rows")
    ncols = len(df.columns)
    add(name, "column count", "PASS" if ncols == len(C.COLUMNS[name]) else "FAIL",
        f"{ncols} columns (expected {len(C.COLUMNS[name])})")

    # nulls (single pass)
    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first().asDict()
    bad_required = {c: n for c, n in nulls.items() if n and c in REQUIRED[name]}
    optional = {c: n for c, n in nulls.items() if n and c not in REQUIRED[name]}
    rule(name, "NULLs in required columns", sum(bad_required.values()), detail_ok="none",
         what=f"NULL values {bad_required}")
    if optional:
        add(name, "NULLs in optional columns", "INFO",
            ", ".join(f"{c}={n:,}" for c, n in optional.items()))

    # primary key: missing + duplicates
    key = PRIMARY_KEYS.get(name)  # geolocation has no key (many coordinates per zip prefix)
    if key:
        missing = df.filter(" OR ".join(f"{k} IS NULL" for k in key)).count()
        dups = df.groupBy(*key).count().filter("count > 1").count()
        rule(name, f"missing key {key}", missing)
        rule(name, f"duplicate key {key}", dups, what="duplicated keys")

    # type conversion failures: raw value present but typed value NULL
    conv = (C.TIMESTAMP_COLS.get(name, []) + C.DOUBLE_COLS.get(name, []) + C.INT_COLS.get(name, []))
    if conv:
        combined = raw[name].select(*[F.when(F.trim(F.col(c)) != "", F.col(c)).alias(c) for c in conv])
        typed_part = df.select(*conv)
        bad_total = {}
        for c in conv:
            bad_total[c] = (combined.filter(F.col(c).isNotNull()).count()
                            - typed_part.filter(F.col(c).isNotNull()).count())
        bad_total = {c: n for c, n in bad_total.items() if n}
        rule(name, "data types parse (dates/numbers/ints)", sum(bad_total.values()),
             detail_ok=f"{len(conv)} typed columns OK", what=f"unparseable values {bad_total}")

# ------------------------------------------------------------------ orders
o = typed["orders"]
add("orders", "status values", *(("PASS", "all statuses known") if o.filter(~F.col("order_status").isin(ORDER_STATUSES)).count() == 0
                                 else ("FAIL", "unexpected order_status values")))
add("orders", "status distribution", "INFO",
    ", ".join(f"{r['order_status']}={r['count']:,}" for r in o.groupBy("order_status").count().orderBy(F.desc("count")).collect()))
P, A, CR, D, E = ("order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
                  "order_delivered_customer_date", "order_estimated_delivery_date")
rule("orders", "approved before purchase", o.filter(F.col(A) < F.col(P)).count())
rule("orders", "carrier pick-up before purchase", o.filter(F.col(CR) < F.col(P)).count(),
     fail_status="WARNING", what="orders (kept for investigation, not deleted)")
rule("orders", "delivery before purchase", o.filter(F.col(D) < F.col(P)).count())
rule("orders", "estimated date before purchase", o.filter(F.col(E) < F.col(P)).count())
rule("orders", "carrier pick-up before approval", o.filter(F.col(CR) < F.col(A)).count(),
     fail_status="WARNING", what="orders (kept, flagged in Gold as has_date_anomaly)")
rule("orders", "delivery before carrier pick-up", o.filter(F.col(D) < F.col(CR)).count(),
     fail_status="WARNING", what="orders (kept, flagged in Gold as has_date_anomaly)")
rule("orders", "status 'delivered' without delivery date",
     o.filter((F.col("order_status") == "delivered") & F.col(D).isNull()).count(),
     fail_status="WARNING", what="orders (excluded from delivery metrics, not counted on time)")
rule("orders", "delivery date on non-delivered order",
     o.filter((F.col("order_status") != "delivered") & F.col(D).isNotNull()).count(),
     fail_status="WARNING", what="orders (excluded from delivery metrics)")

# ------------------------------------------------------------------ items / payments / reviews rules
it, pay, rv = typed["items"], typed["payments"], typed["reviews"]
rule("items", "price > 0", it.filter(F.col("price") <= 0).count())
rule("items", "freight_value >= 0", it.filter(F.col("freight_value") < 0).count())
rule("items", "order_item_id >= 1", it.filter(F.col("order_item_id") < 1).count())
rule("payments", "payment_value >= 0", pay.filter(F.col("payment_value") < 0).count())
rule("payments", "payment_value = 0", pay.filter(F.col("payment_value") == 0).count(),
     fail_status="WARNING", what="payments (mostly voucher / not_defined)")
rule("payments", "installments >= 1", pay.filter(F.col("payment_installments") < 1).count(),
     fail_status="WARNING", what="payments with installments < 1")
rule("payments", "payment_sequential >= 1", pay.filter(F.col("payment_sequential") < 1).count())
rule("payments", "payment_type known", pay.filter(~F.col("payment_type").isin(PAYMENT_TYPES + ["not_defined"])).count(),
     what="unknown payment types")
rule("payments", "payment_type 'not_defined'", pay.filter(F.col("payment_type") == "not_defined").count(),
     fail_status="WARNING", what="payments")
# sequence continuity per order: sequentials should be 1..n
seq = pay.groupBy("order_id").agg(F.max("payment_sequential").alias("mx"), F.count("*").alias("n"))
rule("payments", "payment_sequential continuous (1..n)", seq.filter("mx != n").count(),
     fail_status="WARNING", what="orders with gaps in payment_sequential")
rule("reviews", "review_score in 1..5", rv.filter(~F.col("review_score").between(1, 5)).count())
rule("reviews", "answer before creation", rv.filter(F.col("review_answer_timestamp") < F.col("review_creation_date")).count(),
     fail_status="WARNING")
multi = rv.groupBy("order_id").count().filter("count > 1").count()
add("reviews", "orders with multiple reviews", "INFO", f"{multi:,} orders (allowed; key is review_id+order_id)")
shared = rv.groupBy("review_id").agg(F.countDistinct("order_id").alias("n")).filter("n > 1").count()
add("reviews", "review_id reused across orders", "INFO", f"{shared:,} review_ids (not duplicates)")
add("reviews", "missing comments", "INFO",
    f"title NULL={rv.filter('review_comment_title IS NULL').count():,}, "
    f"message NULL={rv.filter('review_comment_message IS NULL').count():,} (optional free text)")
geo = typed["geolocation"]
rule("geolocation", "lat/lng in Brazil bounds",
     geo.filter(~(F.col("geolocation_lat").between(-34, 6) & F.col("geolocation_lng").between(-75, -34))).count(),
     fail_status="WARNING")
add("geolocation", "exact duplicate rows", "INFO",
    f"{geo.count() - geo.dropDuplicates().count():,} (zip prefixes have many coordinates by design)")
pr = typed["products"]
rule("products", "dimensions/weight > 0",
     pr.filter((F.col("product_weight_g") <= 0) | (F.col("product_length_cm") <= 0)
               | (F.col("product_height_cm") <= 0) | (F.col("product_width_cm") <= 0)).count(),
     fail_status="WARNING")
rule("products", "category present", pr.filter("product_category_name IS NULL").count(),
     fail_status="WARNING", what="products without category (reported as 'unknown' in Gold)")
tr = typed["translation"]
rule("products", "category found in translation table",
     pr.filter("product_category_name IS NOT NULL").join(tr, "product_category_name", "left_anti")
     .select("product_category_name").distinct().count(),
     fail_status="WARNING", what="categories without English translation (Portuguese name kept)")
for name, col in [("customers", "customer_state"), ("sellers", "seller_state")]:
    rule(name, f"{col} is 2-letter code", typed[name].filter(~F.col(col).rlike("^[A-Z]{2}$")).count())

# ------------------------------------------------------------------ referential integrity
def orphan(child, ckey, parent, pkey, label, status="FAIL"):
    n = typed[child].filter(F.col(ckey).isNotNull()).join(
        typed[parent].select(F.col(pkey).alias("_k")), F.col(ckey) == F.col("_k"), "left_anti").count()
    rule(child, f"{ckey} exists in {parent}.{pkey}", n, fail_status=status, what=label)

orphan("items", "order_id", "orders", "order_id", "orphan items")
orphan("items", "product_id", "products", "product_id", "orphan items")
orphan("items", "seller_id", "sellers", "seller_id", "orphan items")
orphan("orders", "customer_id", "customers", "customer_id", "orphan orders")
orphan("payments", "order_id", "orders", "order_id", "orphan payments")
orphan("reviews", "order_id", "orders", "order_id", "orphan reviews")
no_items = o.join(it.select("order_id").distinct(), "order_id", "left_anti")
add("orders", "orders without items", "INFO" if no_items.filter(
    F.col("order_status").isin("delivered")).count() == 0 else "WARNING",
    ", ".join(f"{r['order_status']}={r['count']:,}" for r in no_items.groupBy("order_status").count().collect())
    + " (expected for canceled/unavailable orders)")
rule("orders", "orders without any payment", o.join(pay.select("order_id").distinct(), "order_id", "left_anti").count(),
     fail_status="WARNING", what="orders")

# ------------------------------------------------------------------ business rule: payments vs items
cmp_ = (pay.groupBy("order_id").agg(F.sum("payment_value").alias("paid"))
        .join(it.groupBy("order_id").agg((F.sum("price") + F.sum("freight_value")).alias("expected")), "order_id"))
mismatch = cmp_.filter(F.abs(F.col("paid") - F.col("expected")) > 0.01).count()
add("payments", "payment total = item price + freight", "PASS" if mismatch == 0 else "WARNING",
    f"{mismatch:,} of {cmp_.count():,} orders differ by > 0.01 (typically installment interest / vouchers)")

# ------------------------------------------------------------------ Silver freshness (HDFS vs local cleaned files)
if os.path.isdir(LOCAL_CLEANED_DIR):
    for name, fname in SILVER_NAMES.items():
        local_path = f"file://{os.path.abspath(LOCAL_CLEANED_DIR)}/{fname}"
        local = (spark.read.schema(raw[name].schema).option("header", True).option("quote", '"')
                 .option("escape", '"').option("multiLine", name in C.MULTILINE).csv(local_path))

        def fingerprint(d):
            r = d.select(F.count("*").alias("n"),
                         F.sum(F.xxhash64(*[F.coalesce(F.col(c), F.lit("")) for c in d.columns]).cast("decimal(38,0)")).alias("h")).first()
            return r["n"], r["h"]
        a, b = fingerprint(raw[name]), fingerprint(local)
        add(name, "Silver matches local Data/cleaned", "PASS" if a == b else "FAIL",
            f"rows {a[0]:,} vs {b[0]:,}; content hash {'equal' if a[1] == b[1] else 'DIFFERENT - re-upload Silver file'}")
else:
    add("silver", "freshness vs local Data/cleaned", "WARNING",
        f"{LOCAL_CLEANED_DIR} not available; mount ./Data:/data:ro or set LOCAL_CLEANED_DIR")

# ------------------------------------------------------------------ report
spark.stop()
os.makedirs(C.REPORT_DIR, exist_ok=True)
counts = {s: sum(1 for r in results if r[2] == s) for s in ["PASS", "INFO", "WARNING", "FAIL"]}
with open(os.path.join(C.REPORT_DIR, "qa_results.csv"), "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["dataset", "check", "status", "detail"])
    w.writerows(results)
with open(os.path.join(C.REPORT_DIR, "qa_report.md"), "w", encoding="utf-8") as fh:
    fh.write(f"# Spark data validation report\n\nGenerated: {datetime.now():%Y-%m-%d %H:%M:%S}  \n"
             f"Data lake: `{C.BASE}`\n\n")
    fh.write("**Summary:** " + " | ".join(f"{k}: {v}" for k, v in counts.items()) + "\n\n")
    fh.write(f"**Overall: {'FAIL' if counts['FAIL'] else 'PASS (with warnings)' if counts['WARNING'] else 'PASS'}**\n\n")
    for ds in dict.fromkeys(r[0] for r in results):
        fh.write(f"## {ds}\n\n| Status | Check | Detail |\n|---|---|---|\n")
        for r in results:
            if r[0] == ds:
                fh.write(f"| {r[2]} | {r[1]} | {r[3].replace('|', '/')} |\n")
        fh.write("\n")
print(f"\nSUMMARY {counts}  -> {C.REPORT_DIR}/qa_report.md")
sys.exit(1 if counts["FAIL"] else 0)
