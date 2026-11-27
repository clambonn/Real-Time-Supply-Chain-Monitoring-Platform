# Spark data validation report

Generated: 2026-11-27 10:23:40
Data lake: `hdfs://namenode:9000/supply-chain`

**Summary:** PASS: 72 | INFO: 9 | WARNING: 15 | FAIL: 0

**Overall: PASS (with warnings)**

## orders

| Status | Check | Detail |
|---|---|---|
| PASS | row count | 99,441 rows |
| PASS | column count | 8 columns (expected 8) |
| PASS | NULLs in required columns | none |
| INFO | NULLs in optional columns | order_approved_at=160, order_delivered_carrier_date=1,783, order_delivered_customer_date=2,965 |
| PASS | missing key ['order_id'] | 0 violations |
| PASS | duplicate key ['order_id'] | 0 violations |
| PASS | data types parse (dates/numbers/ints) | 5 typed columns OK |
| PASS | status values | all statuses known |
| INFO | status distribution | delivered=96,478, shipped=1,107, canceled=625, unavailable=609, invoiced=314, processing=301, created=5, approved=2 |
| PASS | approved before purchase | 0 violations |
| WARNING | carrier pick-up before purchase | 166 orders (kept for investigation, not deleted) |
| PASS | delivery before purchase | 0 violations |
| PASS | estimated date before purchase | 0 violations |
| WARNING | carrier pick-up before approval | 1,359 orders (kept, flagged in Gold as has_date_anomaly) |
| WARNING | delivery before carrier pick-up | 23 orders (kept, flagged in Gold as has_date_anomaly) |
| WARNING | status 'delivered' without delivery date | 8 orders (excluded from delivery metrics, not counted on time) |
| WARNING | delivery date on non-delivered order | 6 orders (excluded from delivery metrics) |
| PASS | customer_id exists in customers.customer_id | 0 violations |
| INFO | orders without items | shipped=1, unavailable=603, created=5, canceled=164, invoiced=2 (expected for canceled/unavailable orders) |
| WARNING | orders without any payment | 1 orders |
| PASS | Silver matches local Data/cleaned | rows 99,441 vs 99,441; content hash equal |

## payments

| Status | Check | Detail |
|---|---|---|
| PASS | row count | 103,886 rows |
| PASS | column count | 5 columns (expected 5) |
| PASS | NULLs in required columns | none |
| PASS | missing key ['order_id', 'payment_sequential'] | 0 violations |
| PASS | duplicate key ['order_id', 'payment_sequential'] | 0 violations |
| PASS | data types parse (dates/numbers/ints) | 3 typed columns OK |
| PASS | payment_value >= 0 | 0 violations |
| WARNING | payment_value = 0 | 9 payments (mostly voucher / not_defined) |
| WARNING | installments >= 1 | 2 payments with installments < 1 |
| PASS | payment_sequential >= 1 | 0 violations |
| PASS | payment_type known | 0 violations |
| WARNING | payment_type 'not_defined' | 3 payments |
| WARNING | payment_sequential continuous (1..n) | 80 orders with gaps in payment_sequential |
| PASS | order_id exists in orders.order_id | 0 violations |
| WARNING | payment total = item price + freight | 387 of 98,665 orders differ by > 0.01 (typically installment interest / vouchers) |
| PASS | Silver matches local Data/cleaned | rows 103,886 vs 103,886; content hash equal |

## reviews

| Status | Check | Detail |
|---|---|---|
| PASS | row count | 99,224 rows |
| PASS | column count | 7 columns (expected 7) |
| PASS | NULLs in required columns | none |
| INFO | NULLs in optional columns | review_comment_title=87,658, review_comment_message=58,274 |
| PASS | missing key ['review_id', 'order_id'] | 0 violations |
| PASS | duplicate key ['review_id', 'order_id'] | 0 violations |
| PASS | data types parse (dates/numbers/ints) | 3 typed columns OK |
| PASS | review_score in 1..5 | 0 violations |
| PASS | answer before creation | 0 violations |
| INFO | orders with multiple reviews | 547 orders (allowed; key is review_id+order_id) |
| INFO | review_id reused across orders | 789 review_ids (not duplicates) |
| INFO | missing comments | title NULL=87,658, message NULL=58,274 (optional free text) |
| PASS | order_id exists in orders.order_id | 0 violations |
| PASS | Silver matches local Data/cleaned | rows 99,224 vs 99,224; content hash equal |

## items

| Status | Check | Detail |
|---|---|---|
| PASS | row count | 112,650 rows |
| PASS | column count | 7 columns (expected 7) |
| PASS | NULLs in required columns | none |
| PASS | missing key ['order_id', 'order_item_id'] | 0 violations |
| PASS | duplicate key ['order_id', 'order_item_id'] | 0 violations |
| PASS | data types parse (dates/numbers/ints) | 4 typed columns OK |
| PASS | price > 0 | 0 violations |
| PASS | freight_value >= 0 | 0 violations |
| PASS | order_item_id >= 1 | 0 violations |
| PASS | order_id exists in orders.order_id | 0 violations |
| PASS | product_id exists in products.product_id | 0 violations |
| PASS | seller_id exists in sellers.seller_id | 0 violations |

## customers

| Status | Check | Detail |
|---|---|---|
| PASS | row count | 99,441 rows |
| PASS | column count | 5 columns (expected 5) |
| PASS | NULLs in required columns | none |
| PASS | missing key ['customer_id'] | 0 violations |
| PASS | duplicate key ['customer_id'] | 0 violations |
| PASS | customer_state is 2-letter code | 0 violations |

## sellers

| Status | Check | Detail |
|---|---|---|
| PASS | row count | 3,095 rows |
| PASS | column count | 4 columns (expected 4) |
| PASS | NULLs in required columns | none |
| PASS | missing key ['seller_id'] | 0 violations |
| PASS | duplicate key ['seller_id'] | 0 violations |
| PASS | seller_state is 2-letter code | 0 violations |

## products

| Status | Check | Detail |
|---|---|---|
| PASS | row count | 32,951 rows |
| PASS | column count | 9 columns (expected 9) |
| PASS | NULLs in required columns | none |
| INFO | NULLs in optional columns | product_category_name=610, product_name_lenght=610, product_description_lenght=610, product_photos_qty=610, product_weight_g=2, product_length_cm=2, product_height_cm=2, product_width_cm=2 |
| PASS | missing key ['product_id'] | 0 violations |
| PASS | duplicate key ['product_id'] | 0 violations |
| PASS | data types parse (dates/numbers/ints) | 7 typed columns OK |
| WARNING | dimensions/weight > 0 | 4 records |
| WARNING | category present | 610 products without category (reported as 'unknown' in Gold) |
| WARNING | category found in translation table | 2 categories without English translation (Portuguese name kept) |

## translation

| Status | Check | Detail |
|---|---|---|
| PASS | row count | 71 rows |
| PASS | column count | 2 columns (expected 2) |
| PASS | NULLs in required columns | none |
| PASS | missing key ['product_category_name'] | 0 violations |
| PASS | duplicate key ['product_category_name'] | 0 violations |

## geolocation

| Status | Check | Detail |
|---|---|---|
| PASS | row count | 1,000,163 rows |
| PASS | column count | 5 columns (expected 5) |
| PASS | NULLs in required columns | none |
| PASS | data types parse (dates/numbers/ints) | 2 typed columns OK |
| WARNING | lat/lng in Brazil bounds | 42 records |
| INFO | exact duplicate rows | 261,831 (zip prefixes have many coordinates by design) |
