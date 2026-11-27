# Spark Phase (Phase 5) – Architecture, Gold Data Contract and Runbook

Status: handed off. All commands below were executed against the project data; see
`Docs/Spark_Phase_Report.md` for exactly what was run and where (important: the execution
environment used for verification had no Docker/HDFS, see *Known limitations*).

## 1. Architecture and data flow

```
 Data/raw  ──(HDFS put)──▶  Bronze  hdfs://namenode:9000/supply-chain/raw/       9 original CSV files
 Data/cleaned ─(put)────▶  Silver  hdfs://namenode:9000/supply-chain/cleaned/   3 cleaned CSV files
                               │
                               ▼   spark-runner container (PySpark, local[*], reads/writes HDFS)
 read_and_validate.py ─▶ reports/qa_report.md  (PASS / INFO / WARNING / FAIL)
 4 analytical jobs    ─▶  Gold   hdfs://namenode:9000/supply-chain/analytics/    Parquet
 verify_gold.py       ─▶ schema + reconciliation test of every Gold dataset
```

* Silver exists only for `orders`, `payments`, `reviews` (the cleaning phase changed only those).
  Orders, payments and reviews are always read from Silver. Items, customers, sellers, products,
  translation and geolocation have no cleaned version and are read from Bronze.
* All paths are fixed and defined once in `spark/common.py` (no recursive file search).
  Root = `SUPPLY_CHAIN_BASE` (default `hdfs://namenode:9000/supply-chain`).
* Files are read with an explicit all-string schema and converted with `try_cast` /
  `try_to_timestamp`, so a malformed value becomes NULL and is *counted by validation*
  instead of being guessed by `inferSchema`. Review files are read with `multiLine=true`
  (comments contain line breaks: 103,080 physical lines = 99,224 reviews).

| File | Purpose |
|---|---|
| `spark/common.py` | Paths, column lists, typed readers, shared delivery rule (`with_delivery_flags`), session factory |
| `spark/read_and_validate.py` | Validation of all 9 datasets, Silver freshness check, QA report |
| `spark/delivery_performance.py`, `seller_performance.py`, `product_category.py`, `payment_analysis.py` | Gold jobs |
| `spark/verify_gold.py` | Reads every Gold dataset back, checks schema/keys/nulls and reconciles totals with the inputs |
| `spark/run_pipeline.sh` | Runs validation → 4 jobs → verification, stops on first failure |
| `spark/reports/` | `qa_report.md`, `qa_results.csv`, `last_run_log.txt` from the last verified run |

## 2. Running it

Start Hadoop and make sure Bronze/Silver are loaded (existing procedure, `spark_audit.ps1` checks it):

```bash
docker compose -f docker-compose.hadoop.yml up -d namenode datanode
docker compose -f docker-compose.hadoop.yml build spark-runner
```

Whole phase (recommended, stops at first failure):

```bash
docker compose -f docker-compose.hadoop.yml run --rm -e HADOOP_USER_NAME=hadoop spark-runner bash run_pipeline.sh
```

A single step:

```bash
docker compose -f docker-compose.hadoop.yml run --rm -e HADOOP_USER_NAME=hadoop spark-runner python delivery_performance.py
```

Order: `read_and_validate` → `delivery_performance` / `seller_performance` / `product_category` /
`payment_analysis` (independent of each other) → `verify_gold`. Jobs are idempotent (`overwrite`).

Refresh after the cleaning notebooks change Silver: re-upload the changed file
(`hdfs dfs -put -f /tmp/<file> /supply-chain/cleaned/`), then run the pipeline.
`read_and_validate.py` compares row count and a content hash of each HDFS Silver file with
`Data/cleaned/` (mounted read-only at `/data` in `spark-runner`) and reports FAIL if they differ.

Local run without HDFS (e.g. development): `pip install pyspark`, Java 17+, then
`SUPPLY_CHAIN_BASE=file:///some/lake LOCAL_CLEANED_DIR=Data/cleaned bash run_pipeline.sh`
where `some/lake` contains `raw/` and `cleaned/` copies of `Data/raw` and `Data/cleaned`.

Dependencies: Python 3.13, PySpark 4.3.0 (pip, in `spark/Dockerfile`), Java 17, HDFS reachable as
`namenode:9000`. No other Python packages are needed.

## 3. Business rules (single source of truth)

* **Evaluable delivery** = `order_status = 'delivered'` AND actual delivery date AND estimated date present.
  Only evaluable orders are classified. A missing date is **never** counted as on time.
* **Day granularity**: `delay_days = date(delivered) − date(estimated)`; `is_late = delay_days > 0`.
  The estimated date has no time of day, so a delivery at any time on the estimated day is on time.
  (The previous seller job compared timestamps and produced 7,826 late orders vs 6,534 under the
  day rule used by the delivery job; both jobs now share one function.)
* Date anomalies (carrier pick-up before approval/purchase, delivery before pick-up) are **kept** and
  flagged (`has_date_anomaly`), never deleted.
* Sales metrics (seller, product, category) cover every order item in `olist_order_items`.
* **Payments vs items**: customers pay `item price + freight`, so
  `expected_payment = total_item_price + total_freight` and `payment_difference = total_payment − expected_payment`.
  Comparing against price alone (the previous logic) mismatches almost every order by the freight amount;
  it is still exposed as `diff_vs_item_price`. Mismatch tolerance 0.01.
* Payment uniqueness uses `(order_id, payment_sequential)`; review uniqueness uses `(review_id, order_id)`.

## 4. Gold datasets (input contract for the next phase)

Root: `hdfs://namenode:9000/supply-chain/analytics/`. Format: Parquet (snappy, directory of part files;
read the directory, not a single file). Types are Spark types. "Key" = unique per row. Row counts are those
of the verified run on the current data.

### delivery_performance/detail — 96,470 rows — key `order_id`
One row per evaluable delivered order.

| Column | Type | Description |
|---|---|---|
| order_id, customer_id | string | Order and customer ids |
| customer_state | string | Customer state (UF); NULL only if customer missing |
| purchase_month | string | `yyyy-MM` of purchase |
| total_items / order_value / freight_value | bigint / double / double | Items count, Σprice, Σfreight of the order (0 if no items) |
| actual_delivery_days | int | Calendar days purchase → customer delivery |
| estimated_delivery_days | int | Calendar days purchase → estimated delivery |
| delay_days | int | Delivery date − estimated date (>0 late, ≤0 on time/early) |
| is_late | int | 1 late, 0 not late |
| has_date_anomaly | boolean | Delivery before carrier pick-up or pick-up before approval |

### delivery_performance/monthly — 23 rows — key `purchase_month`
### delivery_performance/state — 27 rows — key `customer_state`
Columns (both): group key; `total_orders` bigint; `avg_delivery_days`, `avg_estimated_days` double (days);
`late_orders` bigint; `late_percentage` double (0–100); `avg_delay_days` double (negative = early).

### seller_performance/summary — 3,095 rows — key `seller_id`
| Column | Type | Description |
|---|---|---|
| seller_id | string | Seller |
| total_orders | bigint | Distinct orders containing the seller's items |
| total_items_sold | bigint | Order items |
| total_sales / avg_item_price / total_freight | double | Σprice, mean price, Σfreight |
| delivered_orders | bigint | Distinct evaluable delivered orders |
| late_orders | bigint | Distinct late orders |
| late_percentage | double | late_orders / delivered_orders × 100; **NULL** if no delivered order |
| seller_state, seller_city | string | Seller location |

### seller_performance/detail — 112,650 rows — key `(order_id, order_item_id)`
`order_id` string, `order_item_id` int, `seller_id` string, `price`, `freight_value` double, `order_status` string,
`order_delivered_customer_date`, `order_estimated_delivery_date` timestamp, `is_late` int (**NULL** when the order is not evaluable).

### product_category/products — 32,951 rows — key `product_id`
`product_id`; `product_category_name` (Portuguese, `unknown` if missing); `product_category_name_english`
(falls back to the Portuguese name when no translation exists); `has_translation` boolean;
`total_items_sold`, `total_orders` bigint; `total_sales`, `total_freight`, `avg_price` double.

### product_category/categories — 74 rows — key `product_category_name`
`product_category_name`, `product_category_name_english`, `has_translation`, `total_items_sold` bigint,
`unique_products` bigint, `total_sales`, `total_freight`, `avg_price` double.

### payment_analysis/payment_type — 5 rows — key `payment_type`
`payment_type` string; `total_transactions`, `total_orders` bigint; `total_payment`, `avg_payment`,
`avg_installments` double. (`not_defined` = 3 zero-value rows in the source.)

### payment_analysis/order_comparison — 99,441 rows — key `order_id`
`order_id`; `has_payment`, `has_items` boolean (orders existing on only one side are kept: 775 orders have no
items, 1 has no payment); `payment_transactions` bigint; `max_installments` int; `total_payment`,
`total_item_price`, `total_freight`, `expected_payment`, `payment_difference`, `diff_vs_item_price` double;
`payment_mismatch` boolean (|difference| > 0.01; **NULL** when payment or items are missing).
Verified: 303 of 98,665 comparable orders differ (installment interest / vouchers).

### Reading from the next phase
```python
spark.read.parquet("hdfs://namenode:9000/supply-chain/analytics/seller_performance/summary")
```
Monetary values are `double` rounded to 2 decimals; use `decimal` casting if exact sums are needed downstream.
Paths and names are listed in `common.GOLD`; the schema contract is `CONTRACT` in `verify_gold.py`
(`verify_gold.py` is the executable test of this document).

## 5. Next phase integration (Phase 6 Kafka / Phase 7 Airflow)

* The roadmap asks Spark to "define the first outputs that downstream Kafka/Airflow integration can use";
  nothing in Spark depends on Kafka. Kafka/Airflow are **not** implemented here.
* Airflow should call the existing entry points rather than duplicate logic: one task per script
  (`read_and_validate` → four jobs in parallel → `verify_gold`); each exits non-zero on failure, so
  retries/alerts can key off the exit code. Command form: see section 2.
* Kafka events (order/shipment) can be joined to Gold on `order_id`, `seller_id`, `customer_state`.
  Gold is a **batch snapshot** of the CSV data in HDFS; streamed orders (SQL Server `staging.stg_orders`)
  are not reflected until they are exported to HDFS and the pipeline re-run.

## 6. Known limitations / assumptions

* Verified in a sandbox without Docker/HDFS using local-filesystem mode and PySpark 4.0.0 / Java 21 /
  Python 3.12 (project: PySpark 4.3.0 / Java 17 / Python 3.13). Code uses only APIs available in 4.0+;
  the HDFS-specific runs must be repeated once with the command in section 2 (see the report).
* Gold is not partitioned (≈100k rows); `spark.sql.shuffle.partitions` is set to 8.
* Canceled/unavailable orders with items still count in sales metrics (booked, not necessarily revenue).
* Source anomalies are reported, not fixed: 166 orders with carrier pick-up before purchase, 1,359 before
  approval, 23 delivered before pick-up, 8 'delivered' orders without delivery date, 6 canceled orders with
  a delivery date, 610 products without category, 2 categories without translation (`pc_gamer`,
  `portateis_cozinha_e_preparadores_de_alimentos`), 42 geolocation points outside Brazil's bounds,
  80 orders with non-continuous `payment_sequential`, 2 payments with 0 installments, 3 `not_defined` payments.
* Geolocation is validated but not used by any Gold job (not required by the roadmap).

## 7. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Connection refused namenode:9000` | `docker compose -f docker-compose.hadoop.yml up -d namenode datanode`; run via `spark-runner` (hostname resolves only inside the compose network) |
| `Permission denied` writing `/supply-chain/analytics` | Add `-e HADOOP_USER_NAME=hadoop` |
| `Path does not exist ... olist_orders_cleaned.csv` | Upload Silver: `hdfs dfs -put` (see `spark_audit.ps1`) |
| Validation FAIL "Silver matches local Data/cleaned" | HDFS Silver is stale: re-upload with `-put -f`, rerun |
| Validation WARNING "freshness ... not available" | `./Data` not mounted at `/data`; recreate the container after pulling the compose change |
| Review row count too high (~103k instead of 99,224) | CSV was read without `multiLine=true` (comments contain line breaks) |
| `verify_gold` schema FAIL | A job was changed without updating `CONTRACT` and this document |
