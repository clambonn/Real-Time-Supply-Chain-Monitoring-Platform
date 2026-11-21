# Kafka Consumer — `staging.stg_orders` (`SupplyChain_DWH`)

## Overview
This module implements the Kafka Consumer for the `SupplyChain_DWH` data warehouse pipeline. It consumes order events from the `orders` Kafka topic, parses JSON payloads, validates required fields and data formats, and inserts records into `staging.stg_orders` via the stored procedure `staging.sp_insert_stg_order` with idempotency (duplicate prevention), error handling, and explicit commit ordering (SQL Server commit before Kafka offset commit).

## Alignment & Specifications
- **Kafka Broker & Topic:** `localhost:9092`, topic `orders`, consumer group configured for staging ingestion.
- **JSON Payload Schema (Producer Alignment):**
  - `order_id`
  - `customer_id`
  - `order_status`
  - `order_purchase_timestamp`
- **Target Database & Table (Staging Alignment):**
  - Database: `SupplyChain_DWH`
  - Table: `staging.stg_orders` (`order_id` as `PRIMARY KEY` / unique identifier, plus required order attributes and `ingested_at`).
  - Stored Procedure: `staging.sp_insert_stg_order`.

## Project Files
- `consumer.py`: Main consumer script reading from Kafka, validating payloads, calling `staging.sp_insert_stg_order`, and managing commits/errors.
- `requirements.txt`: Python dependencies (`kafka-python`, `pyodbc`).
- `README.md`: Setup, execution, and testing guide.

## Prerequisites
1. Kafka Broker running on `localhost:9092` with the `orders` topic created.
2. SQL Server running with the `SupplyChain_DWH` database, `staging.stg_orders` table, and `staging.sp_insert_stg_order` stored procedure created.
3. Python environment (Anaconda / Python 3) with required packages installed.

## Installation
From the `kafka_consumer` directory, install the dependencies:

```powershell
C:\Users\dell\anaconda3\python.exe -m pip install -r requirements.txt

```

## Running the Consumer
Start the consumer from the `kafka_consumer` directory:

```powershell
C:\Users\dell\anaconda3\python.exe consumer.py
```

Keep this terminal running while messages are published to the `orders` topic.

## Testing & Verification
1. **Start Consumer:** Run `consumer.py` and wait until it connects to SQL Server and Kafka and starts listening on `orders`.
2. **Send Test & Duplicate Orders:** Publish valid order messages (including multiple orders and a duplicate `order_id`) to the `orders` topic.
3. **Verify in SQL Server (SSMS):** Query `staging.stg_orders` to confirm new orders are inserted:
   ```sql
   SELECT order_id, customer_id, order_status, order_purchase_timestamp, ingested_at
   FROM staging.stg_orders
   ORDER BY ingested_at DESC;
   ```
4. **Verify Duplicate Prevention:** Confirm no `order_id` appears more than once:
   ```sql
   SELECT order_id, COUNT(*) AS cnt
   FROM staging.stg_orders
   GROUP BY order_id
   HAVING COUNT(*) > 1;
   ```