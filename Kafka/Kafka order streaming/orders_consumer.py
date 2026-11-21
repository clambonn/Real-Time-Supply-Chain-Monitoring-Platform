import json
import time
import urllib

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata
from sqlalchemy import create_engine, text


# ==========================================
# CONFIGURATION
# ==========================================

TOPIC_NAME = 'orders_topic'
BOOTSTRAP_SERVERS = ['localhost:9092']
GROUP_ID = 'dwh_staging_orders_group'

BATCH_SIZE = 10
MAX_WAIT_SECONDS = 5


# ==========================================
# SQL SERVER CONNECTION (SupplyChain_DWH)
# ==========================================

server_name = r'DESKTOP-4RIU2UD\SQLEXPRESS'
database_name = 'SupplyChain_DWH'
driver = 'ODBC Driver 17 for SQL Server'

params = urllib.parse.quote_plus(
    f"DRIVER={{{driver}}};"
    f"SERVER={server_name};"
    f"DATABASE={database_name};"
    f"Trusted_Connection=yes"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    pool_pre_ping=True
)


# ==========================================
# SQL STORED PROCEDURE
# ==========================================

insert_order_sp = text("""
    EXEC staging.sp_insert_stg_order
        @order_id = :order_id,
        @customer_id = :customer_id,
        @order_status = :order_status,
        @order_purchase_timestamp = :order_purchase_timestamp,
        @order_approved_at = :order_approved_at,
        @order_delivered_carrier_date = :order_delivered_carrier_date,
        @order_delivered_customer_date = :order_delivered_customer_date,
        @order_estimated_delivery_date = :order_estimated_delivery_date
""")


# ==========================================
# VALIDATION
# ==========================================

def clean_str_field(value, field_name, max_len=255):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        raise ValueError(
            f"Field '{field_name}' cannot be a JSON object or array."
        )
    text_val = str(value).strip()
    if len(text_val) > max_len:
        raise ValueError(
            f"Field '{field_name}' exceeds max length of {max_len}."
        )
    return text_val if text_val else None


# ==========================================
# CREATE KAFKA CONSUMER
# ==========================================

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    max_poll_records=100
)


# ==========================================
# PROCESS BATCH
# ==========================================

def process_batch(batch):
    if not batch:
        return

    valid_orders = []
    failed_count = 0

    for message in batch:
        try:
            if message.value is None:
                raise ValueError("Empty or tombstone message.")

            payload = json.loads(message.value.decode('utf-8'))

            if not isinstance(payload, dict):
                raise ValueError("Message must be a JSON object.")

            order_id = clean_str_field(payload.get('order_id'), 'order_id', 50)
            if not order_id:
                raise ValueError("Missing order_id.")

            valid_orders.append({
                'order_id': order_id,
                'customer_id': clean_str_field(payload.get('customer_id'), 'customer_id', 50),
                'order_status': clean_str_field(payload.get('order_status'), 'order_status', 20),
                'order_purchase_timestamp': clean_str_field(payload.get('order_purchase_timestamp'), 'order_purchase_timestamp'),
                'order_approved_at': clean_str_field(payload.get('order_approved_at'), 'order_approved_at'),
                'order_delivered_carrier_date': clean_str_field(payload.get('order_delivered_carrier_date'), 'order_delivered_carrier_date'),
                'order_delivered_customer_date': clean_str_field(payload.get('order_delivered_customer_date'), 'order_delivered_customer_date'),
                'order_estimated_delivery_date': clean_str_field(payload.get('order_estimated_delivery_date'), 'order_estimated_delivery_date'),
            })

        except Exception as e:
            failed_count += 1
            print(
                f"[INVALID MESSAGE] Partition: {message.partition} | "
                f"Offset: {message.offset} | Error: {e}"
            )

    # --------------------------------------
    # SQL TRANSACTION
    # --------------------------------------
    with engine.begin() as conn:
        for order in valid_orders:
            conn.execute(insert_order_sp, order)

    # --------------------------------------
    # MANUAL OFFSET COMMIT
    # --------------------------------------
    offsets = {}
    for message in batch:
        tp = TopicPartition(message.topic, message.partition)
        next_offset = message.offset + 1
        if tp not in offsets or next_offset > offsets[tp].offset:
            offsets[tp] = OffsetAndMetadata(next_offset, '')

    consumer.commit(offsets=offsets)

    # --------------------------------------
    # LOGGING
    # --------------------------------------
    print("\n===================================")
    print(f"Received Messages: {len(batch)}")
    print(f"Processed Orders (sp_insert_stg_order): {len(valid_orders)}")
    print(f"Invalid Messages: {failed_count}")
    print("SQL Transaction: SUCCESS")
    print("Kafka Offset Commit: SUCCESS")
    print("===================================\n")


# ==========================================
# MAIN LOOP
# ==========================================

batch_data = []
batch_start_time = None

print("\nKafka Consumer Started")
print(f"Topic: {TOPIC_NAME}")
print(f"Group ID: {GROUP_ID}")
print(f"Batch Size: {BATCH_SIZE}\n")

try:
    while True:
        records = consumer.poll(timeout_ms=1000, max_records=100)

        for messages in records.values():
            if messages and batch_start_time is None:
                batch_start_time = time.monotonic()
            batch_data.extend(messages)

        while len(batch_data) >= BATCH_SIZE:
            current_batch = batch_data[:BATCH_SIZE]
            batch_data = batch_data[BATCH_SIZE:]
            process_batch(current_batch)
            batch_start_time = time.monotonic() if batch_data else None

        if (
            batch_data
            and batch_start_time is not None
            and time.monotonic() - batch_start_time >= MAX_WAIT_SECONDS
        ):
            print("Processing partial batch after timeout...")
            current_batch = batch_data
            batch_data = []
            batch_start_time = None
            process_batch(current_batch)

except KeyboardInterrupt:
    print("\nStopping Consumer...")
    if batch_data:
        print(f"Processing remaining {len(batch_data)} messages...")
        process_batch(batch_data)
        batch_data = []

except Exception as e:
    print(f"\n[CONSUMER ERROR] {e}")
    raise

finally:
    consumer.close()
    engine.dispose()
    print("Consumer Closed.")
