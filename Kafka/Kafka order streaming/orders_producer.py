import json
import time
import urllib
from datetime import datetime

from kafka import KafkaProducer
from sqlalchemy import create_engine, text


# ==========================================
# CONFIGURATION
# ==========================================

TOPIC_NAME = 'orders_topic'
BOOTSTRAP_SERVERS = ['localhost:9092']

BATCH_SIZE = 10
BATCH_INTERVAL = 10


# ==========================================
# SQL SERVER CONNECTION (SupplyChain_Source)
# ==========================================

server_name = r'DESKTOP-4RIU2UD\SQLEXPRESS'
database_name = 'SupplyChain_Source'
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
# SQL QUERIES
# ==========================================

extract_orders_query = text("""
    SELECT
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date
    FROM source.orders o
    INNER JOIN source.etl_watermark w
        ON w.pipeline_name = 'orders'
    WHERE
        w.last_order_purchase_timestamp IS NULL
        OR o.order_purchase_timestamp > w.last_order_purchase_timestamp
        OR (
            o.order_purchase_timestamp = w.last_order_purchase_timestamp
            AND o.order_id > w.last_order_id
        )
    ORDER BY
        o.order_purchase_timestamp,
        o.order_id;
""")

update_watermark_query = text("""
    UPDATE source.etl_watermark
    SET
        last_order_purchase_timestamp = :last_ts,
        last_order_id = :last_id
    WHERE pipeline_name = 'orders';
""")


# ==========================================
# HELPERS
# ==========================================

def serialize_value(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    return str(val).strip()


# ==========================================
# CREATE PRODUCER
# ==========================================

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(
        x,
        ensure_ascii=False
    ).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8'),
    acks='all',
    retries=5,
    max_in_flight_requests_per_connection=1,
    linger_ms=10,
    request_timeout_ms=30000
)


# ==========================================
# MAIN PROCESS
# ==========================================

try:
    print("Fetching new orders from SupplyChain_Source...")

    with engine.connect() as conn:
        rows = conn.execute(extract_orders_query).mappings().all()

    total_orders = len(rows)
    print(f"Found {total_orders} new orders.")

    if total_orders == 0:
        print("No new orders to stream.")
    else:
        print("\nStarting Kafka Producer...\n")

        for i in range(0, total_orders, BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            futures = []

            for row in batch:
                order_id = serialize_value(row['order_id'])
                if not order_id:
                    raise ValueError("Missing order_id in source.orders.")

                order_dict = {
                    'order_id': order_id,
                    'customer_id': serialize_value(row['customer_id']),
                    'order_status': serialize_value(row['order_status']),
                    'order_purchase_timestamp': serialize_value(row['order_purchase_timestamp']),
                    'order_approved_at': serialize_value(row['order_approved_at']),
                    'order_delivered_carrier_date': serialize_value(row['order_delivered_carrier_date']),
                    'order_delivered_customer_date': serialize_value(row['order_delivered_customer_date']),
                    'order_estimated_delivery_date': serialize_value(row['order_estimated_delivery_date']),
                }

                future = producer.send(
                    TOPIC_NAME,
                    key=order_id,
                    value=order_dict
                )
                futures.append(future)

            for future in futures:
                metadata = future.get(timeout=30)
                print(
                    f"Sent Order | Partition: {metadata.partition} | "
                    f"Offset: {metadata.offset}"
                )

            producer.flush(timeout=30)

            last_row = batch[-1]
            with engine.begin() as conn:
                conn.execute(
                    update_watermark_query,
                    {
                        'last_ts': last_row['order_purchase_timestamp'],
                        'last_id': serialize_value(last_row['order_id'])
                    }
                )

            next_index = i + len(batch)
            print(f"\n[SUCCESS] Processed rows {i} to {next_index - 1}")
            print(f"Watermark updated to order_id: {last_row['order_id']}")

            if next_index < total_orders:
                print(f"Waiting {BATCH_INTERVAL} seconds...\n")
                time.sleep(BATCH_INTERVAL)

        print("\nAll new orders sent successfully!")

except KeyboardInterrupt:
    print("\nStopping Producer...")

except Exception as e:
    print(f"\n[PRODUCER ERROR] {e}")
    raise

finally:
    producer.close(timeout=30)
    engine.dispose()
    print("Producer Closed.")
