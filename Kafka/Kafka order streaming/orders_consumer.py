import json
import time
import urllib
import pandas as pd

from pathlib import Path

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata

from sqlalchemy import create_engine, text


# ==========================================
# CONFIGURATION
# ==========================================

BASE_DIR = Path(__file__).resolve().parent

CUSTOMERS_FILE = (
    BASE_DIR / 'Data' / 'Raw' /
    'olist_customers_dataset.csv'
)

TOPIC_NAME = 'orders_topic'

BOOTSTRAP_SERVERS = ['localhost:9092']

GROUP_ID = 'bronze_orders_group'

BATCH_SIZE = 10

MAX_WAIT_SECONDS = 5


# ==========================================
# SQL SERVER CONNECTION
# ==========================================

server_name = r'DESKTOP-4RIU2UD\SQLEXPRESS'

database_name = 'SupplyChainDB'

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
# LOAD CUSTOMER REFERENCE DATA
# ==========================================

print("Loading customer reference CSV...")

if not CUSTOMERS_FILE.exists():
    raise FileNotFoundError(
        f"Customers CSV not found: {CUSTOMERS_FILE}"
    )

customers_df = pd.read_csv(
    CUSTOMERS_FILE,
    dtype=str
)

customers_df = customers_df.astype(object).where(
    pd.notna(customers_df),
    None
)

customers_lookup = (
    customers_df
    .drop_duplicates(subset=['customer_id'])
    .set_index('customer_id')
    .to_dict(orient='index')
)

print(
    f"Loaded {len(customers_lookup)} customer records."
)


# ==========================================
# SQL QUERIES
# ==========================================

check_customer_query = text("""
    SELECT 1
    FROM bronze.olist_customers_dataset
    WHERE customer_id = :customer_id
""")

insert_customer_query = text("""
    INSERT INTO bronze.olist_customers_dataset (
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    )
    SELECT
        :customer_id,
        :customer_unique_id,
        :customer_zip_code_prefix,
        :customer_city,
        :customer_state
    WHERE NOT EXISTS (
        SELECT 1
        FROM bronze.olist_customers_dataset
            WITH (UPDLOCK, HOLDLOCK)
        WHERE customer_id = :customer_id
    )
""")

insert_order_query = text("""
    INSERT INTO bronze.Orders (
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    )
    SELECT
        :order_id,
        :customer_id,
        :order_status,
        :order_purchase_timestamp,
        :order_approved_at,
        :order_delivered_carrier_date,
        :order_delivered_customer_date,
        :order_estimated_delivery_date
    WHERE NOT EXISTS (
        SELECT 1
        FROM bronze.Orders WITH (UPDLOCK, HOLDLOCK)
        WHERE order_id = :order_id
    )
""")

dead_letter_query = text("""
    INSERT INTO bronze.Dead_Letter (
        topic,
        partition_id,
        message_offset,
        raw_message,
        error_message
    )
    SELECT
        :topic,
        :partition_id,
        :message_offset,
        CAST(:raw_message AS NVARCHAR(MAX)),
        :error_message
    WHERE NOT EXISTS (
        SELECT 1
        FROM bronze.Dead_Letter WITH (UPDLOCK, HOLDLOCK)
        WHERE topic = :topic
          AND partition_id = :partition_id
          AND message_offset = :message_offset
    )
""")


# ==========================================
# VALIDATION & CUSTOMER HANDLING
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


def ensure_customer_exists(conn, customer_id):

    if not customer_id:
        print("[WARNING] Order has no customer_id.")
        return "missing"

    existing_customer = conn.execute(
        check_customer_query,
        {'customer_id': customer_id}
    ).first()

    if existing_customer:
        return "exists"

    customer_data = customers_lookup.get(customer_id)

    if customer_data is None:

        print(
            f"[WARNING] Customer {customer_id} "
            "not found in SQL Server or source CSV."
        )

        return "missing"

    result = conn.execute(
        insert_customer_query,
        {
            'customer_id': customer_id,
            'customer_unique_id':
                clean_str_field(
                    customer_data.get('customer_unique_id'),
                    'customer_unique_id'
                ),
            'customer_zip_code_prefix':
                clean_str_field(
                    customer_data.get('customer_zip_code_prefix'),
                    'customer_zip_code_prefix'
                ),
            'customer_city':
                clean_str_field(
                    customer_data.get('customer_city'),
                    'customer_city'
                ),
            'customer_state':
                clean_str_field(
                    customer_data.get('customer_state'),
                    'customer_state'
                )
        }
    )

    if result.rowcount == 1:
        print(
            f"[NEW CUSTOMER] Added customer: {customer_id}"
        )
        return "inserted"

    return "exists"


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

    dead_letters = []

    for message in batch:

        try:

            if message.value is None:
                raise ValueError(
                    "Empty or tombstone message."
                )

            payload = json.loads(
                message.value.decode('utf-8')
            )

            if not isinstance(payload, dict):
                raise ValueError(
                    "Message must be a JSON object."
                )

            order_id = clean_str_field(
                payload.get('order_id'),
                'order_id'
            )

            if not order_id:
                raise ValueError("Missing order_id.")

            valid_orders.append({

                'order_id': order_id,

                'customer_id': clean_str_field(
                    payload.get('customer_id'),
                    'customer_id'
                ),

                'order_status': clean_str_field(
                    payload.get('order_status'),
                    'order_status'
                ),

                'order_purchase_timestamp': clean_str_field(
                    payload.get('order_purchase_timestamp'),
                    'order_purchase_timestamp'
                ),

                'order_approved_at': clean_str_field(
                    payload.get('order_approved_at'),
                    'order_approved_at'
                ),

                'order_delivered_carrier_date': clean_str_field(
                    payload.get('order_delivered_carrier_date'),
                    'order_delivered_carrier_date'
                ),

                'order_delivered_customer_date': clean_str_field(
                    payload.get('order_delivered_customer_date'),
                    'order_delivered_customer_date'
                ),

                'order_estimated_delivery_date': clean_str_field(
                    payload.get('order_estimated_delivery_date'),
                    'order_estimated_delivery_date'
                )

            })

        except Exception as e:

            raw_message = (
                message.value.decode('utf-8', errors='replace')
                if message.value is not None
                else '<NULL_TOMBSTONE>'
            )

            dead_letters.append({

                'topic': message.topic,

                'partition_id': message.partition,

                'message_offset': message.offset,

                'raw_message': raw_message,

                'error_message': str(e)[:2000]

            })


    # --------------------------------------
    # SQL TRANSACTION
    # --------------------------------------

    inserted_orders = 0

    inserted_customers = 0

    with engine.begin() as conn:

        for order in valid_orders:

            customer_status = ensure_customer_exists(
                conn,
                order['customer_id']
            )

            if customer_status == "inserted":
                inserted_customers += 1

            result = conn.execute(
                insert_order_query,
                order
            )

            if result.rowcount == 1:
                inserted_orders += 1

        if dead_letters:

            conn.execute(
                dead_letter_query,
                dead_letters
            )


    # --------------------------------------
    # MANUAL OFFSET COMMIT
    # --------------------------------------

    offsets = {}

    for message in batch:

        tp = TopicPartition(
            message.topic,
            message.partition
        )

        next_offset = message.offset + 1

        if (
            tp not in offsets
            or next_offset > offsets[tp].offset
        ):

            offsets[tp] = OffsetAndMetadata(
                next_offset,
                ''
            )

    consumer.commit(offsets=offsets)


    # --------------------------------------
    # LOGGING
    # --------------------------------------

    print("\n===================================")

    print(f"Received Messages: {len(batch)}")

    print(f"Valid Orders: {len(valid_orders)}")

    print(f"Inserted Orders: {inserted_orders}")

    print(
        f"Duplicate Orders: "
        f"{len(valid_orders) - inserted_orders}"
    )

    print(f"Inserted Customers: {inserted_customers}")

    print(f"Dead Letter Messages: {len(dead_letters)}")

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

        records = consumer.poll(
            timeout_ms=1000,
            max_records=100
        )

        for messages in records.values():

            if messages and batch_start_time is None:
                batch_start_time = time.monotonic()

            batch_data.extend(messages)

        while len(batch_data) >= BATCH_SIZE:

            current_batch = batch_data[:BATCH_SIZE]

            batch_data = batch_data[BATCH_SIZE:]

            process_batch(current_batch)

            batch_start_time = (
                time.monotonic() if batch_data else None
            )

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

        print(
            f"Processing remaining {len(batch_data)} messages..."
        )

        process_batch(batch_data)

        batch_data = []


except Exception as e:

    print(f"\n[CONSUMER ERROR] {e}")

    raise


finally:

    consumer.close()

    engine.dispose()

    print("Consumer Closed.")