import pandas as pd
import time
import json
import os

from pathlib import Path
from kafka import KafkaProducer


# ==========================================
# CONFIGURATION
# ==========================================

BASE_DIR = Path(__file__).resolve().parent

FILE_PATH = (
    BASE_DIR / 'Data' / 'Raw' /
    'olist_orders_dataset.csv'
)

WATERMARK_FILE = BASE_DIR / 'watermark.txt'

TOPIC_NAME = 'orders_topic'

BOOTSTRAP_SERVERS = ['localhost:9092']

BATCH_SIZE = 10

BATCH_INTERVAL = 10


# ==========================================
# WATERMARK FUNCTIONS
# ==========================================

def read_watermark():

    if not WATERMARK_FILE.exists():
        return 0

    content = WATERMARK_FILE.read_text().strip()

    if not content:
        return 0

    try:
        value = int(content)
    except ValueError:
        raise ValueError("Invalid watermark file!")

    if value < 0:
        raise ValueError("Watermark cannot be negative!")

    return value


def save_watermark(next_index):

    temp_file = WATERMARK_FILE.with_suffix('.tmp')

    temp_file.write_text(str(next_index))

    os.replace(temp_file, WATERMARK_FILE)


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

    print(f"Reading orders: {FILE_PATH}")

    if not FILE_PATH.exists():
        raise FileNotFoundError(FILE_PATH)

    df_orders = pd.read_csv(
        FILE_PATH,
        dtype=str
    )

    df_orders = df_orders.astype(object).where(
        pd.notna(df_orders),
        None
    )

    start_index = read_watermark()

    if start_index > len(df_orders):
        raise ValueError(
            "Watermark exceeds CSV row count."
        )

    print(f"Total Orders: {len(df_orders)}")

    print(f"Starting from row: {start_index}")

    print("\nStarting Kafka Producer...\n")


    # ======================================
    # STREAMING LOOP
    # ======================================

    for i in range(
        start_index,
        len(df_orders),
        BATCH_SIZE
    ):

        batch = df_orders.iloc[
            i:i + BATCH_SIZE
        ]

        futures = []

        for row_idx, row in batch.iterrows():

            order_dict = row.to_dict()

            order_id = order_dict.get('order_id')

            if not order_id or not str(order_id).strip():
                raise ValueError(
                    f"Missing order_id at row {row_idx}."
                )

            future = producer.send(

                TOPIC_NAME,

                key=str(order_id).strip(),

                value=order_dict

            )

            futures.append(future)


        for future in futures:

            metadata = future.get(timeout=30)

            print(
                f"Sent Order | Partition: "
                f"{metadata.partition} | "
                f"Offset: {metadata.offset}"
            )


        producer.flush(timeout=30)


        next_index = i + len(batch)

        save_watermark(next_index)


        print(
            f"\n[SUCCESS] Processed rows "
            f"{i} to {next_index - 1}"
        )

        print(f"Watermark: {next_index}")


        if next_index < len(df_orders):

            print(
                f"Waiting {BATCH_INTERVAL} seconds...\n"
            )

            time.sleep(BATCH_INTERVAL)


    print("\nAll Orders sent successfully!")


except KeyboardInterrupt:

    print("\nStopping Producer...")


except Exception as e:

    print(f"\n[PRODUCER ERROR] {e}")

    raise


finally:

    producer.close(timeout=30)

    print("Producer Closed.")