import json
from datetime import datetime
from kafka import KafkaConsumer
import pyodbc

# ==========================================
# CONFIGURATION
# ==========================================
TOPIC_NAME = "orders"
BOOTSTRAP_SERVERS = ["localhost:9092"]
GROUP_ID = "stg_orders_consumer_group"

SERVER_NAME = r"DESKTOP-KMK92SK\SQLEXPRESS01"
DATABASE_NAME = "SupplyChain_DWH"
DRIVER_NAME = "ODBC Driver 17 for SQL Server"

REQUIRED_FIELDS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
]

# ==========================================
# HELPERS
# ==========================================
def parse_datetime(value):
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        return value
    val_str = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(val_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Invalid timestamp format: {value}")


def validate_order(data):
    if not isinstance(data, dict):
        raise ValueError("Message payload must be a JSON object.")

    for field in REQUIRED_FIELDS:
        if field not in data or data[field] is None or str(data[field]).strip() == "":
            raise ValueError(f"Missing or empty required field: {field}")

    order_id = str(data["order_id"]).strip()
    customer_id = str(data["customer_id"]).strip()
    order_status = str(data["order_status"]).strip()
    purchase_ts = parse_datetime(data["order_purchase_timestamp"])

    if len(order_id) > 50:
        raise ValueError("order_id exceeds 50 characters.")
    if len(customer_id) > 50:
        raise ValueError("customer_id exceeds 50 characters.")
    if len(order_status) > 20:
        raise ValueError("order_status exceeds 20 characters.")
    if purchase_ts is None:
        raise ValueError("order_purchase_timestamp cannot be null.")

    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_status": order_status,
        "order_purchase_timestamp": purchase_ts,
        "order_approved_at": parse_datetime(data.get("order_approved_at")),
        "order_delivered_carrier_date": parse_datetime(data.get("order_delivered_carrier_date")),
        "order_delivered_customer_date": parse_datetime(data.get("order_delivered_customer_date")),
        "order_estimated_delivery_date": parse_datetime(data.get("order_estimated_delivery_date")),
    }


def get_sql_connection():
    conn_str = (
        f"DRIVER={{{DRIVER_NAME}}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        f"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)


def insert_order_to_staging(cursor, order_data):
    sql = """
        EXEC staging.sp_insert_stg_order
            @order_id = ?,
            @customer_id = ?,
            @order_status = ?,
            @order_purchase_timestamp = ?,
            @order_approved_at = ?,
            @order_delivered_carrier_date = ?,
            @order_delivered_customer_date = ?,
            @order_estimated_delivery_date = ?
    """
    params = (
        order_data["order_id"],
        order_data["customer_id"],
        order_data["order_status"],
        order_data["order_purchase_timestamp"],
        order_data["order_approved_at"],
        order_data["order_delivered_carrier_date"],
        order_data["order_delivered_customer_date"],
        order_data["order_estimated_delivery_date"],
    )
    cursor.execute(sql, params)


# ==========================================
# MAIN CONSUMER LOOP
# ==========================================
def main():
    print("Connecting to SQL Server...")
    sql_conn = get_sql_connection()
    sql_cursor = sql_conn.cursor()

    print(f"Connecting to Kafka ({BOOTSTRAP_SERVERS}) and subscribing to '{TOPIC_NAME}'...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print("Consumer is running. Waiting for messages...\n")

    try:
        for message in consumer:
            try:
                order_data = validate_order(message.value)
                insert_order_to_staging(sql_cursor, order_data)

                # Commit SQL transaction first
                sql_conn.commit()

                # Commit Kafka offset only after SQL commit succeeds
                consumer.commit()

                print(f"[OK] Processed order_id={order_data['order_id']} (offset={message.offset})")

            except json.JSONDecodeError as e:
                sql_conn.rollback()
                print(f"[JSON ERROR] offset={message.offset} -> {e}")

            except ValueError as e:
                sql_conn.rollback()
                print(f"[VALIDATION ERROR] offset={message.offset} -> {e}")

            except pyodbc.Error as e:
                sql_conn.rollback()
                print(f"[SQL ERROR] offset={message.offset} -> {e}")

            except Exception as e:
                sql_conn.rollback()
                print(f"[UNEXPECTED ERROR] offset={message.offset} -> {e}")

    except KeyboardInterrupt:
        print("\nStopping Consumer...")

    finally:
        consumer.close()
        sql_cursor.close()
        sql_conn.close()
        print("Connections closed cleanly.")


if __name__ == "__main__":
    main()