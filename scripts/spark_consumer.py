import os
import sys
import logging
import redis
import json
import time
import base64
from datetime import datetime, timezone
from pymongo import MongoClient, ReplaceOne

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ROOT_DIR
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from config.env_config import EnvConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("StreamSyncSparkConsumer") \
    .getOrCreate()


# Decode Debezium base64-encoded decimal fields
def decode_decimal(b64str):
    try:
        raw = base64.b64decode(b64str)
        int_val = int.from_bytes(raw, byteorder='big', signed=True)
        return float(int_val) / 100
    except Exception as e:
        logger.error(f"Decimal decode error: {e}")
        return None


# Convert days to UTC date
def decode_date(days):
    try:
        return datetime.fromtimestamp(days * 86400, tz=timezone.utc)
    except Exception as e:
        logger.error(f"Date decode error: {e}")
        return None


# Parse debezium payload
def parse_payload(payload, topic):
    try:
        result = {}
        for key, val in payload.items():
            if topic == EnvConfig.TOPIC_ORDERS and key == 'total_amount':
                result[key] = decode_decimal(val)
            elif topic == EnvConfig.TOPIC_SHIPPING and key == 'estimated_delivery':
                result[key] = decode_date(val)
            else:
                result[key] = val
        return result
    except Exception as e:
        logger.error(f"Payload parsing failed: {e}")
        return {}

def process_partition(rows, collection_name, topic, redis_update=False):
    try:
        redis_client = redis.Redis(
            host=EnvConfig.REDIS_HOST,
            port=EnvConfig.REDIS_PORT,
            decode_responses=True
        )
        redis_client.ping()
    except redis.ConnectionError:
        logger.warning("Redis unavailable during partition processing")
        redis_client = None

    try:
        mongo_client = MongoClient(EnvConfig.MONGO_URI)
        mong_db = mongo_client.get_database()
        mongo_col = mong_db[collection_name]
    except Exception as e:
        logger.error(f"MongoDB connection failed during partition: {e}")
        return

    bulk_ops = []

    for row in rows:
        try:
            msg = json.loads(row.value.decode("utf-8"))
            payload = msg.get("payload", {})
            op = payload.get("op")
            before = payload.get("before")
            after = payload.get("after")

            if op not in ('c', 'u', 'd'):
                continue

            if op in ('c', 'u') and after:
                parsed = parse_payload(after, topic)
                pk_field = list(parsed.keys())[0] if parsed else None
                
                if pk_field:
                    bulk_ops.append(
                        ReplaceOne({pk_field: parsed[pk_field]}, parsed, upsert=True)
                    )
                    if redis_update and redis_client and "order_id" in parsed:
                        redis_client.set(f"order:{parsed['order_id']}", parsed.get("status", ""))
            elif op == 'd' and before:
                parsed = parse_payload(before, topic)
                pk_field = list(parsed.keys())[0] if parsed else None

                if pk_field:
                    mongo_col.delete_one({pk_field: parsed[pk_field]})
                    if redis_update and redis_client and "order_id" in parsed:
                        redis_client.delete(f"order:{parsed['order_id']}")
        except Exception as e:
            logger.error(f"Error processing record: {e}")

    if bulk_ops:
        mongo_col.bulk_write(bulk_ops)
        logger.info(f"[{collection_name}] - Upserted {len(bulk_ops)} documents")
    else:
        logger.info(f"[{collection_name}] - No valid operations found")


def process_stream(topic, collection_name, redis_update=False):
    logger.info(f"Listening on {topic}...")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", EnvConfig.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    return df.select(col("value")).writeStream \
        .foreachBatch(lambda batch_df, _: batch_df.rdd.foreachPartition(
            lambda rows: process_partition(rows, collection_name, topic, redis_update)
        )) \
        .start()

def main():
    customers_query = process_stream(EnvConfig.TOPIC_CUSTOMERS, "customers")
    orders_query = process_stream(EnvConfig.TOPIC_ORDERS, "orders", redis_update=True)
    shipping_query = process_stream(EnvConfig.TOPIC_SHIPPING, "shipping_info")

    logger.info("Spark streaming is running. Press Ctrl+C to stop...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Graceful shutdown initiated...")
        customers_query.stop()
        orders_query.stop()
        shipping_query.stop()
        spark.stop()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    main()
