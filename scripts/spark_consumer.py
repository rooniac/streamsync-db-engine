import os
import sys
import logging
import redis
import json
import time
from pymongo import MongoClient, ReplaceOne

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# ROOT_DIR
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from config.env_config import EnvConfig

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

# Redis connection
try:
    redis_client = redis.Redis(
        host=EnvConfig.REDIS_HOST,
        port=EnvConfig.REDIS_PORT,
        decode_responses=True
    )
    redis_client.ping()
    logger.info("Connected to Redis")
except redis.ConnectionError:
    logger.error("Redis connection failed")
    sys.exit(1)

# MongoDB connection
try:
    mongo_client = MongoClient(EnvConfig.MONGO_URI)
    mong_db = mongo_client.get_database()
    logger.info("Connected to MongoDB")
except Exception as e:
    logger.error(f"MongoDB connection failed: {e}")
    sys.exit(1)

# Spark session
spark = SparkSession.builder \
    .appName("StreamSyncSparkConsumer") \
    .getOrCreate()

# Schemas
customer_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("address", StringType()),
    StructField("created_at", TimestampType())
])

order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("total_amount", DoubleType()),
    StructField("status", StringType()),
    StructField("created_at", TimestampType()),
    StructField("updated_at", TimestampType())
])

shipping_schema = StructType([
    StructField("shipping_id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("shipping_address", StringType()),
    StructField("shipping_method", StringType()),
    StructField("estimated_delivery", DateType()),
    StructField("status", StringType())
])

# Read Kafka stream
def get_kafka_df(topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", EnvConfig.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

# Partition processing
def process_partition(rows, schema, collection_name, redis_update=False):
    mongo_col = mong_db[collection_name]
    bulk_ops = []

    for row in rows:
        try:
            msg = json.loads(row.value.decode("utf-8"))
            payload = msg.get("payload", {})
            after = payload.get("after")
            op = payload.get("op")

            if not after:
                continue

            if op in ('c', 'u'):
                pk_field = list(after.keys())[0]
                bulk_ops.append(
                    ReplaceOne({pk_field: after[pk_field]}, after, upsert=True)
                )

                if redis_update and "order_id" in after:
                    redis_client.set(f"order:{after['order_id']}", after.get("status", ""))

            elif op == 'd':
                pk_field = list(after.keys())[0]
                mongo_col.delete_one({pk_field: after[pk_field]})
                if redis_update and "order_id" in after:
                    redis_client.delete(f"order:{after['order_id']}")

        except Exception as e:
            logger.error(f"Error processing record: {e}")

    if bulk_ops:
        mongo_col.bulk_write(bulk_ops)
        logger.info(f"[{collection_name}] - Upserted {len(bulk_ops)} documents")

# Stream processor
def process_stream(df, schema, collection_name, redis_update=False):
    return df.select(col("value")).writeStream \
        .foreachBatch(lambda batch_df, _: batch_df.rdd.foreachPartition(
            lambda rows: process_partition(rows, schema, collection_name, redis_update)
        )) \
        .start()

# Main function
def main():
    customers_df = get_kafka_df(EnvConfig.TOPIC_CUSTOMERS)
    orders_df = get_kafka_df(EnvConfig.TOPIC_ORDERS)
    shipping_df = get_kafka_df(EnvConfig.TOPIC_SHIPPING)

    customers_query = process_stream(customers_df, customer_schema, "customers")
    orders_query = process_stream(orders_df, order_schema, "orders", redis_update=True)
    shipping_query = process_stream(shipping_df, shipping_schema, "shipping_info")

    logger.info("Spark streaming is running. Press Ctrl+C to stop...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Graceful shutdown initiated...")

        customers_query.stop()
        orders_query.stop()
        shipping_query.stop()

        logger.info("All queries stopped. Stopping Spark session...")
        spark.stop()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    main()
