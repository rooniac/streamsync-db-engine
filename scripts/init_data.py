import os
import sys
import logging
import random
from faker import Faker
import mysql.connector

# ========== PATH CONFIG ==========
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from config.env_config import EnvConfig

# ========== LOGGER ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

fake = Faker()

def insert_data():
    try:
        conn = mysql.connector.connect(
            host=EnvConfig.MYSQL_HOST,
            port=int(EnvConfig.MYSQL_PORT),
            user=EnvConfig.MYSQL_USER,
            password=EnvConfig.MYSQL_PASSWORD,
            database=EnvConfig.MYSQL_DATABASE
        )
        cursor = conn.cursor()
        logger.info("Connected to MySQL.")

        # Insert customers
        customers = []
        emails = set()

        while len(customers) < 300:
            name = fake.name()
            email = fake.email()
            if email in emails:
                continue
            emails.add(email)
            address = fake.address().replace("\n", ", ")
            customers.append((name, email, address))

        cursor.executemany(
            "INSERT INTO customers (name, email, address) VALUES (%s, %s, %s)",
            customers
        )
        conn.commit()
        logger.info("Inserted 300 customers")

        # Get customer IDs
        cursor.execute("SELECT customer_id FROM customers")
        customer_ids = [row[0] for row in cursor.fetchall()]

        order_statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
        shipping_methods = ["Standard", "Express", "Next-day", "Pickup"]

        # Insert orders + shipping_info
        for _ in range(1000):
            customer_id = random.choice(customer_ids)
            total_amount = round(random.uniform(10.0, 500.0), 2)
            status = random.choice(order_statuses)

            cursor.execute(
                "INSERT INTO orders (customer_id, total_amount, status) VALUES (%s, %s, %s)",
                (customer_id, total_amount, status)
            )
            order_id = cursor.lastrowid

            shipping_address = fake.address().replace("\n", ", ")
            method = random.choice(shipping_methods)
            eta = fake.date_between(start_date="+1d", end_date="+10d")
            ship_status = "ready" if status in ["processing", "shipped"] else "pending"

            cursor.execute(
                "INSERT INTO shipping_info (order_id, shipping_address, shipping_method, estimated_delivery, status) VALUES (%s, %s, %s, %s, %s)",
                (order_id, shipping_address, method, eta, ship_status)
            )

        conn.commit()
        logger.info("Inserted 1000 orders + shipping_info")

    except mysql.connector.Error as err:
        logger.error(f"MySQL error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            logger.info("MySQL connection closed.")

if __name__ == "__main__":
    insert_data()
