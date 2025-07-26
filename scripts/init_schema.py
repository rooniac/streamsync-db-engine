import os
import sys
import logging
import mysql.connector

# ========== PATH CONFIG ==========
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from config.env_config import EnvConfig

# ========== LOGGER ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def execute_sql_file(sql_path):
    if not os.path.exists(sql_path):
        logger.error(f"SQL file not found: {sql_path}")
        return

    with open(sql_path, 'r', encoding='utf-8') as f:
        sql_statements = f.read()

    try:
        conn = mysql.connector.connect(
            host=EnvConfig.MYSQL_HOST,
            port=int(EnvConfig.MYSQL_PORT),
            user=EnvConfig.MYSQL_USER,
            password=EnvConfig.MYSQL_PASSWORD
        )
        cursor = conn.cursor()
        logger.info("Connected to MySQL successfully.")

        for statement in sql_statements.split(";"):
            stmt = statement.strip()
            if stmt:
                try:
                    cursor.execute(stmt)
                except mysql.connector.Error as err:
                    logger.error(f"MySQL Error: {err}\n>>> {stmt}")

        conn.commit()
        logger.info("MySQL schema created successfully.")

    except mysql.connector.Error as conn_err:
        logger.error(f"MySQL connection error: {conn_err}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            logger.info("MySQL connection closed.")

if __name__ == "__main__":
    execute_sql_file("sql/create_tables.sql")
