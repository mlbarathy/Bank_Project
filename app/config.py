import os
from dotenv import load_dotenv
load_dotenv()
import uuid

import os
import uuid
from dotenv import load_dotenv

load_dotenv()

# Set target DB: "POSTGRES", "MSSQL", or "MYSQL"

TARGET_DB = "MYSQL"

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "json_events"
CHECKPOINT_LOC = f"/Users/lakshimi.mariappan/Desktop/Personal/Project/Bank_V1_28May/checkpoints/flaskstream_{uuid.uuid4()}"

# ============================ #
#       POSTGRES CONFIG       #
# ============================ #
POSTGRES_USER = "postgress"
POSTGRES_PASSWORD = "postgress"
POSTGRES_DB = "mydatabase"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5433"
POSTGRES_TABLE = "bank_json"
POSTGRES_HISTORY_TABLE = "bank_json_history"
POSTGRESS_TABLE_PKID = "MsgId"
POSTGRES_DRIVER = "org.postgresql.Driver"

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_CON_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

POSTGRESS_CON = {
    "dbname": POSTGRES_DB,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "host": POSTGRES_HOST,
    "port": POSTGRES_PORT,
}

POSTGRESS_SPARK_CONF = {
    "url": POSTGRES_URL,
    "dbtable": POSTGRES_TABLE,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": POSTGRES_DRIVER
}

# ============================ #
#         MYSQL CONFIG        #
# ============================ #
MYSQL_USER = "mysql"
MYSQL_PASSWORD = "mysql*0123"
MYSQL_DATABASE = "mydb"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_TABLE = "bank_json"
MYSQL_HISTORY_TABLE = "bank_json_history"
MYSQL_TABLE_PKID = "MsgId"

MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
MYSQL_CON_URL = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

MYSQL_CON = {
    "host": MYSQL_HOST,
    "port": MYSQL_PORT,
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "database": MYSQL_DATABASE,
}

MYSQL_SPARK_CONF = {
    "url": MYSQL_URL,
    "dbtable": MYSQL_TABLE,
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": MYSQL_DRIVER
}
