import os
from dotenv import load_dotenv
load_dotenv()
import uuid

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "json_events"
POSTGRES_USER =  "postgress"
POSTGRES_PASSWORD = "postgress"
POSTGRES_DB = "mydatabase"
POSTGRES_TABLE = "bank_json"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5433"
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

CHECKPOINT_LOC = f"/Users/lakshimi.mariappan/Desktop/Personal/Project/Bank_V1_28May/checkpoints/flaskstream_{uuid.uuid4()}"

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
    "driver": "org.postgresql.Driver"
}