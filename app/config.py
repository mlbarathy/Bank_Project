import os
from dotenv import load_dotenv
load_dotenv()
import uuid


TARGET_DB= "POSTGRES"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "json_events"

### POSTGRESS DETAILS

POSTGRES_USER =  "postgress"
POSTGRES_PASSWORD = "postgress"
POSTGRES_DB = "mydatabase"
POSTGRES_TABLE = "bank_json"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5433"
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_CON_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRESS_TABLE_PKID = "MsgId"
POSTGRES_HISTORY_TABLE = "bank_json_history"
CHECKPOINT_LOC = f"/Users/lakshimi.mariappan/Desktop/Personal/Project/Bank_V1_28May/checkpoints/flaskstream_{uuid.uuid4()}"
POSTGRES_DRIVER="org.postgresql.Driver"

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

### MSSQL DETAILS

MSSQL_URL="jdbc:sqlserver://localhost:1433;databaseName=your_database"
MSSQL_USER="sa"
MSSQL_PASSWORD="mssql*0123"
MSSQL_DRIVER="com.microsoft.sqlserver.jdbc.SQLServerDriver"
MSSQL_TABLE="bank_json"
MSSQL_HISTORY_TABLE="bank_json_history"
MSSQL_TABLE_PKID="MsgId"

