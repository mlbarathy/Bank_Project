
import os
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, String
from app.config import *
from pyspark.sql.functions import col


def load_json_query():
    PARENT_DIR = os.path.dirname(os.path.dirname(__file__))
    query_path = os.path.join(PARENT_DIR, "script", "json_query.py")
    print(query_path)
    namespace = {}
    with open(query_path) as f:
        exec(f.read(), namespace)
    return namespace.get("json_query", "")

def create_table_from_df_if_not_exists(df, table_name, pk_field, engine, include_version=False):
    metadata = MetaData()
    metadata.reflect(bind=engine)

    if table_name in metadata.tables:
        print(f"✅ Table {table_name} already exists.")
        return

    columns = [Column(field.name, String) for field in df.schema.fields]
    if include_version:
        columns.append(Column("version", String))
    new_table = Table(table_name, metadata, *columns)
    new_table.create(bind=engine)
    print(f"🆕 Created table: {table_name} (version column: {include_version})")

def get_existing_ids(spark, table_name, pk_column):
    if not table_name:
        return []
    try:
        existing_df = spark.read.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", table_name) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        return existing_df.select(pk_column).distinct().rdd.flatMap(lambda x: x).collect()
    except Exception as e:
        print(f"⚠️ Could not fetch existing IDs from {table_name}: {e}")
        return []

def get_current_version(spark, pk_value):
    try:
        history_df = spark.read.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_HISTORY_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        versions_df = history_df.filter(col(POSTGRESS_TABLE_PKID) == pk_value) \
            .select("version") \
            .orderBy(col("version").desc())

        return versions_df.first()["version"] if versions_df.count() > 0 else 0
    except:
        return 0

load_json_query()
