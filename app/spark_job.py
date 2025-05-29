from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import psycopg2
import os
from app.config import *
from app.script.json_query import json_query
from app.datatypes import *
from pyspark.sql.types import *

load_dotenv()

def create_spark_session():
    return (
        SparkSession.builder
        .appName("KafkaToPostgres")
        .config("spark.jars.packages", ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "org.postgresql:postgresql:42.2.27"
        ]))
        .master("local[*]")
        .getOrCreate()
    )

def load_json_query():
    query_path = os.path.join(os.path.dirname(__file__), "script", "json_query.py")
    namespace = {}
    with open(query_path) as f:
        exec(f.read(), namespace)
    return namespace.get("json_query", "")

def read_from_kafka(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10)
        .load()
    )

def get_postgres_table_schema(spark):
    try:
        df_pg = spark.read.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return set(df_pg.schema.names)
    except Exception:
        return set()

def spark_to_postgres_type(spark_type):

    mapping = {
        StringType(): "TEXT",
        IntegerType(): "INTEGER",
        LongType(): "BIGINT",
        DoubleType(): "DOUBLE PRECISION",
        FloatType(): "REAL",
        BooleanType(): "BOOLEAN",
        TimestampType(): "TIMESTAMP",
        DateType(): "DATE",
    }
    return mapping.get(spark_type, "TEXT")

def add_missing_columns_to_postgres(missing_columns_with_types):
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        cursor = conn.cursor()
        for col, col_type in missing_columns_with_types:
            cursor.execute(f'ALTER TABLE {POSTGRES_TABLE} ADD COLUMN IF NOT EXISTS "{col}" {col_type};')
        conn.commit()
        cursor.close()
        conn.close()
        print(f"➕ Added missing columns: {[col for col, _ in missing_columns_with_types]}")
    except Exception as e:
        print(f"❌ Error altering PostgreSQL schema: {e}")

def create_table_from_df(result_df):
    try:
        columns_with_types = [
            f'"{field.name}" {spark_to_postgres_type(field.dataType)}'
            for field in result_df.schema.fields
        ]
        create_stmt = f'CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} ({", ".join(columns_with_types)});'
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        cursor = conn.cursor()
        cursor.execute(create_stmt)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"🆕 Created PostgreSQL table '{POSTGRES_TABLE}' with schema from result_df")
    except Exception as e:
        print(f"❌ Error creating PostgreSQL table: {e}")

def writestream_non_empty_batches(batch_df, batch_id):
    spark = batch_df.sparkSession
    value_df = batch_df.selectExpr("CAST(value AS STRING) as value")

    if value_df.rdd.isEmpty():
        print(f"⚠️ Skipping empty batch (id: {batch_id})")
        return

    value_df.createOrReplaceTempView("messageView")
    try:
        result_df = spark.sql(load_json_query())
        result_df.show(100, False)

        if result_df.rdd.isEmpty():
            print(f"⚠️ No data after SQL extraction in batch {batch_id}")
            return

        postgres_cols = get_postgres_table_schema(spark)
        result_cols = set(result_df.columns)

        if not postgres_cols:
            create_table_from_df(result_df)
            postgres_cols = set(result_df.columns)  # Update after creation

        missing_cols = result_cols - postgres_cols
        if missing_cols:
            missing_types = [(col, spark_to_postgres_type(result_df.schema[col].dataType)) for col in missing_cols]
            add_missing_columns_to_postgres(missing_types)

        result_df.write.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(f"✅ Batch {batch_id} written to PostgreSQL")

    except AnalysisException as ae:
        print(f"❌ SQL error in batch {batch_id}: {ae}")
    except Exception as e:
        print(f"❌ Error writing batch {batch_id}: {e}")

def start_spark_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    kafka_df = read_from_kafka(spark)
    query = (
        kafka_df.writeStream
        .queryName("KafkaToPostgresStream")
        .foreachBatch(writestream_non_empty_batches)
        .option("checkpointLocation", CHECKPOINT_LOC)
        .outputMode("append")
        .start()
    )

    print("🚀 Spark streaming job started.")
    print(f"📡 Kafka Topic: {KAFKA_TOPIC}")
    print(f"🗄️  Target PostgreSQL Table: {POSTGRES_TABLE}")
    query.awaitTermination()

def main():
    start_spark_stream()

if __name__ == '__main__':
    main()
