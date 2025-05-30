

from pyspark.sql import SparkSession,DataFrame
from app.utils.functions import *
from pyspark.sql.functions import col, lit, max as spark_max, coalesce
from pyspark.sql.utils import AnalysisException
import psycopg2

load_dotenv()

def create_spark_session():
    return (
        SparkSession.builder
        .appName("KafkaToPostgres")
        .config("spark.jars.packages", ",".join(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0","org.postgresql:postgresql:42.2.27"]))
        .master("local[*]")
        .getOrCreate()
    )

def writestream_non_empty_batches(batch_df, batch_id):

    spark = batch_df.sparkSession
    value_df = batch_df.selectExpr("CAST(value AS STRING) as value")

    if value_df.rdd.isEmpty():
        print(f"⚠️ Skipping empty batch (id: {batch_id})")
        return

    value_df.createOrReplaceTempView("messageView")

    try:
        query_string = load_json_query()
        result_df = spark.sql(query_string)

        print(f"✅ result_df count: {result_df.count()}")
        print("🔍 Checking PKID field:", POSTGRESS_TABLE_PKID)
        result_df.select(POSTGRESS_TABLE_PKID).distinct().show()
        result_df.printSchema()

        if result_df.rdd.isEmpty():
            print(f"⚠️ No data after SQL extraction in batch {batch_id}")
            return

        engine = create_engine(POSTGRES_CON_URL)

        # Ensure tables exist
        create_table_from_df_if_not_exists(result_df, POSTGRES_TABLE, POSTGRESS_TABLE_PKID, engine)
        create_table_from_df_if_not_exists(result_df, POSTGRES_HISTORY_TABLE, POSTGRESS_TABLE_PKID, engine, include_version=True)

        # Get existing primary keys
        existing_ids = get_existing_ids(spark, POSTGRES_TABLE, POSTGRESS_TABLE_PKID)
        existing_ids_broadcast = spark.sparkContext.broadcast(existing_ids)

        # Separate new and update records
        new_records = result_df.filter(~col(POSTGRESS_TABLE_PKID).isin(existing_ids_broadcast.value))
        update_records = result_df.filter(col(POSTGRESS_TABLE_PKID).isin(existing_ids_broadcast.value))

        print("🆕 New records count:", new_records.count())
        print("♻️ Update records count:", update_records.count())

        # Write new records
        if not new_records.rdd.isEmpty():
            new_records.write.format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", POSTGRES_TABLE) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"✅ Inserted {new_records.count()} new records")

        if not update_records.rdd.isEmpty():
            print(f"📥 Update records count: {update_records.count()}")

            # Load current records from main PostgreSQL table
            current_pg_df = spark.read.format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", POSTGRES_TABLE) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .load()

            # Join on primary key
            join_cond = current_pg_df[POSTGRESS_TABLE_PKID] == update_records[POSTGRESS_TABLE_PKID]
            joined_df = current_pg_df.alias("curr").join(update_records.alias("new"), join_cond, "inner")

            # Columns to compare (excluding PK)
            data_columns = [c for c in current_pg_df.columns if c != POSTGRESS_TABLE_PKID]

            # Detect changed records
            change_conditions = [col(f"curr.{c}") != col(f"new.{c}") for c in data_columns]
            combined_change_condition = change_conditions[0]
            for cond in change_conditions[1:]:
                combined_change_condition = combined_change_condition | cond

            changed_records = joined_df.filter(combined_change_condition).select("curr.*")

            if not changed_records.rdd.isEmpty():
                # Load history to find current version
                history_df = spark.read.format("jdbc") \
                    .option("url", POSTGRES_URL) \
                    .option("dbtable", POSTGRES_HISTORY_TABLE) \
                    .option("user", POSTGRES_USER) \
                    .option("password", POSTGRES_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()

                max_versions_df = history_df.groupBy(POSTGRESS_TABLE_PKID) \
                    .agg(spark_max("version").alias("max_version"))

                versioned_old = changed_records.join(max_versions_df, on=POSTGRESS_TABLE_PKID, how="left") \
                    .withColumn("version", coalesce(col("max_version").cast("int"), lit(0)) + 1) \
                    .drop("max_version")

                # Write versioned records to history table
                versioned_old = versioned_old.dropDuplicates()
                versioned_old.write.format("jdbc") \
                    .option("url", POSTGRES_URL) \
                    .option("dbtable", POSTGRES_HISTORY_TABLE) \
                    .option("user", POSTGRES_USER) \
                    .option("password", POSTGRES_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                print(f"📦 Archived {versioned_old.count()} changed records")

                # Remove only changed records from main table
                changed_ids = changed_records.select(POSTGRESS_TABLE_PKID).distinct().rdd.map(lambda row: row[0]).collect()
                ids_str = ", ".join([f"'{x}'" for x in changed_ids])
                delete_query = f'DELETE FROM {POSTGRES_TABLE} WHERE "{POSTGRESS_TABLE_PKID}" IN ({ids_str})'

                conn = psycopg2.connect(**POSTGRESS_CON)
                cursor = conn.cursor()
                cursor.execute(delete_query)
                conn.commit()
                cursor.close()
                conn.close()

                # Write updated records (only changed)
                update_records_filtered = update_records.filter(col(POSTGRESS_TABLE_PKID).isin(changed_ids))

                update_records_filtered.write.format("jdbc") \
                    .option("url", POSTGRES_URL) \
                    .option("dbtable", POSTGRES_TABLE) \
                    .option("user", POSTGRES_USER) \
                    .option("password", POSTGRES_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                print(f"🔄 Updated {update_records_filtered.count()} records in {POSTGRES_TABLE}")
            else:
                print("✅ No actual data changes detected — skipping versioning.")

    except AnalysisException as ae:
        print(f"❌ SQL error in batch {batch_id}: {ae}")
    except Exception as e:
        print(f"❌ Error writing batch {batch_id}: {e}")

def start_spark_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1)
        .load()
    )

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
