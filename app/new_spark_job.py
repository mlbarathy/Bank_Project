from app.utils.functions import *


load_dotenv()

def start_spark_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    db_conf = get_db_config()
    print("🚀 Spark streaming job started.")
    print(f"📡 Kafka Topic: {KAFKA_TOPIC}")
    print(f"🗄️  Target {TARGET_DB} Table: {db_conf['main_table']}")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("multiLine", "true")
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1)
        .load()
    )

    query = (
        kafka_df.writeStream
        .queryName("KafkaToDBStream")
        .foreachBatch(writestream_non_empty_batches)
        .option("checkpointLocation", CHECKPOINT_LOC)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()

def main():
    start_spark_stream()

if __name__ == '__main__':
    main()
