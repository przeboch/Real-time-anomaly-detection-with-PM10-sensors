# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 app.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import shutil
import os
import threading
import time
from datetime import datetime

# Kafka
SERVER = "broker:9092"
TOPIC = "pm10"

# Ścieżki
CHECKPOINT_PATH = "/home/jovyan/notebooks/spark"

MERGED_SENSOR_PARQUET = "/home/jovyan/notebooks/spark/sensor_data/merged_sensor_parquet"
MERGED_SENSOR_PARQUET_HOURLY = MERGED_SENSOR_PARQUET + "_hourly"

ANOMALY_PARQUET_PATH = "/home/jovyan/notebooks/spark/anomalies/parquet"

# Schemat JSON z Kafka
SCHEMA = StructType([
    StructField("station_id", IntegerType()),
    StructField("reading_date", StringType()),
    StructField("datetime_from_sensor", TimestampType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType())
])

# ✅ Funkcja do zapisu pojedynczego batcha
def my_merge_function(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        print(f"⚠️  Batch {epoch_id} is empty – skipping.")
        return

    batch_df.write.mode("append").parquet(MERGED_SENSOR_PARQUET)
    print(f"✅ Batch {epoch_id} appended to {MERGED_SENSOR_PARQUET}")

# ✅ Funkcja do scalania wielu part-* w jeden merged.parquet
def merge_parquet_files(input_path, output_path, tmp_path="/tmp/merged_parquet_tmp"):
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.parquet(os.path.join(input_path, "*.parquet"))
    df.coalesce(1).write.mode("overwrite").parquet(tmp_path)

    part_file = [f for f in os.listdir(tmp_path) if f.startswith("part-") and f.endswith(".parquet")][0]

    final_file_path = os.path.join(output_path, "merged.parquet")
    os.makedirs(output_path, exist_ok=True)
    shutil.move(os.path.join(tmp_path, part_file), final_file_path)
    shutil.rmtree(tmp_path)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"🟢 [{now}] Scalony plik zapisany jako: {final_file_path}")

# ✅ Harmonogram co 1h – osobny wątek
def run_hourly_merger():
    while True:
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"🕐 [{now}] Rozpoczynam scalanie plików...")
            merge_parquet_files(
                input_path=MERGED_SENSOR_PARQUET,
                output_path=MERGED_SENSOR_PARQUET_HOURLY
            )
        except Exception as e:
            print(f"❌ Błąd przy scalaniu: {e}")
        time.sleep(3600)

def detect_spikes_in_batch(df, epoch_id):
    if df.rdd.isEmpty():
        print(f"⚠️  Batch {epoch_id} is empty – no data to process.")
        return

    spark = df.sparkSession

    try:
        historical_df = spark.read.parquet(f"{MERGED_SENSOR_PARQUET}/*.parquet")

        combined_df = historical_df.unionByName(df)

        window_spec = Window.partitionBy("station_id").orderBy("datetime_from_sensor")

        result = (
            combined_df
            .withColumn("prev_value", lag("value").over(window_spec))
            .withColumn("delta", col("value") - col("prev_value"))
            .filter(col("prev_value").isNotNull())
            .filter(abs(col("delta")) > 30)
            .withColumn("air_quality_class",
                when(col("value") <= 50, "dobry")
                .when(col("value") <= 100, "umiarkowany")
                .when(col("value") <= 150, "dostateczny")
                .when(col("value") <= 200, "zły")
                .otherwise("bardzo zły")
            )
            .select("station_id", "datetime_from_sensor", "value", "prev_value", "delta", "air_quality_class")
            .orderBy(desc("delta"))
        )

        if result.rdd.isEmpty():
            print(f"ℹ️ Batch {epoch_id} – no PM10 spikes detected.")
            return

        print(f"\n🚨 Sudden PM10 spikes in batch {epoch_id}")
        result.show(truncate=False)

        result.write.mode("append").parquet(ANOMALY_PARQUET_PATH)

        kafka_ready = result.selectExpr(
            "CAST(station_id AS STRING) AS key",
            "to_json(struct(*)) AS value"
        )

        kafka_ready.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", SERVER) \
            .option("topic", "pm10_anomalies") \
            .save()

        print("📤 Anomalie PM10 wysłane do Kafka topic: 'pm10_anomalies'")

    except Exception as e:
        print(f"❌ Error in spike detection for batch {epoch_id}: {e}")

def send_classified_to_kafka(df, epoch_id):
    if df.rdd.isEmpty():
        print(f"⚠️ Batch {epoch_id} is empty – skipping classified send.")
        return
    try:
        kafka_ready = df.selectExpr(
            "CAST(station_id AS STRING) AS key",
            "to_json(struct(*)) AS value"
        )

        kafka_ready.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", SERVER) \
            .option("topic", "pm10_class") \
            .save()

        print(f"📤 [Batch {epoch_id}] PM10 classified data sent to Kafka topic: 'pm10_class'")
    except Exception as e:
        print(f"❌ Error sending classified batch {epoch_id} to Kafka: {e}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("PM10-Streaming").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", SERVER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json("json_str", SCHEMA).alias("data"))
        .select("data.*")
    )

    classified = parsed.withColumn("air_quality_class",
        when(col("value") <= 50, "dobry")
        .when(col("value") <= 100, "umiarkowany")
        .when(col("value") <= 150, "dostateczny")
        .when(col("value") <= 200, "zły")
        .otherwise("bardzo zły")
    )

    class_query = (
        classified.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 100)
        .trigger(processingTime='1 minute')
        .start()
    )

    parquet_query = (
        classified.writeStream
        .outputMode("append")
        .foreachBatch(my_merge_function)
        .option("checkpointLocation", CHECKPOINT_PATH + "/sensor_parquet_merge")
        .trigger(processingTime="1 minute")
        .start()
    )

    anomaly_query = (
        classified.writeStream
        .outputMode("append")
        .foreachBatch(detect_spikes_in_batch)
        .option("checkpointLocation", CHECKPOINT_PATH + "/anomaly_detection")
        .trigger(processingTime="1 minute")
        .start()
    )

    classified_to_kafka_query = (
        classified.writeStream
        .outputMode("append")
        .foreachBatch(send_classified_to_kafka)
        .option("checkpointLocation", CHECKPOINT_PATH + "/classified_to_kafka")
        .trigger(processingTime="1 minute")
        .start()
    )
    
    #Uruchomienie scalania co 1h w osobnym wątku
    threading.Thread(target=run_hourly_merger, daemon=True).start()

    
    spark.streams.awaitAnyTermination()
