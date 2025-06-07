# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 app_if.py

import os
import joblib
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

import shutil
import os
import threading
import time
from datetime import datetime

# Kafka config
SERVER = "broker:9092"
TOPIC = "pm10"
CHECKPOINT_PATH = "/home/jovyan/notebooks/spark/checkpoints"

ANOMALY_PARQUET_PATH = "/home/jovyan/notebooks/spark/anomalies/iforest"
MERGED_ANOMALY_PARQUET_HOURLY = ANOMALY_PARQUET_PATH + "_hourly"

SCHEMA = StructType([
    StructField("station_id", IntegerType()),
    StructField("reading_date", StringType()),
    StructField("datetime_from_sensor", TimestampType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType())
])

# Wczytaj wytrenowany model Isolation Forest
isoforest_model = joblib.load("/home/jovyan/notebooks/spark/isoforest_model.pkl")

# UDF do predykcji
def predict_anomaly(value):
    if value is None:
        return 0
    try:
        prediction = isoforest_model.predict([[value]])
        #df_tmp = pd.DataFrame({"value": [value]})
        #pred = isoforest_model.predict(df_tmp)
        return int(prediction[0] == -1)
    except:
        return 0

predict_anomaly_udf = udf(predict_anomaly, IntegerType())

# Funkcja wykrywania anomalii przez Isolation Forest
def detect_isoforest_anomalies(df, epoch_id):
    if df.rdd.isEmpty():
        print(f"⚠️ Batch {epoch_id} is empty – skipping.")
        return
    try:
        print(f"ℹ️ [Batch {epoch_id}] Isolation Forest anomaly detection...")
        df_with_anomalies = df.withColumn("anomaly", predict_anomaly_udf(col("value")))
        anomalies = df_with_anomalies.filter(col("anomaly") == 1)

        if anomalies.rdd.isEmpty():
            print(f"ℹ️ No anomalies in batch {epoch_id}")
            return

        anomalies.select("station_id", "datetime_from_sensor", "value").show(truncate=False)

        anomalies.write.mode("append").parquet(ANOMALY_PARQUET_PATH)

        kafka_ready = anomalies.selectExpr(
            "CAST(station_id AS STRING) AS key",
            "to_json(struct(*)) AS value"
        )
        kafka_ready.write.format("kafka") \
            .option("kafka.bootstrap.servers", SERVER) \
            .option("topic", "if_anomalies") \
            .save()

        print("📤 Anomalie wysłane do Kafka topic: 'if_anomalies'")

    except Exception as e:
        print(f"❌ Error during ISOForest detection: {e}")

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

def run_hourly_merger():
    while True:
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"🕐 [{now}] Rozpoczynam scalanie plików...")
            merge_parquet_files(
                input_path=ANOMALY_PARQUET_PATH,
                output_path=MERGED_ANOMALY_PARQUET_HOURLY
            )
        except Exception as e:
            print(f"❌ Błąd przy scalaniu: {e}")
        time.sleep(3600)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("IsolationForest-PM10").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

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
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json("json_str", SCHEMA).alias("data"))
        .select("data.*")
    )

    isoforest_query = (
        parsed.writeStream
        .outputMode("append")
        .foreachBatch(detect_isoforest_anomalies)
        .option("checkpointLocation", CHECKPOINT_PATH + "/iforest_detection")
        .trigger(processingTime="15 seconds")
        .start()
    )

    #Uruchomienie scalania co 1h w osobnym wątku
    threading.Thread(target=run_hourly_merger, daemon=True).start()
    
    spark.streams.awaitAnyTermination()
