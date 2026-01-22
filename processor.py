import os
import sys

# --- 🛠️ FIX: UNSET PRE-INSTALLED SPARK VARIABLES ---
# This prevents the conflict between the Docker Spark and  pip-installed Spark
os.environ.pop('SPARK_HOME', None)
os.environ.pop('SPARK_CONF_DIR', None)

# --- NOW IMPORT PYSPARK ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, LongType


# --- 1. CONFIGURE SPARK ---
# We need to download the drivers for Kafka and S3 (MinIO) automatically
spark = SparkSession.builder \
    .appName("F1-Strategy-Engine") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 2. DEFINE SCHEMA ---
# This matches the JSON you created in vision_producer.py
# { "frame_id": 12, "event_type": "PIT_ENTRY", "pit_timer": 4.5 ... }
schema = StructType([
    StructField("frame_id", LongType()),
    StructField("timestamp", DoubleType()),
    StructField("event_type", StringType()),
    StructField("pit_timer", DoubleType()),
    StructField("detections", ArrayType(StringType())) # We keep this raw for now
])

# --- 3. READ STREAM (FROM REDPANDA) ---
print("🏎️  Connecting to Redpanda Stream...")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:29092") \
    .option("subscribe", "f1-vision-events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert binary "value" to String, then parse JSON
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# --- 4. ANALYTICS LOGIC ---
# We only want to save data when the car is ACTUALLY in the pit (or just exiting)
# 'PIT_ENTRY' or 'PIT_EXIT'. We ignore generic 'TRACK_ACTION' to save space.
pitting_events = parsed_stream.filter(
    (col("event_type") == "PIT_ENTRY") | (col("event_type") == "PIT_EXIT")
)

# --- 5. WRITE STREAM (TO CONSOLE & MINIO) ---.
# later we will switch this to .format("parquet").save("s3a://f1-data/...")

query = pitting_events.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("✅ Strategy Engine Running... Waiting for Pit Stops.")
query.awaitTermination()