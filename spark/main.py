import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, window, from_json, to_timestamp,
    concat, lit, rand, split, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, FloatType
)
from elasticsearch import Elasticsearch
from datetime import datetime

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "msa-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
KAFKA_TOPIC = "msa-logs"
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
DOS_THRESHOLD = int(os.getenv("DOS_THRESHOLD", "100"))  # 1분 윈도우 임계값
SALT_BUCKETS = 10  # 분산 버킷 수

es = Elasticsearch(ES_HOST)

spark = SparkSession.builder \
    .appName("MSA-AnomalyDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("service", StringType()),
    StructField("ip", StringType()),
    StructField("method", StringType()),
    StructField("uri", StringType()),
    StructField("status", IntegerType()),
    StructField("has_valid_token", BooleanType()),
    StructField("user_id", StringType()),
    StructField("content_id", IntegerType()),
    StructField("genre", StringType()),
    StructField("watch_ratio", FloatType()),
    StructField("response_time", IntegerType())
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

logs = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("log")
).select("log.*") \
 .withColumn("event_time", to_timestamp(col("timestamp"))) \
 .filter(col("ip").isNotNull())

# 인증 우회 탐지
auth_bypass = logs.filter(
    (col("has_valid_token") == False) &
    (col("uri").contains("/stream/"))
)

def process_auth_bypass(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    for row in batch_df.collect():
        doc = {
            "timestamp": row["timestamp"] or datetime.utcnow().isoformat(),
            "service": row["service"],
            "ip": row["ip"],
            "uri": row["uri"],
            "user_id": row["user_id"],
            "attack_type": "auth_bypass",
            "detected_at": datetime.utcnow().isoformat()
        }
        try:
            es.index(index="msa-detections", body=doc)
            print(f"[인증 우회 탐지] IP: {row['ip']} URI: {row['uri']}")
        except Exception as e:
            print(f"ES 저장 실패: {e}")

auth_bypass.writeStream \
    .foreachBatch(process_auth_bypass) \
    .outputMode("append") \
    .start()

# DoS 탐지
# Stage 1: Salt 붙여서 Hot Key 분산
dos_stage1 = logs \
    .withColumn("salt", (rand() * SALT_BUCKETS).cast("int").cast("string")) \
    .withColumn("ip_salt", concat(col("ip"), lit("_"), col("salt"))) \
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("ip_salt")
    ) \
    .agg(count("*").alias("partial_count"))

def process_dos(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    # Stage 2: Salt 제거 후 재집계
    stage2 = batch_df \
        .withColumn("ip", split(col("ip_salt"), "_")[0]) \
        .groupBy("window", "ip") \
        .agg(spark_sum("partial_count").alias("req_count"))

    attackers = stage2.filter(col("req_count") > DOS_THRESHOLD)
    for row in attackers.collect():
        doc = {
            "timestamp": datetime.utcnow().isoformat(),
            "ip": row["ip"],
            "req_count": row["req_count"],
            "threshold": DOS_THRESHOLD,
            "attack_type": "dos",
            "detected_at": datetime.utcnow().isoformat()
        }
        try:
            es.index(index="msa-detections", body=doc)
            print(f"[DoS 탐지] IP: {row['ip']} 요청수: {row['req_count']} 임계값: {DOS_THRESHOLD}")
        except Exception as e:
            print(f"ES 저장 실패: {e}")

dos_stage1.writeStream \
    .foreachBatch(process_dos) \
    .outputMode("update") \
    .start()

spark.streams.awaitAnyTermination()