from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, ArrayType

from spark_streaming_listener import CSVStreamingQueryListener

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse-server:8123/ecommercial_warehouse"
CLICKHOUSE_TABLE = "raw_user_review"
CLICKHOUSE_USER = "clickhouse"
CLICKHOUSE_PASSWORD = "123456"

idempotency_cols = ["url_id", "observed_at"]

user_review_schema = StructType([
    StructField("user_name", StringType(), True),
    StructField("joined_duration", StringType(), True),
    StructField("Đã viết", StringType(), True),
    StructField("Đã nhận", StringType(), True),
    StructField("star_score", StringType(), True),
    StructField("status", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("images_urls", StringType(), True),
    StructField("rate_time", StringType(), True),
    StructField("use_time", StringType(), True),
    StructField("prod_properties", StringType(), True),
    StructField("observed_at", StringType(), True)
])

spark = SparkSession.builder.appName("Load Raw User Review Snapshot").getOrCreate()
spark.streams.addListener(CSVStreamingQueryListener("/opt/spark/metrics/load_raw_user_review_snapshot_metrics.csv"))

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka-1:9092")
    .option("subscribe", "user_review")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
)

parsed_df = df.select("key", F.from_json("value", user_review_schema).alias("value")).select("key", "value.*")\
    .withColumn("ingestion_time", F.current_timestamp())

saved_df = parsed_df.select(
    F.trim(parsed_df.key).alias("url_id"),
    F.trim(parsed_df.user_name).alias("user_name"),
    F.trim(parsed_df.joined_duration).alias("joined_duration"),
    F.trim(F.col("Đã viết")).alias("reviewed"),
    F.trim(F.col("Đã nhận")).alias("thank_received"),
    F.trim(parsed_df.star_score).alias("star_score"),
    F.trim(parsed_df.status).alias("status"),
    F.trim(parsed_df.comment).alias("comment"),
    F.trim(parsed_df.images_urls).alias("images_urls"),
    F.trim(parsed_df.rate_time).alias("rated_at"),
    F.trim(parsed_df.use_time).alias("use_time"),
    F.trim(parsed_df.prod_properties).alias("prod_properties"),
    F.trim(parsed_df.observed_at).alias("observed_at"),
    parsed_df.ingestion_time.alias("ingested_at"),
).withColumn("idempotency_key", F.sha2(F.concat_ws("\u0001", *[F.coalesce(F.col(c), F.lit("<NULL>")) for c in idempotency_cols]), 256)) \
.dropDuplicates(["idempotency_key"])

def write_to_clickhouse(batch_df, batch_id):
    (
        batch_df.write.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", CLICKHOUSE_TABLE)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("batchsize", 50000)
        .mode("append")
        .save()
    )
    
query = saved_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "file:///opt/spark/checkpoints/load_raw_user_review_snapshot_ck") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()
    
query.awaitTermination()