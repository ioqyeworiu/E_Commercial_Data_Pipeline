from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

from spark_streaming_listener import CSVStreamingQueryListener

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse-server:8123/ecommercial_warehouse"
CLICKHOUSE_TABLE = "raw_product_snapshot"
CLICKHOUSE_USER = "clickhouse"
CLICKHOUSE_PASSWORD = "123456"

idempotency_cols = ["url_id", "observed_at"]

spark = SparkSession.builder.appName("Load Raw Product Snapshot").getOrCreate()
spark.streams.addListener(CSVStreamingQueryListener("/opt/spark/metrics/load_raw_product_snapshot_metrics.csv"))

product_snapshot_schema = StructType(
    [
        StructField("urlId", StringType(), True),
        StructField("title", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("discount", StringType(), True),
        StructField("sellPrice", StringType(), True),
        StructField("sold", StringType(), True),
        StructField("productRate", StringType(), True),
        StructField("productRateCount", StringType(), True),
        StructField("categoryTree", StringType(), True),
        StructField("brandOrigin", StringType(), True),
        StructField("madeIn", StringType(), True),
        StructField("warranty", StringType(), True),
        StructField("description", StringType(), True),
        StructField("seller", StringType(), True),
        StructField("sellerRate", StringType(), True),
        StructField("sellerRateCount", StringType(), True),
        StructField("additionalInfo", StringType(), True),
        StructField("observed_at", StringType(), True),
    ]
)

product_snapshot_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka-1:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "product_data_changes")
    .load()
)

records_df = product_snapshot_df.selectExpr("CAST(value AS STRING) AS message")

records = records_df.select(F.from_json("message", product_snapshot_schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_time", F.current_timestamp())

saved_records = records.select(
    F.trim(records.urlId).alias("url_id"),
    F.trim(records.title).alias("title"),
    F.trim(records.brand).alias("brand"),
    F.trim(records.price).alias("price"),
    F.trim(records.discount).alias("discount"),
    F.trim(records.sellPrice).alias("sell_price"),
    F.trim(records.sold).alias("sold"),
    F.trim(records.productRate).alias("product_rate"),
    F.trim(records.productRateCount).alias("product_rate_cnt"),
    F.trim(records.categoryTree).alias("category_tree"),
    F.trim(records.brandOrigin).alias("brand_origin"),
    F.trim(records.madeIn).alias("made_in"),
    F.trim(records.warranty).alias("warranty"),
    F.trim(records.description).alias("description"),
    F.trim(records.seller).alias("seller"),
    F.trim(records.sellerRate).alias("seller_rate"),
    F.trim(records.sellerRateCount).alias("seller_rate_cnt"),
    F.trim(records.additionalInfo).alias("additional_info"),
    F.trim(records.observed_at).alias("observed_at"),
    records.ingestion_time.alias("ingested_at"),
) .withColumn("idempotency_key", F.sha2(F.concat_ws("\u0001", *[F.coalesce(F.col(c), F.lit("<NULL>")) for c in idempotency_cols]), 256)) \
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
    
query = saved_records.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "file:///opt/spark/checkpoints/load_raw_product_snapshot_ck") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()
    
query.awaitTermination()