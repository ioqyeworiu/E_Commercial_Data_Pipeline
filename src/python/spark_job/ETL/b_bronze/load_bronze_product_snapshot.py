from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from preprocessing_func.product_snapshot_norm import *

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse-server:8123/ecommercial_warehouse"
CLICKHOUSE_RAW_TABLE = "raw_product_snapshot"
CLICKHOUSE_BRONZE_TABLE = "bronze_product_snapshot"
CLICKHOUSE_USER = "clickhouse"
CLICKHOUSE_PASSWORD = "123456"

spark = SparkSession.builder.appName("Load Bronze Product Snapshot").getOrCreate()

raw_product_snapshot_df = (
    spark.read.format("jdbc")
    .option("url", CLICKHOUSE_URL)
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", CLICKHOUSE_RAW_TABLE)
    .option("user", CLICKHOUSE_USER)
    .option("password", CLICKHOUSE_PASSWORD)
    .load()
)

bronze_product_snapshot_df = raw_product_snapshot_df.select(
    F.lower(raw_product_snapshot_df.idempotency_key).alias("idempotency_key").cast("string"),
    F.lower(raw_product_snapshot_df.url_id).alias("url_id").cast("int"),
    normalize_title(raw_product_snapshot_df.title, lower=True).alias("title").cast("string"),
    F.lower(raw_product_snapshot_df.brand).alias("brand").cast("string"),
    normalize_price(raw_product_snapshot_df.price).alias("price").cast("float"),
    normalize_discount(raw_product_snapshot_df.discount).alias("discount").cast("float"),
    normalize_price(raw_product_snapshot_df.sell_price).alias("sell_price").cast("float"),
    normalize_sold(raw_product_snapshot_df.sold).alias("sold").cast("int"),
    normalize_rating(raw_product_snapshot_df.product_rate).alias("product_rate").cast("float"),
    normalize_rating_count(raw_product_snapshot_df.product_rate_cnt).alias("product_rate_cnt").cast("int"),
    F.lower(raw_product_snapshot_df.category_tree).alias("category_tree").cast("string"),
    F.lower(raw_product_snapshot_df.brand_origin).alias("brand_origin").cast("string"),
    F.lower(raw_product_snapshot_df.made_in).alias("made_in").cast("string"),
    normalize_warranty(raw_product_snapshot_df.warranty).alias("warranty").cast("string"),
    F.lower(raw_product_snapshot_df.description).alias("description").cast("string"),
    F.lower(raw_product_snapshot_df.seller).alias("seller").cast("string"),
    normalize_rating(raw_product_snapshot_df.seller_rate).alias("seller_rate").cast("float"),
    normalize_rating_count(raw_product_snapshot_df.seller_rate_cnt).alias("seller_rate_cnt").cast("int"),
    F.lower(raw_product_snapshot_df.additional_info).alias("additional_info").cast("string"),
    raw_product_snapshot_df.observed_at.alias("observed_at").cast("timestamp"),
).withColumn("ingested_at", F.current_timestamp()) \
.dropDuplicates(["idempotency_key"])

bronze_product_snapshot_df.write.format("jdbc") \
    .option("url", CLICKHOUSE_URL) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("dbtable", CLICKHOUSE_BRONZE_TABLE) \
    .option("user", CLICKHOUSE_USER) \
    .option("password", CLICKHOUSE_PASSWORD) \
    .mode("append") \
    .save()