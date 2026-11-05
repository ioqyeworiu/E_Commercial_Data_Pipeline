from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from preprocessing_func.review_snapshot_norm import *

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse-server:8123/ecommercial_warehouse"
CLICKHOUSE_RAW_TABLE = "raw_uesr_review"
CLICKHOUSE_BRONZE_TABLE = "bronze_user_review"
CLICKHOUSE_USER = "clickhouse"
CLICKHOUSE_PASSWORD = "123456"

spark = SparkSession.builder.appName("Load Bronze User Review").getOrCreate()

raw_user_review_df = (
    spark.read.format("jdbc")
    .option("url", CLICKHOUSE_URL)
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", CLICKHOUSE_RAW_TABLE)
    .option("user", CLICKHOUSE_USER)
    .option("password", CLICKHOUSE_PASSWORD)
    .load()
)

saved_df = raw_user_review_df.select(
    F.lower(raw_user_review_df.key).alias("url_id"),
    F.lower(raw_user_review_df.user_name).alias("user_name"),
    to_hours(raw_user_review_df.joined_duration).alias("joined_duration"),
    parse_count(raw_user_review_df.wrote).alias("reviewed"),
    parse_count(raw_user_review_df.received).alias("thank_received"),
    parse_star_score(raw_user_review_df.star_score).alias("star_score"),
    F.lower(raw_user_review_df.status).alias("status"),
    clean_comment(raw_user_review_df.comment).alias("comment"),
    F.lower(raw_user_review_df.images_urls).alias("images_urls"),
    to_hours(raw_user_review_df.rate_time).alias("rate_at"),
    to_hours(raw_user_review_df.use_time).alias("use_time"),
    clean_prod_properties(raw_user_review_df.prod_properties).alias("prod_properties"),
    raw_user_review_df.observed_at.alias("observed_at")
)

saved_df.select(
    raw_user_review_df.url_id.cast("int"),
    raw_user_review_df.user_name.cast("string"),
    raw_user_review_df.joined_duration.cast("float"),
    raw_user_review_df.reviewed.cast("int"),
    raw_user_review_df.thank_received.cast("int"),
    raw_user_review_df.star_score.cast("float"),
    raw_user_review_df.status.cast("string"),
    raw_user_review_df.comment.cast("string"),
    raw_user_review_df.images_urls.cast("string"),
    raw_user_review_df.rate_at.cast("float"),
    raw_user_review_df.use_time.cast("float"),
    raw_user_review_df.prod_properties.cast("string"),
    raw_user_review_df.observed_at.cast("timestamp")
).withColumn("ingested_at", F.current_timestamp())

saved_df.write.format("jdbc") \
.option("url", CLICKHOUSE_URL) \
.option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
.option("dbtable", CLICKHOUSE_BRONZE_TABLE) \
.option("user", CLICKHOUSE_USER)\
.option("password", CLICKHOUSE_PASSWORD)\
.mode("append")\
.save()