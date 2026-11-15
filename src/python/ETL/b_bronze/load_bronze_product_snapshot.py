from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from preprocessing_func.product_snapshot_norm import *

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse-server:8123/ecommercial_warehouse"
CLICKHOUSE_RAW_TABLE = "raw_product_snapshot"
CLICKHOUSE_BRONZE_TABLE = "bronze_product_snapshot"
CLICKHOUSE_USER = "clickhouse"
CLICKHOUSE_PASSWORD = "123456"

# Loại trùng theo idempotency_key NGAY TRONG ClickHouse, thay cho .dropDuplicates() của Spark.
# Vì sao: idempotency_key gần như duy nhất (10,05 triệu dòng -> 10 triệu), nên dropDuplicates phải
# giữ gần như toàn bộ dữ liệu trong bảng hash. Executor 500m chỉ có ~110 MiB bộ nhớ thực thi
# -> spill ~16,6 GB xuống /tmp của worker mỗi lần chạy (ghi vào docker_data.vhdx, xoá rồi cũng không co).
# Cách làm: chỉ dò các key bị trùng bằng hash 64-bit (bảng hash ~300 MB, không cần dòng đầy đủ).
#   - Nhánh 1: dòng có hash xuất hiện đúng 1 lần -> key chắc chắn duy nhất -> đi thẳng, không gom.
#   - Nhánh 2: dòng có hash xuất hiện >1 lần (~50k key) -> GROUP BY theo idempotency_key thật, giữ 1 dòng.
# Hai key khác nhau mà trùng hash chỉ bị đẩy sang nhánh 2 và vẫn ra 2 dòng riêng -> kết quả luôn đúng.
# Ngữ nghĩa giống dropDuplicates(["idempotency_key"]): mỗi key giữ 1 dòng bất kỳ (các bản trùng là
# cùng một message Kafka gửi lại nên giống hệt nhau). Spark không còn shuffle -> không spill.
_RAW_COLS = [
    "url_id", "observed_at", "title", "brand", "price", "discount", "sell_price", "sold",
    "product_rate", "product_rate_cnt", "category_tree", "brand_origin", "made_in", "warranty",
    "description", "seller", "seller_rate", "seller_rate_cnt", "additional_info",
]
_DUP_HASHES = f"""
    SELECT h FROM (SELECT cityHash64(idempotency_key) AS h FROM {CLICKHOUSE_RAW_TABLE})
    GROUP BY h HAVING count() > 1"""
RAW_DEDUP_QUERY = f"""
SELECT idempotency_key, {", ".join(_RAW_COLS)}
FROM {CLICKHOUSE_RAW_TABLE}
WHERE cityHash64(idempotency_key) NOT IN ({_DUP_HASHES})
UNION ALL
SELECT idempotency_key, {", ".join(f"any({c})" for c in _RAW_COLS)}
FROM {CLICKHOUSE_RAW_TABLE}
WHERE cityHash64(idempotency_key) IN ({_DUP_HASHES})
GROUP BY idempotency_key
"""

spark = SparkSession.builder.appName("Load Bronze Product Snapshot").getOrCreate()

raw_product_snapshot_df = (
    spark.read.format("jdbc")
    .option("url", CLICKHOUSE_URL)
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("query", RAW_DEDUP_QUERY)   # đã loại trùng -> không cần dropDuplicates
    .option("user", CLICKHOUSE_USER)
    .option("password", CLICKHOUSE_PASSWORD)
    .load()
)

bronze_product_snapshot_df = raw_product_snapshot_df.select(
    F.lower(raw_product_snapshot_df.idempotency_key).alias("idempotency_key").cast("string"),
    F.lower(raw_product_snapshot_df.url_id).alias("url_id").cast("int"),
    normalize_title(raw_product_snapshot_df.title, lower=True).alias("title").cast("string"),
    F.trim(raw_product_snapshot_df.brand).alias("brand").cast("string"),  # giữ tên gốc
    normalize_brand(raw_product_snapshot_df.brand).alias("brand_normalized").cast("string"),
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
).withColumn("ingested_at", F.current_timestamp())

bronze_product_snapshot_df.write.format("jdbc") \
    .option("url", CLICKHOUSE_URL) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("dbtable", CLICKHOUSE_BRONZE_TABLE) \
    .option("user", CLICKHOUSE_USER) \
    .option("password", CLICKHOUSE_PASSWORD) \
    .mode("append") \
    .save()