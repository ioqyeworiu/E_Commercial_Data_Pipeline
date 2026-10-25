from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

# ====== CẤU HÌNH (sửa cho khớp môi trường của bạn) ======
KAFKA_BOOTSTRAP = "kafka-1:9092"
KAFKA_TOPIC = "product_data_changes"
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse-server:8123/ecommercial_warehouse"
CLICKHOUSE_TABLE = "bronze"
CLICKHOUSE_USER = "clickhouse"
CLICKHOUSE_PASSWORD = 123456
CHECKPOINT_DIR = "/tmp/checkpoint/product_changes"

spark = SparkSession.builder.appName("kafka_to_clickhouse").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ====== 1. ĐỌC TỪ KAFKA ======
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# raw = (
#     spark.read.format("kafka")
#     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
#     .option("subscribe", KAFKA_TOPIC)
#     .option("startingOffsets", "earliest")
#     .option("failOnDataLoss", "false")
#     .load()
# ).selectExpr(
#     "CAST(key AS STRING) as key",
#     "CAST(value AS STRING) as value",
#     "timestamp"
# ).show(5, truncate=False) 

# ====== 2. PARSE JSON ======
# Mỗi message là 1 MẢNG [{...}]. Cho mọi cột là String rồi tự ép kiểu.
# Không khai báo additionalInfo => tự động bị bỏ.
item_schema = StructType([
    StructField("urlId", StringType()),
    StructField("title", StringType()),
    StructField("brand", StringType()),
    StructField("price", StringType()),
    StructField("discount", StringType()),
    StructField("sellPrice", StringType()),
    StructField("sold", StringType()),
    StructField("productRate", StringType()),
    StructField("productRateCount", StringType()),
    StructField("categoryTree", StringType()),
    StructField("brandOrigin", StringType()),
    StructField("madeIn", StringType()),
    StructField("warranty", StringType()),
    StructField("description", StringType()),
    StructField("seller", StringType()),
    StructField("sellerRate", StringType()),
    StructField("sellerRateCount", StringType()),
])

parsed = (
    raw.select([F.from_json(F.col("value").cast("string"), ArrayType(item_schema)).alias("arr"), "timestamp"])
    .select(F.explode("arr").alias("d"), "timestamp")   # mảng -> mỗi phần tử 1 dòng
    .select("d.*", "timestamp")
)

# ====== 3. TIỀN XỬ LÝ (giống SQL) ======
def tc(col, dtype):
    """try_cast: sai kiểu thì ra NULL (không báo lỗi)."""
    return F.expr(f"try_cast({col} as {dtype})")

def txt(col):
    """Chuỗi: bỏ khoảng trắng 2 đầu, NULL -> chuỗi rỗng."""
    return F.coalesce(F.trim(F.col(col)), F.lit(""))

# "153 đánh giá" -> 153 ; "1.4k+ đánh giá" -> 1400
rate_num = F.regexp_extract("sellerRateCount", r"([0-9]+\.?[0-9]*)", 1).cast("double")
rate_mul = F.when(F.col("sellerRateCount").rlike(r"(?i)[0-9]\s*k"), 1000).otherwise(1)

categories = F.split(F.coalesce(F.col("categoryTree"), F.lit("")), r"\s>\s")

clean = (
    parsed.select(
        tc("urlId", "bigint").alias("url_id"),
        txt("title").alias("title"),
        txt("brand").alias("brand"),
        tc("price", "double").alias("price"),                 # được phép NULL
        tc("discount", "float").alias("discount"),
        tc("sellPrice", "double").alias("sell_price"),
        tc("sold", "int").alias("sold"),
        tc("productRate", "float").alias("product_rate"),    # được phép NULL
        tc("productRateCount", "int").alias("product_rate_count"),
        txt("categoryTree").alias("category_tree"),
        categories[0].alias("category_lv1"),
        F.element_at(categories, -1).alias("category_leaf"),
        txt("brandOrigin").alias("brand_origin"),
        txt("madeIn").alias("made_in"),
        txt("warranty").alias("warranty"),
        txt("description").alias("description"),
        txt("seller").alias("seller"),
        tc("sellerRate", "float").alias("seller_rate"),
        (rate_num * rate_mul).cast("int").alias("seller_rate_count"),
        F.col("timestamp").alias("timestamp"),
    )
    # Cột BẮT BUỘC: sai kiểu / thiếu => bỏ dòng
    .dropna(subset=["url_id", "sell_price", "sold", "discount"])
    # Giá trị vô lý => bỏ dòng
    .filter((F.col("sell_price") > 0) & (F.col("sold") >= 0) & (F.col("title") != ""))
)

# ====== 4. GHI VÀO CLICKHOUSE ======
def write_to_clickhouse(batch_df, batch_id):
    (
        batch_df.write.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", CLICKHOUSE_TABLE)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .mode("append")
        .save()
    )

query = (
    clean.writeStream
    .foreachBatch(write_to_clickhouse)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="10 seconds")
    .start()
)
query.awaitTermination()
