from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

from spark_kafka_to_clickhouse_eval import CsvMetricsListener

# ====== CẤU HÌNH (sửa cho khớp môi trường của bạn) ======
KAFKA_BOOTSTRAP = "kafka-1:9092"
KAFKA_TOPIC = "product_data_changes"
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse-server:8123/ecommercial_warehouse"
CLICKHOUSE_TABLE = "bronze"
CLICKHOUSE_USER = "clickhouse"
CLICKHOUSE_PASSWORD = 123456
CHECKPOINT_DIR = "/tmp/checkpoint/product_changes"

spark = SparkSession.builder.appName("kafka_to_clickhouse").getOrCreate()
# Đăng ký Listener
csv_path = "/opt/spark/metrics/streaming_metrics.csv"
spark.streams.addListener(CsvMetricsListener(csv_path))

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

# ====== 2.1. LỌC NGÀNH THỜI TRANG MỞ RỘNG ======
# So khớp từng cấp categoryTree; không tìm trong title/description.
# Không dùng từ đơn "túi", "vòng", "kính", "phụ kiện" vì dễ lấy nhầm
# túi rác, vòng bi, kính xây dựng, phụ kiện điện thoại...
FASHION_CATEGORY_PATTERN = (
    r"^(?:thời trang|quần áo|trang phục|đồ lót|đồ ngủ|đồ bơi|"
    r"giày dép|giày|dép|sandal|sneaker|bốt|boots|"
    r"túi xách|túi đeo|túi thời trang|ví|bóp|ba lô|balo|"
    r"đồng hồ đeo tay|đồng hồ nam|đồng hồ nữ|đồng hồ trẻ em|"
    r"đồng hồ thông minh|trang sức|nữ trang|"
    r"mắt kính|kính mắt|kính mát|kính râm|gọng kính|kính thời trang|"
    r"vòng tay|vòng cổ|lắc tay|lắc chân|dây chuyền|nhẫn|"
    r"bông tai|hoa tai|khuyên tai|mặt dây chuyền|"
    r"phụ kiện thời trang|phụ kiện tóc|phụ kiện trang sức|"
    r"thắt lưng|dây nịt|khăn choàng|khăn quàng|cà vạt|nơ đeo cổ|"
    r"mũ|nón|tất|vớ|găng tay)(?:\s|$)"
    # Nhánh quần áo có tên ngắn, có thể kèm nhóm nam/nữ/trẻ em.
    r"|^(?:áo|quần|váy|đầm|chân váy|áo thun|áo sơ mi|áo khoác|"
    r"áo len|áo hoodie|quần jean|quần jeans|quần tây|quần short)"
    r"(?:\s+(?:nam|nữ|trẻ em|bé trai|bé gái|em bé|unisex))?$"
    r"|^đồng hồ$"
)
category_levels = F.split(
    F.lower(F.trim(F.coalesce(F.col("categoryTree"), F.lit("")))),
    r"\s*>\s*",
)
fashion_only = parsed.filter(
    F.exists(
        category_levels,
        lambda level: F.regexp_replace(F.trim(level), r"\s+", " ").rlike(
            FASHION_CATEGORY_PATTERN
        ),
    )
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

categories = F.split(F.coalesce(F.col("categoryTree"), F.lit("")), r"\s*>\s*")

clean = (
    fashion_only.select(
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
