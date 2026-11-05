CREATE TABLE silver_product(
    product_sk UInt64,
    category_sk UInt64,
    title String,
    made_in String,
    warranty String,
    description String,
    valid_from DATETIME DEFAULT now(),
    valid_to DATETIME DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    ingested_at DATETIME
)
ENGINE MergeTree
Order By (title);



CREATE TABLE silver_category(
    category_sk UInt64,
    title String,
    up_category_sk UInt64,
    level Int32,
    valid_from DATETIME DEFAULT now(),
    valid_to DATETIME DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    ingested_at DATETIME
)
ENGINE MergeTree
ORDER BY (title);



CREATE TABLE silver_brand(
    brand_sk UInt64,
    title String,
    brand_origin String,
    valid_from DATETIME DEFAULT now(),
    valid_to DATETIME DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    ingested_at DATETIME
)
ENGINE MergeTree
ORDER BY (title);



CREATE TABLE silver_seller(
    seller_sk UInt64,
    name String,
    seller_rate Float32,
    seller_rate_cnt Int64,
    valid_from DATETIME DEFAULT now(),
    valid_to DATETIME DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    ingested_at DATETIME
)
ENGINE MergeTree
ORDER BY (name);



CREATE TABLE silver_snapshot(
    product_sk UInt64,
    brand_sk UInt64,
    seller_sk UInt64,
    price DOUBLE,
    discount Float32,
    sell_price DOUBLE,
    sold Int32,
    product_rate Float32,
    product_rate_cnt Int32,
    observed_at DATETIME,
    ingested_at DATETIME
)
ENGINE MergeTree
PARTITION BY observed_at
ORDER BY (observed_at);
