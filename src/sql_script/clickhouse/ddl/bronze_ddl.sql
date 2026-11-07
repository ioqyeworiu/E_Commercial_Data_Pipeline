CREATE TABLE bronze_product_snapshot(
    url_id BIGINT,
    title String,
    brand String,
    price float,
    discount float,
    sell_price float,
    sold int,
    product_rate float,
    product_rate_cnt int,
    category_tree String,
    brand_origin String,
    made_in String,
    warranty String,
    description String,
    seller String,
    seller_rate float,
    seller_rate_cnt int,
    additional_info String,
    observed_at DATETIME('Asia/Ho_Chi_Minh'),
    ingested_at DATETIME('Asia/Ho_Chi_Minh'),
    idempotency String
)
ENGINE MergeTree
PARTITION BY toDate(ingested_at)
ORDER BY (url_id, observed_at);


CREATE TABLE bronze_user_review(
    url_id BIGINT,
    user_name String,
    joined_duration int, --đơn vị là giờ
    reviewed int,
    thank_received int,
    star_score float,
    comment String,
    status String,
    images_urls String,
    rated_at float, --đơn vị là giờ
    use_time float, --đơn vị là giờ
    prod_properties String,
    observed_at DATETIME('Asia/Ho_Chi_Minh'),
    ingested_at DATETIME('Asia/Ho_Chi_Minh'),
    idempotency_key String
)
ENGINE MergeTree
PARTITION BY toDate(ingested_at)
ORDER BY (url_id, observed_at);