CREATE TABLE raw_product_snapshot(
    url_id String,
    title String,
    brand String,
    price String,
    discount String,
    sell_price String,
    sold String,
    product_rate String,
    product_rate_cnt String,
    category_tree String,
    brand_origin String,
    made_in String,
    warranty String,
    description String,
    seller String,
    seller_rate String,
    seller_rate_cnt String,
    additional_info String,
    observed_at String,
    ingested_at DATETIME('Asia/Ho_Chi_Minh'),
    idempotency_key String
)
ENGINE MergeTree
PARTITION BY toDate(ingested_at)
ORDER BY (url_id, observed_at);



CREATE TABLE raw_user_review(
    url_id String,
    user_name String,
    joined_duration String,
    reviewed String,
    thank_received String,
    star_score String,
    comment String,
    status String,
    images_urls String,
    rated_at String,
    use_time String,
    prod_properties String,
    observed_at String,
    ingested_at DATETIME('Asia/Ho_Chi_Minh'),
    idempotency_key String
)
ENGINE MergeTree
PARTITION BY toDate(ingested_at)
ORDER BY (url_id, observed_at);