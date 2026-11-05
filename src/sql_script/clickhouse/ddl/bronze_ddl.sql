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
    observed_at DATETIME,
    ingested_at DATETIME
)
ENGINE MergeTree
PARTITION BY ingested_at
ORDER BY (url_id, observed_at);

SHOW CREATE TABLE bronze_product_snapshot;

-- DROP TABLE bronze
--
-- SYSTEM RELOAD CONFIG;
--
-- RESTORE DATABASE ecommercial_warehouse
-- FROM Disk('backups', 'backup_2026.zip');


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
    observed_at String,
    ingested_at DATETIME
)
ENGINE MergeTree
PARTITION BY ingested_at
ORDER BY (url_id, observed_at);