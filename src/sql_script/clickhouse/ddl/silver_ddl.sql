CREATE TABLE silver_product(
    product_sk String,
    url_id Int64,                    -- khóa bền (durable key): mọi version của 1 sản phẩm có chung url_id
    category_sk String,
    title String,
    made_in String,
    warranty String,
    description String,
    valid_from DATETIME('Asia/Ho_Chi_Minh') DEFAULT now(),
    valid_to DATETIME('Asia/Ho_Chi_Minh') DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    ingested_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
Order By (title);



CREATE TABLE silver_category(
    category_sk String,
    category_level_1 String,
    category_level_2 String,
    category_level_3 String,
    category_level_4 String,
    category_level_5 String,
    category_level_6 String,
    valid_from DATETIME('Asia/Ho_Chi_Minh') DEFAULT now(),
    valid_to DATETIME('Asia/Ho_Chi_Minh') DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    ingested_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
ORDER BY (category_sk);



CREATE TABLE silver_brand(
    brand_sk String,                 -- lower(hex(SHA256(brand_normalized)))
    name String,                     -- tên gốc dùng để hiển thị
    brand_normalized String,         -- khóa chuẩn hóa, dùng để gộp và sinh brand_sk
    brand_origin String,
    name_alias String,
    valid_from DATETIME('Asia/Ho_Chi_Minh') DEFAULT now(),
    valid_to DATETIME('Asia/Ho_Chi_Minh') DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    ingested_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
ORDER BY (name);



CREATE TABLE silver_seller(
    seller_sk String,
    name String,
    seller_rate Float32,
    seller_rate_cnt Int64,
    valid_from DATETIME('Asia/Ho_Chi_Minh') DEFAULT now(),
    valid_to DATETIME('Asia/Ho_Chi_Minh') DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    ingested_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
ORDER BY (name);



CREATE TABLE silver_snapshot(
    product_sk String,
    url_id Int64,                    -- khóa bền, gold dùng để tính chênh lệch theo từng sản phẩm
    product_sk_2 String,
    brand_sk String,
    seller_sk String,
    category_sk String,
    price DOUBLE,
    discount Float32,
    sell_price DOUBLE,
    sold Int32,
    product_rate Float32,
    product_rate_cnt Int32,
    observed_at DATETIME('Asia/Ho_Chi_Minh'),
    ingested_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
PARTITION BY toDate(ingested_at)
ORDER BY (observed_at);

-- Migration cho bảng đã tạo trước đó (dữ liệu cũ sẽ có url_id = 0 -> TRUNCATE rồi nạp lại):
-- ALTER TABLE silver_product  ADD COLUMN IF NOT EXISTS url_id Int64 AFTER product_sk;
-- ALTER TABLE silver_snapshot ADD COLUMN IF NOT EXISTS url_id Int64 AFTER product_sk;


CREATE TABLE silver_user(
    user_name String,
    joined_duration String,
    reviewed Int32,
    thank_received Int32,
    valid_from DATETIME('Asia/Ho_Chi_Minh') DEFAULT now(),
    valid_to DATETIME('Asia/Ho_Chi_Minh') DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    ingested_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
PARTITION BY toDate(ingested_at)
ORDER BY (user_name);

CREATE TABLE silver_review(
    user_name String,
    url_id BigInt,
    review_content String,
    star_score Int32,
    ingested_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
PARTITION BY toDate(ingested_at)
ORDER BY (user_name);