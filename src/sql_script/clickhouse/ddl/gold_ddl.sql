-- =====================================================================================
-- GOLD LAYER - star schema
--
-- Loader nằm ở src/python/ETL/d_gold/, chạy theo thứ tự số trong tên file:
--   01..06  dimension   (đọc silver_* ; dim_reviewer đọc bronze_user_review vì silver_review chưa có)
--   07      gold_fact_product_snapshot  (đọc silver_snapshot + silver_brand + silver_seller)
--   08..11  các fact còn lại (đọc gold_fact_product_snapshot ; fact_review đọc bronze_user_review)
--
-- Idempotency: mọi bảng dùng ReplacingMergeTree(updated_at) và loader tính lại toàn bộ,
-- nên chạy lại loader N lần không nhân đôi dữ liệu. Dòng trùng ORDER BY key sẽ được gộp
-- khi merge -> khi truy vấn cần kết quả chính xác thì dùng FINAL (các view agg đã dùng sẵn).
--
-- Lưu ý: ReplacingMergeTree chỉ gộp các dòng trùng key nằm trong CÙNG partition, nên
-- partition key luôn được suy ra từ chính cột trong ORDER BY (observed_at, date_key, ...).
--
-- Khóa surrogate = lower(hex(SHA256(natural key))) giống silver.
-- =====================================================================================



-- ---------------------------------------------------------------- DIMENSIONS

CREATE TABLE gold_dim_date(
    date_key UInt32,                         -- YYYYMMDD
    full_date Date,
    day UInt8,
    day_of_week UInt8,                       -- 1 = Thứ Hai ... 7 = Chủ Nhật
    day_name LowCardinality(String),
    week_of_year UInt8,                      -- ISO week
    month UInt8,
    quarter UInt8,
    year UInt16,
    year_month UInt32,                       -- YYYYMM
    is_weekend Bool,
    is_double_day Bool,                      -- ngày đôi: 1.1, 2.2, ..., 11.11, 12.12
    is_black_friday Bool,
    is_tet Bool,                             -- mùng 1 -> mùng 3 Tết Nguyên Đán
    holiday_name LowCardinality(String),
    sale_event LowCardinality(String),       -- '11.11', 'Black Friday', ... ; '' nếu không có
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE ReplacingMergeTree(updated_at)
ORDER BY (date_key);



CREATE TABLE gold_dim_category(
    category_key String,                     -- = silver_category.category_sk = SHA256(category_tree)
    category_path String,                    -- 'thời trang nam > áo nỉ - áo len nam > áo len dệt kim'
    category_level_1 LowCardinality(String),
    category_level_2 LowCardinality(String),
    category_level_3 LowCardinality(String),
    category_level_4 String,
    category_level_5 String,
    category_level_6 String,
    category_depth UInt8,
    leaf_category String,                    -- cấp sâu nhất có giá trị
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE ReplacingMergeTree(updated_at)
ORDER BY (category_key);



CREATE TABLE gold_dim_brand(
    brand_key String,                        -- SHA256(brand_normalized) ; SHA256('unknown') khi không có brand
    brand_normalized String,                 -- khóa entity resolution (KHÔNG gồm brand_origin như silver)
    brand_name String,                       -- tên hiển thị xuất hiện nhiều nhất
    brand_origin LowCardinality(String),     -- xuất xứ xuất hiện nhiều nhất (bỏ giá trị rỗng)
    brand_origins Array(String),             -- mọi xuất xứ đã thấy (vd 'oem' có rất nhiều)
    name_aliases Array(String),              -- các cách viết tên gốc đã được gộp về brand này
    is_unknown Bool,
    first_seen_at DATETIME('Asia/Ho_Chi_Minh'),
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE ReplacingMergeTree(updated_at)
ORDER BY (brand_key);



CREATE TABLE gold_dim_seller(
    seller_key String,                       -- SHA256(tên seller) ; KHÔNG gồm seller_rate như silver
    seller_name String,
    is_official Bool,                        -- tên chứa 'official' / 'chính hãng' / là 'tiki trading'
    is_unknown Bool,
    first_seen_at DATETIME('Asia/Ho_Chi_Minh'),
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE ReplacingMergeTree(updated_at)
ORDER BY (seller_key);



-- SCD Type 2: mỗi dòng là 1 version của sản phẩm (đổi title / made_in / warranty -> version mới)
CREATE TABLE gold_dim_product(
    product_key String,                      -- = silver product_sk (khóa của version)
    product_id Int64,                        -- = url_id (khóa bền, chung cho mọi version)
    title String,
    made_in LowCardinality(String),
    warranty LowCardinality(String),
    description String,
    version_no UInt16,
    valid_from DATETIME('Asia/Ho_Chi_Minh'), -- lần đầu version này được crawler nhìn thấy
    valid_to Nullable(DATETIME('Asia/Ho_Chi_Minh')),  -- NULL = đang hiệu lực
    is_current Bool,
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE ReplacingMergeTree(updated_at)
ORDER BY (product_id, product_key);



CREATE TABLE gold_dim_reviewer(
    user_key String,                         -- SHA256(user_name) ; nguồn chỉ có tên nên 2 người trùng tên sẽ bị gộp
    user_name String,
    joined_at_est Nullable(DATETIME('Asia/Ho_Chi_Minh')),  -- observed_at - joined_duration (độ chính xác theo năm/tháng)
    tenure_band LowCardinality(String),
    reviewed_cnt UInt32,                     -- số đánh giá đã viết (lần quan sát gần nhất)
    thank_received UInt32,                   -- số lượt cảm ơn đã nhận (lần quan sát gần nhất)
    first_seen_at DATETIME('Asia/Ho_Chi_Minh'),
    last_seen_at DATETIME('Asia/Ho_Chi_Minh'),
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE ReplacingMergeTree(updated_at)
ORDER BY (user_key);



-- ---------------------------------------------------------------- FACTS

-- F1. Grain: 1 sản phẩm x 1 lần crawl
CREATE TABLE gold_fact_product_snapshot(
    product_id Int64,
    observed_at DATETIME('Asia/Ho_Chi_Minh'),
    date_key UInt32,
    hour_of_day UInt8,
    product_key String,
    brand_key String,
    seller_key String,
    category_key String,
    list_price UInt64,                       -- giá gốc ; nguồn để price = 0 khi không giảm -> dùng sell_price
    sell_price UInt64,                       -- giá bán thực tế (VND)
    discount_amount UInt64,                  -- list_price - sell_price
    discount_pct Float32,
    sold_cum UInt32,                         -- LŨY KẾ: không được sum theo thời gian
    product_rate Float32,
    product_rate_cnt UInt32,                 -- LŨY KẾ
    seller_rate Float32,
    seller_rate_cnt UInt32,                  -- LŨY KẾ
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (product_id, observed_at);



-- F2. Grain: 1 sản phẩm x 1 ngày (periodic snapshot) - fact chính cho phân tích
CREATE TABLE gold_fact_product_daily(
    date_key UInt32,
    product_id Int64,
    product_key String,                      -- trạng thái cuối ngày
    brand_key String,
    seller_key String,
    category_key String,
    open_sell_price UInt64,
    close_sell_price UInt64,
    min_sell_price UInt64,
    max_sell_price UInt64,
    avg_sell_price Float64,
    close_list_price UInt64,
    avg_discount_pct Float32,
    max_discount_pct Float32,
    sold_eod UInt32,                         -- lũy kế cuối ngày
    units_sold_est Nullable(UInt32),         -- sold_eod - sold_eod của ngày quan sát trước ; NULL ở ngày đầu tiên
    revenue_est Nullable(UInt64),            -- units_sold_est * avg_sell_price
    rating_eod Float32,
    rating_cnt_eod UInt32,
    new_ratings Nullable(UInt32),
    days_since_prev_obs Nullable(UInt16),    -- > 1 nghĩa là units_sold_est gộp nhiều ngày không crawl
    n_observations UInt32,
    price_change_cnt UInt32,
    first_observed_at DATETIME('Asia/Ho_Chi_Minh'),
    last_observed_at DATETIME('Asia/Ho_Chi_Minh'),
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE ReplacingMergeTree(updated_at)
PARTITION BY intDiv(date_key, 100)
ORDER BY (product_id, date_key);



-- F3. Grain: 1 lần thay đổi giá bán hoặc giá gốc (event)
CREATE TABLE gold_fact_price_change(
    product_id Int64,
    changed_at DATETIME('Asia/Ho_Chi_Minh'), -- lần quan sát đầu tiên thấy giá mới
    date_key UInt32,
    product_key String,
    brand_key String,
    seller_key String,
    category_key String,
    old_sell_price UInt64,
    new_sell_price UInt64,
    change_amount Int64,
    change_pct Float32,
    direction LowCardinality(String),        -- 'up' | 'down' | 'unchanged' (chỉ giá gốc đổi)
    old_list_price UInt64,
    new_list_price UInt64,
    old_discount_pct Float32,
    new_discount_pct Float32,
    old_price_since DATETIME('Asia/Ho_Chi_Minh'),
    old_price_duration_hours Float32,
    is_list_price_inflated Bool,             -- giá gốc tăng đồng thời % giảm tăng -> nghi "giảm giá ảo"
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(changed_at)
ORDER BY (product_id, changed_at);



-- F4. Grain: 1 review
-- Không partition: cùng 1 review có thể được crawl lại ở tháng khác, partition theo observed_at
-- sẽ làm ReplacingMergeTree không gộp được.
CREATE TABLE gold_fact_review(
    review_key String,                       -- SHA256(url_id, user_name, prod_properties, comment) vì nguồn không có review_id
    product_id Int64,
    product_key String,                      -- version hiện tại của sản phẩm ; '' nếu chưa có snapshot
    brand_key String,
    seller_key String,
    category_key String,
    user_key String,
    reviewed_at_est Nullable(DATETIME('Asia/Ho_Chi_Minh')), -- observed_at - rated_at ('4 năm trước' -> sai số theo năm)
    review_date_key UInt32,                  -- 0 = không xác định
    observed_at DATETIME('Asia/Ho_Chi_Minh'),
    observed_date_key UInt32,
    star_score UInt8,                        -- 0 = không có sao
    is_positive Bool,                        -- >= 4 sao
    is_negative Bool,                        -- 1-2 sao
    comment_length UInt32,
    word_count UInt32,
    image_count UInt16,
    has_image Bool,
    use_time_hours Nullable(Float32),
    review_status LowCardinality(String),    -- 'cực kì hài lòng', 'hài lòng', ...
    prod_properties String,                  -- biến thể đã mua (degenerate dimension)
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE ReplacingMergeTree(updated_at)
ORDER BY (product_id, review_key);



-- F5. Grain: 1 seller x 1 ngày
CREATE TABLE gold_fact_seller_daily(
    date_key UInt32,
    seller_key String,
    seller_rate_eod Float32,
    seller_rate_cnt_eod UInt32,
    new_seller_ratings Nullable(UInt32),
    active_products UInt32,                  -- số sản phẩm của seller được quan sát trong ngày
    avg_sell_price Float64,
    avg_discount_pct Float32,
    units_sold_est UInt64,                   -- tổng các units_sold_est đã biết của sản phẩm
    revenue_est UInt64,
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE ReplacingMergeTree(updated_at)
PARTITION BY intDiv(date_key, 100)
ORDER BY (seller_key, date_key);




-- ---------------------------------------------------------------- AGGREGATE VIEWS (cho dashboard)

CREATE VIEW gold_agg_category_daily AS
SELECT
    f.date_key AS date_key,
    f.category_key AS category_key,
    c.category_path AS category_path,
    c.category_level_1 AS category_level_1,
    c.category_level_2 AS category_level_2,
    uniqExact(f.product_id) AS products,
    sum(f.units_sold_est) AS units_sold_est,
    sum(f.revenue_est) AS revenue_est,
    avg(f.avg_sell_price) AS avg_sell_price,
    avg(f.avg_discount_pct) AS avg_discount_pct,
    sum(f.price_change_cnt) AS price_changes
FROM gold_fact_product_daily AS f FINAL
LEFT JOIN gold_dim_category AS c FINAL ON c.category_key = f.category_key
GROUP BY f.date_key, f.category_key, c.category_path, c.category_level_1, c.category_level_2;



CREATE VIEW gold_agg_brand_daily AS
SELECT
    f.date_key AS date_key,
    f.brand_key AS brand_key,
    b.brand_name AS brand_name,
    b.brand_origin AS brand_origin,
    uniqExact(f.product_id) AS products,
    sum(f.units_sold_est) AS units_sold_est,
    sum(f.revenue_est) AS revenue_est,
    avg(f.avg_sell_price) AS avg_sell_price,
    avg(f.avg_discount_pct) AS avg_discount_pct,
    avg(f.rating_eod) AS avg_rating
FROM gold_fact_product_daily AS f FINAL
LEFT JOIN gold_dim_brand AS b FINAL ON b.brand_key = f.brand_key
GROUP BY f.date_key, f.brand_key, b.brand_name, b.brand_origin;
