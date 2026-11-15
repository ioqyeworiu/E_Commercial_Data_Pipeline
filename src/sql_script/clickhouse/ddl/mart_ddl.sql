-- =====================================================================================
-- DATA MART LAYER: bảng phục vụ dashboard, dựng từ lớp gold
--
-- Loader nằm ở src/python/ETL/e_mart/, chạy theo thứ tự số trong tên file:
--   01  mart_kpi_daily              (đọc trước tiên: các mart khác lấy cờ is_reliable_day từ đây)
--   02  mart_category_daily
--   03  mart_brand_daily
--   04  mart_seller_daily
--   05  mart_price_discount_daily
--   06  mart_product_scorecard      (cần 01 để biết ngày chốt số liệu as_of_date)
--   07  mart_sale_event_product
--   08  mart_review_monthly
--
-- Cách nạp: mỗi loader dựng bảng tạm <mart>__stage (CREATE TABLE ... AS <mart>), INSERT toàn bộ, rồi
-- ALTER TABLE <mart> REPLACE PARTITION tuple() FROM <mart>__stage, rồi DROP bảng tạm.
--   - Mart không có PARTITION BY nên cả bảng là một partition tuple(): REPLACE PARTITION thay
--     toàn bộ dữ liệu trong một thao tác nguyên tử.
--   - Không dùng EXCHANGE TABLES: thư mục data của ClickHouse là bind mount trên Windows,
--     không hỗ trợ renameat2() nên EXCHANGE báo UNSUPPORTED_METHOD.
--   - Chạy lại bao nhiêu lần cũng ra cùng kết quả, không nhân đôi.
--   - Dashboard không bao giờ thấy bảng rỗng hoặc dữ liệu nửa cũ nửa mới.
--   - Vì vậy mart dùng MergeTree thường và truy vấn dashboard KHÔNG cần FINAL.
--   - Đừng thêm PARTITION BY cho mart: REPLACE PARTITION tuple() sẽ không còn thay được cả bảng.
--
-- Quy ước cột:
--   - Lưu THÀNH PHẦN CỘNG ĐƯỢC (sum, count) thay vì tỉ lệ, để dashboard gộp lên cấp cao hơn
--     (category cấp 2 -> cấp 1, ngày -> tháng) mà vẫn đúng. Ví dụ giá TB = sell_price_sum / priced_products,
--     rating TB = rating_sum / rated_products, % giảm TB = discount_pct_sum / products_observed.
--   - products_observed, products_selling... cộng được theo category/brand/seller TRONG CÙNG MỘT NGÀY
--     (mỗi sản phẩm thuộc đúng 1 category, 1 brand, 1 seller ở F2). Cộng theo ngày thì ra "sản phẩm-ngày".
--   - active_brands, active_sellers là uniqExact: KHÔNG cộng được theo bất kỳ chiều nào.
--   - is_reliable_day = false ở ngày crawl chưa đủ (ngày đầu/cuối kỳ crawl, ngày đầu tiên chưa có mốc
--     để tính units_sold_est). Truy vấn dashboard lọc is_reliable_day để số theo ngày không bị kéo thấp.
-- =====================================================================================



-- ---------------------------------------------------------------- HÀM DÙNG CHUNG
-- Một định nghĩa phân khúc cho mọi mart (M5, M6, M7) để các dashboard khớp nhau.
-- SQL UDF của ClickHouse là đối tượng toàn server (không thuộc database nào).
-- Tiền tố số ở nhãn ('1. ', '2. '...) để ORDER BY theo chữ cũng ra đúng thứ tự.

CREATE OR REPLACE FUNCTION mart_price_band AS (p) -> multiIf(
    p = 0, '0. không rõ giá',
    p < 100000, '1. < 100k',
    p < 300000, '2. 100k-300k',
    p < 1000000, '3. 300k-1tr',
    p < 3000000, '4. 1tr-3tr',
    p < 10000000, '5. 3tr-10tr',
    '6. >= 10tr');

CREATE OR REPLACE FUNCTION mart_discount_band AS (d) -> multiIf(
    d <= 0, '0. không giảm',
    d < 10, '1. < 10%',
    d < 20, '2. 10-20%',
    d < 30, '3. 20-30%',
    d < 40, '4. 30-40%',
    '5. >= 40%');



-- M1. Grain: 1 ngày. KPI tổng của toàn sàn (thẻ số, đường xu hướng)
CREATE TABLE IF NOT EXISTS mart_kpi_daily(
    full_date Date,
    date_key UInt32,
    day_of_week UInt8,                        -- 1 = Thứ Hai ... 7 = Chủ Nhật
    day_name LowCardinality(String),
    is_weekend Bool,
    sale_event LowCardinality(String),        -- '' nếu là ngày thường
    holiday_name LowCardinality(String),
    products_observed UInt32,                 -- số sản phẩm có ít nhất 1 lần crawl trong ngày
    products_sales_known UInt32,              -- số sản phẩm có units_sold_est (không NULL)
    products_selling UInt32,                  -- units_sold_est > 0
    sales_coverage_pct Float32,               -- products_sales_known / products_observed * 100
    is_reliable_day Bool,                     -- products_observed >= 90% ngày đông nhất VÀ sales_coverage_pct >= 90
    active_brands UInt32,                     -- không tính member unknown
    active_sellers UInt32,                    -- không tính member unknown
    units_sold_est UInt64,
    revenue_est UInt64,
    priced_products UInt32,                   -- close_sell_price > 0
    sell_price_sum Float64,                   -- tổng close_sell_price của priced_products
    discount_pct_sum Float64,                 -- tổng avg_discount_pct (chia cho products_observed)
    discounted_products UInt32,               -- avg_discount_pct > 0
    deep_discount_products UInt32,            -- max_discount_pct >= 30
    rated_products UInt32,                    -- rating_cnt_eod > 0
    rating_sum Float64,                       -- tổng rating_eod của rated_products
    new_ratings UInt64,
    price_changes UInt32,                     -- từ F3, đã bỏ "đổi giá giả" do nguồn thiếu giá (giá = 0)
    price_up UInt32,
    price_down UInt32,
    inflated_events UInt32,                   -- is_list_price_inflated (nghi giảm giá ảo)
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
ORDER BY (full_date);



-- M2. Grain: 1 ngày x 1 category (leaf). Gộp lên cấp 1/2 bằng sum các thành phần
CREATE TABLE IF NOT EXISTS mart_category_daily(
    full_date Date,
    date_key UInt32,
    sale_event LowCardinality(String),
    is_weekend Bool,
    is_reliable_day Bool,
    category_key String,
    category_path String,
    category_level_1 LowCardinality(String),  -- 'unknown' khi sản phẩm không có category
    category_level_2 LowCardinality(String),
    category_level_3 LowCardinality(String),
    leaf_category String,
    products_observed UInt32,
    products_sales_known UInt32,
    products_selling UInt32,
    units_sold_est UInt64,
    revenue_est UInt64,
    priced_products UInt32,
    sell_price_sum Float64,
    discount_pct_sum Float64,
    discounted_products UInt32,
    deep_discount_products UInt32,
    rated_products UInt32,
    rating_sum Float64,
    new_ratings UInt64,
    price_changes UInt32,
    price_up UInt32,
    price_down UInt32,
    inflated_events UInt32,
    active_brands UInt32,                     -- uniqExact, không cộng được
    active_sellers UInt32,                    -- uniqExact, không cộng được
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
ORDER BY (full_date, category_level_1, category_key);



-- M3. Grain: 1 ngày x 1 brand (đã entity resolution ở gold) x 1 category cấp 1
-- Có category_level_1 trong grain để lọc dashboard theo danh mục và tính thị phần / HHI trong từng danh mục.
-- Một brand bán ở 3 danh mục thì có 3 dòng mỗi ngày: muốn số của cả brand thì sum theo category_level_1.
CREATE TABLE IF NOT EXISTS mart_brand_daily(
    full_date Date,
    date_key UInt32,
    sale_event LowCardinality(String),
    is_reliable_day Bool,
    brand_key String,
    brand_name String,
    brand_origin LowCardinality(String),
    brand_is_unknown Bool,
    category_level_1 LowCardinality(String),  -- 'unknown' khi sản phẩm không có category
    products_observed UInt32,
    products_sales_known UInt32,
    products_selling UInt32,
    units_sold_est UInt64,
    revenue_est UInt64,
    priced_products UInt32,
    sell_price_sum Float64,
    discount_pct_sum Float64,
    discounted_products UInt32,
    deep_discount_products UInt32,
    rated_products UInt32,
    rating_sum Float64,
    new_ratings UInt64,
    price_changes UInt32,
    price_up UInt32,
    price_down UInt32,
    inflated_events UInt32,
    active_sellers UInt32,                    -- uniqExact, không cộng được
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
ORDER BY (full_date, category_level_1, brand_key);



-- M4. Grain: 1 ngày x 1 seller. Lượng bán tính lại từ F2 (giống M2, M3), rating seller lấy từ F5
CREATE TABLE IF NOT EXISTS mart_seller_daily(
    full_date Date,
    date_key UInt32,
    sale_event LowCardinality(String),
    is_reliable_day Bool,
    seller_key String,
    seller_name String,
    is_official Bool,
    seller_is_unknown Bool,
    main_category_level_1 LowCardinality(String),
    seller_rate_eod Float32,                  -- 0 = chưa có đánh giá
    seller_rate_cnt_eod UInt32,               -- LŨY KẾ: không sum theo ngày
    new_seller_ratings UInt32,                -- NULL ở F5 (ngày đầu) được tính là 0
    products_observed UInt32,
    products_sales_known UInt32,
    products_selling UInt32,
    units_sold_est UInt64,
    revenue_est UInt64,
    priced_products UInt32,
    sell_price_sum Float64,
    discount_pct_sum Float64,
    discounted_products UInt32,
    deep_discount_products UInt32,
    rated_products UInt32,
    rating_sum Float64,
    new_ratings UInt64,
    price_changes UInt32,
    price_up UInt32,
    price_down UInt32,
    inflated_events UInt32,
    active_brands UInt32,                     -- uniqExact, không cộng được
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
ORDER BY (full_date, seller_key);



-- M5. Grain: 1 ngày x 1 category cấp 1 x 1 mức giảm giá x 1 phân khúc giá
-- Trả lời: giảm giá sâu hơn có bán được nhiều hơn không, phân khúc giá nào đóng góp doanh thu
CREATE TABLE IF NOT EXISTS mart_price_discount_daily(
    full_date Date,
    date_key UInt32,
    sale_event LowCardinality(String),
    is_reliable_day Bool,
    category_level_1 LowCardinality(String),
    discount_band LowCardinality(String),     -- '0. không giảm', '1. 1-10%', ..., '5. >= 40%' (theo avg_discount_pct)
    price_band LowCardinality(String),        -- '0. không rõ giá', '1. < 100k', ..., '6. >= 10tr' (theo close_sell_price)
    product_days UInt32,                      -- số dòng sản phẩm x ngày
    product_days_sales_known UInt32,          -- units_sold_est không NULL: mẫu số của "bán TB / sản phẩm / ngày"
    products_selling UInt32,
    units_sold_est UInt64,
    revenue_est UInt64,
    discount_pct_sum Float64,
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
ORDER BY (full_date, category_level_1, discount_band, price_band);



-- M6. Grain: 1 sản phẩm (toàn bộ kỳ đã nạp). "Product 360": thuộc tính hiện tại + bán + giá + review
CREATE TABLE IF NOT EXISTS mart_product_scorecard(
    product_id Int64,
    title String,                             -- version hiện tại (SCD2 is_current)
    made_in LowCardinality(String),
    warranty LowCardinality(String),
    n_versions UInt16,                        -- số version SCD2 (> 1 là đã đổi title / xuất xứ / bảo hành)
    category_key String,                      -- các khóa và thuộc tính dưới đây lấy theo ngày quan sát cuối
    category_path String,
    category_level_1 LowCardinality(String),
    category_level_2 LowCardinality(String),
    leaf_category String,
    brand_key String,
    brand_name String,
    brand_is_unknown Bool,
    seller_key String,
    seller_name String,
    seller_is_official Bool,
    seller_is_unknown Bool,
    first_seen_date Date,
    last_seen_date Date,
    days_observed UInt16,
    current_sell_price UInt64,                -- giá bán cuối cùng > 0 ; 0 nếu chưa từng thấy giá
    current_list_price UInt64,
    current_discount_pct Float32,
    price_band LowCardinality(String),        -- theo current_sell_price, cùng thang với M5
    min_sell_price UInt64,                    -- cả kỳ, bỏ giá 0
    max_sell_price UInt64,
    avg_sell_price Float64,
    units_sold_total UInt64,
    revenue_total UInt64,
    days_sales_known UInt16,
    avg_daily_units Float64,                  -- units_sold_total / days_sales_known
    units_last_7d UInt64,                     -- [as_of_date - 6, as_of_date]
    units_prev_7d UInt64,                     -- [as_of_date - 13, as_of_date - 7]
    growth_7d_pct Nullable(Float32),          -- NULL khi units_prev_7d = 0
    sales_trend LowCardinality(String),       -- 'tăng' | 'giảm' | 'ổn định' | 'mới có doanh số' | 'không bán'
    sold_cum_latest UInt32,
    rating_latest Float32,
    rating_cnt_latest UInt32,
    new_ratings_total UInt64,
    price_changes UInt32,                     -- F3, bỏ đổi giá giả
    price_up UInt32,
    price_down UInt32,
    inflated_events UInt32,
    last_price_change_at Nullable(DATETIME('Asia/Ho_Chi_Minh')),
    n_reviews UInt32,                         -- F4, mọi review
    n_rated_reviews UInt32,                   -- star_score > 0
    star_sum UInt64,
    star_1 UInt32,
    star_2 UInt32,
    star_3 UInt32,
    star_4 UInt32,
    star_5 UInt32,
    n_positive UInt32,                        -- >= 4 sao
    n_negative UInt32,                        -- 1-2 sao
    n_with_image UInt32,
    word_sum UInt64,
    avg_star Nullable(Float32),               -- star_sum / n_rated_reviews
    negative_pct Nullable(Float32),           -- n_negative / n_rated_reviews * 100
    revenue_rank UInt32,                      -- 1 = doanh thu cao nhất
    revenue_cum_share_pct Float32,            -- % doanh thu lũy kế tính tới hết sản phẩm này
    abc_class LowCardinality(String),         -- 'A' (nhóm tạo 80% doanh thu đầu) | 'B' (80-95%) | 'C' (còn lại) | 'N' (không có doanh thu)
    as_of_date Date,                          -- ngày tin cậy cuối cùng (mart_kpi_daily) dùng cho 7d
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
ORDER BY (product_id);



-- M7. Grain: 1 ngày sale x 1 sản phẩm có mặt vào ngày sale
-- Đo hiệu quả ngày sale và phát hiện giảm giá ảo bằng giá tham chiếu 30 ngày trước sự kiện
-- (cách làm giống Chỉ thị Omnibus của EU: giá "trước giảm" phải là giá thấp nhất 30 ngày trước đó).
CREATE TABLE IF NOT EXISTS mart_sale_event_product(
    event_date Date,
    event_date_key UInt32,
    sale_event LowCardinality(String),
    product_id Int64,
    category_level_1 LowCardinality(String),  -- theo ngày sale
    category_level_2 LowCardinality(String),
    brand_key String,
    brand_name String,
    seller_key String,
    seller_name String,
    seller_is_official Bool,
    price_band LowCardinality(String),        -- theo ref_sell_median_30d (giá "bình thường")
    baseline_days UInt8,                      -- số ngày có giá trong [D-30, D-1], không tính ngày sale
    ref_sell_median_30d Nullable(UInt64),     -- giá bán "bình thường": trung vị giá bán các ngày baseline
    ref_sell_min_30d Nullable(UInt64),        -- giá bán thấp nhất 30 ngày trước (mọi ngày, kể cả ngày sale trước)
    ref_list_median_30d Nullable(UInt64),     -- giá gốc "bình thường"
    event_sell_price Nullable(UInt64),        -- trung vị giá bán trong ngày sale (bỏ giá 0)
    event_list_price Nullable(UInt64),
    shown_discount_pct Float32,               -- % giảm lớn nhất hiển thị trong ngày sale
    real_discount_pct Nullable(Float32),      -- (ref_sell_median_30d - event_sell_price) / ref_sell_median_30d * 100
    list_inflation_pct Nullable(Float32),     -- (event_list_price - ref_list_median_30d) / ref_list_median_30d * 100
    is_lowest_30d Bool,                       -- event_sell_price <= ref_sell_min_30d
    discount_verdict LowCardinality(String),  -- xem loader 07
    baseline_units_days UInt8,                -- số ngày dùng để tính lượng bán nền
    baseline_units_avg Nullable(Float64),     -- bán TB / ngày ở ngày thường (bỏ ngày sale và ngày ngay sau sale)
    units_event_day UInt32,                   -- units_sold_est ngày D (NULL -> 0)
    units_next_day UInt32,                    -- ngày D+1: phần bán ngày sale bị dồn sang do chu kỳ crawl
    revenue_event_day UInt64,
    uplift_ratio Nullable(Float32),           -- units_event_day / baseline_units_avg ; NULL khi nền = 0
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
ORDER BY (event_date, product_id);



-- M8. Grain: 1 tháng viết review (ước tính) x 1 category x 1 nhóm thâm niên reviewer
CREATE TABLE IF NOT EXISTS mart_review_monthly(
    review_month Date,                        -- toStartOfMonth(reviewed_at_est) ; 1970-01-01 = không xác định
    category_key String,
    category_level_1 LowCardinality(String),  -- 'unknown' khi review chưa gắn được sản phẩm
    category_level_2 LowCardinality(String),
    tenure_band LowCardinality(String),
    n_reviews UInt32,
    n_rated_reviews UInt32,
    star_sum UInt64,
    star_1 UInt32,
    star_2 UInt32,
    star_3 UInt32,
    star_4 UInt32,
    star_5 UInt32,
    n_positive UInt32,
    n_negative UInt32,
    n_with_image UInt32,
    n_with_comment UInt32,                    -- comment_length > 0
    image_sum UInt64,
    word_sum UInt64,
    updated_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now()
)
ENGINE MergeTree
ORDER BY (review_month, category_level_1, category_key, tenure_band);
