-- =====================================================================================
-- DATA QUALITY - TẦNG GOLD (star schema, xem GOLD_LAYER.md)
-- Mọi bảng gold là ReplacingMergeTree -> luôn đọc bằng FINAL. Mỗi fact được quét 1 lần, các check của
-- fact đó tính chung trong lần quét.
--
--   G01 uniqueness      Grain: khóa trùng sau FINAL (phải 0) ; tỉ lệ dòng chưa merge trước FINAL (INFO)
--   G02 integrity       Khóa ngoại của 5 fact không có trong dimension (kể cả date_key -> gold_dim_date)
--   G02b completeness   Tỉ lệ fact trỏ vào unknown member (brand / seller / danh mục không rõ)
--   G03 consistency     SCD2 gold_dim_product: đúng 1 version current, version_no liên tục, valid_to nối tiếp
--                       valid_from của version sau ; snapshot nằm ngoài khoảng hiệu lực của version nó trỏ tới
--   G04 reconciliation  silver -> F1 (tập khóa), F1 -> F2 (số lần quan sát / ngày), số dòng dimension so với
--                       nguồn, F4 so với số review khác nhau ở bronze, F2 -> F5 (tổng bán / doanh thu)
--   G05 validity        F1: giá gốc < giá bán, discount_amount / % sai, date_key / hour lệch observed_at ...
--   G06 consistency     F2: open / close / avg ngoài [min, max], revenue_est sai, units_sold_est NULL sai ngày,
--                       bộ đếm cuối ngày giảm, ngày không crawl (days_since_prev_obs > 1)
--   G07 consistency     F3: direction lệch dấu, thay đổi rỗng, change_amount / % sai, đổi giá giả do giá = 0
--   G08 consistency     F4: cờ positive / negative / has_image lệch, thiếu sao / ngày / sản phẩm, ngày review sau ngày crawl
--   G09 consistency     F5: active_products, tổng bán lệch so với F1 / F2 ; seller_rate ngoài [0,5]
--   G10 validity        gold_dim_date: lịch liền mạch, thuộc tính ngày đúng
--
-- Chạy: bash run_dq.sh gold
-- =====================================================================================



-- ---------------------------------------------------------------- G01 grain
-- ORDER BY của ReplacingMergeTree chính là grain, nên sau FINAL khóa trùng chỉ còn khi grain bị khai báo sai
-- hoặc partition key không suy ra từ ORDER BY (dòng trùng nằm khác partition thì không được gộp).
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'gold', tbl, c.1, c.2, c.3, col, 'all', c.4, c.5, dq_pct(c.5, c.4), '%', c.6, c.7, '', c.8
FROM (
    SELECT 'gold_dim_date' AS tbl, 'date_key' AS col, (SELECT count() FROM gold_dim_date) AS raw_rows,
           count() AS final_rows, uniqExact(date_key) AS keys FROM gold_dim_date FINAL
    UNION ALL SELECT 'gold_dim_category', 'category_key', (SELECT count() FROM gold_dim_category),
           count(), uniqExact(category_key) FROM gold_dim_category FINAL
    UNION ALL SELECT 'gold_dim_brand', 'brand_key', (SELECT count() FROM gold_dim_brand),
           count(), uniqExact(brand_key) FROM gold_dim_brand FINAL
    UNION ALL SELECT 'gold_dim_seller', 'seller_key', (SELECT count() FROM gold_dim_seller),
           count(), uniqExact(seller_key) FROM gold_dim_seller FINAL
    UNION ALL SELECT 'gold_dim_product', 'product_key', (SELECT count() FROM gold_dim_product),
           count(), uniqExact(product_key) FROM gold_dim_product FINAL
    UNION ALL SELECT 'gold_dim_reviewer', 'user_key', (SELECT count() FROM gold_dim_reviewer),
           count(), uniqExact(user_key) FROM gold_dim_reviewer FINAL
    UNION ALL SELECT 'gold_fact_product_snapshot', 'product_id, observed_at', (SELECT count() FROM gold_fact_product_snapshot),
           count(), uniqExact(cityHash64(product_id, observed_at)) FROM gold_fact_product_snapshot FINAL
    UNION ALL SELECT 'gold_fact_product_daily', 'product_id, date_key', (SELECT count() FROM gold_fact_product_daily),
           count(), uniqExact(cityHash64(product_id, date_key)) FROM gold_fact_product_daily FINAL
    UNION ALL SELECT 'gold_fact_price_change', 'product_id, changed_at', (SELECT count() FROM gold_fact_price_change),
           count(), uniqExact(cityHash64(product_id, changed_at)) FROM gold_fact_price_change FINAL
    UNION ALL SELECT 'gold_fact_review', 'review_key', (SELECT count() FROM gold_fact_review),
           count(), uniqExact(review_key) FROM gold_fact_review FINAL
    UNION ALL SELECT 'gold_fact_seller_daily', 'seller_key, date_key', (SELECT count() FROM gold_fact_seller_daily),
           count(), uniqExact(cityHash64(seller_key, date_key)) FROM gold_fact_seller_daily FINAL
)
ARRAY JOIN [
    ('G01', 'Khóa grain trùng sau FINAL', 'uniqueness', final_rows, toUInt64(final_rows - keys), 0, 0,
        'phải bằng 0 ; > 0 nghĩa là ORDER BY không đúng grain hoặc partition tách các bản trùng'),
    ('G01b', 'Dòng chưa được merge (đọc không có FINAL sẽ đếm trùng)', 'uniqueness', raw_rows,
        toUInt64(raw_rows - final_rows), 0, -1, 'INFO: nhắc lý do mọi truy vấn gold phải dùng FINAL')
] AS c;



-- ---------------------------------------------------------------- F1: G02 + G02b + G05
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
WITH
    lower(hex(SHA256('unknown'))) AS unknown_key,
    lower(hex(SHA256(''))) AS empty_category_key
SELECT
    {run_id:String}, 'gold', 'gold_fact_product_snapshot', c.1, c.2, c.3, c.4, scope, total, c.5,
    dq_pct(c.5, total), '%', c.6, c.7, dq_sample(c.5, c.8), c.9
FROM (
    SELECT
        dq_scope(product_id) AS scope,
        count() AS total,
        countIf(product_key NOT IN (SELECT product_key FROM gold_dim_product FINAL)) AS fk_product,
        countIf(brand_key NOT IN (SELECT brand_key FROM gold_dim_brand FINAL)) AS fk_brand,
        countIf(seller_key NOT IN (SELECT seller_key FROM gold_dim_seller FINAL)) AS fk_seller,
        countIf(category_key NOT IN (SELECT category_key FROM gold_dim_category FINAL)) AS fk_category,
        countIf(date_key NOT IN (SELECT date_key FROM gold_dim_date FINAL)) AS fk_date,
        countIf(brand_key = unknown_key) AS unk_brand,
        countIf(seller_key = unknown_key) AS unk_seller,
        countIf(category_key = empty_category_key) AS unk_category,
        countIf(list_price < sell_price) AS f_list_lt_sell,
        countIf(discount_amount != if(list_price > sell_price, list_price - sell_price, 0)) AS f_disc_amount,
        countIf(discount_pct < 0 OR discount_pct > 100) AS f_disc_pct,
        countIf(date_key != toYYYYMMDD(observed_at) OR hour_of_day != toHour(observed_at)) AS f_date,
        countIf(product_rate < 0 OR product_rate > 5 OR seller_rate < 0 OR seller_rate > 5) AS f_rate,
        countIf(sell_price = 0) AS f_zero_price,
        anyIf(product_key, product_key NOT IN (SELECT product_key FROM gold_dim_product FINAL)) AS s_fk_product,
        anyIf(brand_key, brand_key NOT IN (SELECT brand_key FROM gold_dim_brand FINAL)) AS s_fk_brand,
        anyIf(seller_key, seller_key NOT IN (SELECT seller_key FROM gold_dim_seller FINAL)) AS s_fk_seller,
        anyIf(category_key, category_key NOT IN (SELECT category_key FROM gold_dim_category FINAL)) AS s_fk_category,
        toString(anyIf(date_key, date_key NOT IN (SELECT date_key FROM gold_dim_date FINAL))) AS s_fk_date,
        toString(anyIf((product_id, list_price, sell_price), list_price < sell_price)) AS s_list_lt_sell,
        toString(anyIf((product_id, list_price, sell_price, discount_amount),
                       discount_amount != if(list_price > sell_price, list_price - sell_price, 0))) AS s_disc_amount,
        toString(anyIf((product_id, discount_pct), discount_pct < 0 OR discount_pct > 100)) AS s_disc_pct,
        toString(anyIf((product_id, observed_at, date_key, hour_of_day),
                       date_key != toYYYYMMDD(observed_at) OR hour_of_day != toHour(observed_at))) AS s_date,
        toString(anyIf((product_id, product_rate, seller_rate),
                       product_rate < 0 OR product_rate > 5 OR seller_rate < 0 OR seller_rate > 5)) AS s_rate,
        toString(anyIf(product_id, sell_price = 0)) AS s_zero_price
    FROM gold_fact_product_snapshot FINAL
    GROUP BY scope
)
ARRAY JOIN [
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'product_key -> gold_dim_product', fk_product, 0, 0, s_fk_product, ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'brand_key -> gold_dim_brand', fk_brand, 0, 0, s_fk_brand, ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'seller_key -> gold_dim_seller', fk_seller, 0, 0, s_fk_seller, ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'category_key -> gold_dim_category', fk_category, 0, 0, s_fk_category, ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'date_key -> gold_dim_date', fk_date, 0, 0, s_fk_date, ''),
    ('G02b', 'Fact trỏ vào unknown member', 'completeness', 'brand_key = unknown', unk_brand, 10, 30, '',
        'sản phẩm không có brand ; phân tích thị phần brand nên loại ra'),
    ('G02b', 'Fact trỏ vào unknown member', 'completeness', 'seller_key = unknown', unk_seller, 5, 20, '', ''),
    ('G02b', 'Fact trỏ vào unknown member', 'completeness', 'category_key = danh mục rỗng', unk_category, 0.5, 2, '', ''),
    ('G05a', 'list_price < sell_price', 'validity', 'list_price', f_list_lt_sell, 0, 0, s_list_lt_sell,
        'loader lấy list_price = sell_price khi nguồn không có giá gốc'),
    ('G05b', 'discount_amount khác list_price - sell_price', 'validity', 'discount_amount', f_disc_amount, 0, 0, s_disc_amount, ''),
    ('G05c', 'discount_pct ngoài [0, 100]', 'validity', 'discount_pct', f_disc_pct, 0, 0, s_disc_pct, ''),
    ('G05d', 'date_key / hour_of_day không khớp observed_at', 'consistency', 'date_key', f_date, 0, 0, s_date, ''),
    ('G05e', 'product_rate / seller_rate ngoài [0, 5]', 'validity', 'product_rate', f_rate, 0, 0, s_rate, ''),
    ('G05f', 'sell_price = 0 (nguồn thiếu giá)', 'completeness', 'sell_price', f_zero_price, 0.5, 2, s_zero_price,
        'GOLD_LAYER 13.3: phải lọc sell_price > 0 khi phân tích giá')
] AS c;



-- ---------------------------------------------------------------- F2: G02 + G06
-- units_sold_est = NULL đúng ở ngày quan sát ĐẦU TIÊN của mỗi sản phẩm (chưa có mốc), không ở ngày nào khác.
-- sold_eod, rating_cnt_eod là max lũy kế (GOLD_LAYER 13.16) nên không bao giờ được giảm so với ngày trước.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'gold', 'gold_fact_product_daily', c.1, c.2, c.3, c.4, scope, total, c.5,
    dq_pct(c.5, total), '%', c.6, c.7, dq_sample(c.5, c.8), c.9
FROM (
    SELECT
        dq_scope(product_id) AS scope,
        count() AS total,
        countIf(product_key NOT IN (SELECT product_key FROM gold_dim_product FINAL)) AS fk_product,
        countIf(brand_key NOT IN (SELECT brand_key FROM gold_dim_brand FINAL)) AS fk_brand,
        countIf(seller_key NOT IN (SELECT seller_key FROM gold_dim_seller FINAL)) AS fk_seller,
        countIf(category_key NOT IN (SELECT category_key FROM gold_dim_category FINAL)) AS fk_category,
        countIf(date_key NOT IN (SELECT date_key FROM gold_dim_date FINAL)) AS fk_date,
        countIf(NOT (min_sell_price <= open_sell_price AND open_sell_price <= max_sell_price
                 AND min_sell_price <= close_sell_price AND close_sell_price <= max_sell_price
                 AND avg_sell_price >= min_sell_price - 1e-6 AND avg_sell_price <= max_sell_price + 1e-6)) AS f_price_band,
        countIf(isNull(units_sold_est) != isNull(revenue_est)
             OR (units_sold_est IS NOT NULL AND abs(toFloat64(assumeNotNull(revenue_est)) - round(assumeNotNull(units_sold_est) * avg_sell_price)) > 1)) AS f_revenue,
        countIf(isNull(units_sold_est) != (date_key = first_day)) AS f_units_null,
        countIf(prev_sold > sold_eod OR prev_rc > rating_cnt_eod) AS f_counter_down,
        countIf(days_since_prev_obs > 1) AS f_gap,
        countIf(n_observations = 0 OR first_observed_at > last_observed_at
             OR toYYYYMMDD(first_observed_at) != date_key OR toYYYYMMDD(last_observed_at) != date_key) AS f_obs,
        anyIf(product_key, product_key NOT IN (SELECT product_key FROM gold_dim_product FINAL)) AS s_fk_product,
        anyIf(brand_key, brand_key NOT IN (SELECT brand_key FROM gold_dim_brand FINAL)) AS s_fk_brand,
        anyIf(seller_key, seller_key NOT IN (SELECT seller_key FROM gold_dim_seller FINAL)) AS s_fk_seller,
        anyIf(category_key, category_key NOT IN (SELECT category_key FROM gold_dim_category FINAL)) AS s_fk_category,
        toString(anyIf(date_key, date_key NOT IN (SELECT date_key FROM gold_dim_date FINAL))) AS s_fk_date,
        toString(anyIf((product_id, date_key, min_sell_price, open_sell_price, close_sell_price, max_sell_price),
            NOT (min_sell_price <= open_sell_price AND open_sell_price <= max_sell_price
                 AND min_sell_price <= close_sell_price AND close_sell_price <= max_sell_price
                 AND avg_sell_price >= min_sell_price - 1e-6 AND avg_sell_price <= max_sell_price + 1e-6))) AS s_price_band,
        toString(anyIf((product_id, date_key, units_sold_est, revenue_est),
            isNull(units_sold_est) != isNull(revenue_est)
            OR (units_sold_est IS NOT NULL AND abs(toFloat64(assumeNotNull(revenue_est)) - round(assumeNotNull(units_sold_est) * avg_sell_price)) > 1))) AS s_revenue,
        toString(anyIf((product_id, date_key, first_day), isNull(units_sold_est) != (date_key = first_day))) AS s_units_null,
        toString(anyIf((product_id, date_key, prev_sold, sold_eod), prev_sold > sold_eod OR prev_rc > rating_cnt_eod)) AS s_counter_down,
        toString(anyIf((product_id, date_key, days_since_prev_obs), days_since_prev_obs > 1)) AS s_gap,
        toString(anyIf((product_id, date_key), n_observations = 0 OR first_observed_at > last_observed_at
             OR toYYYYMMDD(first_observed_at) != date_key OR toYYYYMMDD(last_observed_at) != date_key)) AS s_obs
    FROM (
        SELECT *,
               min(date_key) OVER (PARTITION BY product_id) AS first_day,
               lagInFrame(toNullable(sold_eod)) OVER w AS prev_sold,
               lagInFrame(toNullable(rating_cnt_eod)) OVER w AS prev_rc
        FROM gold_fact_product_daily FINAL
        WINDOW w AS (PARTITION BY product_id ORDER BY date_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    )
    GROUP BY scope
)
ARRAY JOIN [
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'product_key -> gold_dim_product', fk_product, 0, 0, s_fk_product, ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'brand_key -> gold_dim_brand', fk_brand, 0, 0, s_fk_brand, ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'seller_key -> gold_dim_seller', fk_seller, 0, 0, s_fk_seller, ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'category_key -> gold_dim_category', fk_category, 0, 0, s_fk_category, ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'date_key -> gold_dim_date', fk_date, 0, 0, s_fk_date, ''),
    ('G06a', 'open / close / avg_sell_price nằm ngoài [min, max] của ngày', 'consistency', 'sell_price', f_price_band, 0, 0, s_price_band, ''),
    ('G06b', 'revenue_est khác round(units_sold_est x avg_sell_price)', 'consistency', 'revenue_est', f_revenue, 0, 0, s_revenue, ''),
    ('G06c', 'units_sold_est NULL không đúng ngày quan sát đầu tiên', 'consistency', 'units_sold_est', f_units_null, 0, 0, s_units_null,
        'NULL = chưa biết (ngày đầu), không phải 0'),
    ('G06d', 'sold_eod / rating_cnt_eod giảm so với ngày quan sát trước', 'consistency', 'sold_eod', f_counter_down, 0, 0, s_counter_down,
        'loader 08 dùng max lũy kế nên phải bằng 0'),
    ('G06e', 'Ngày không có lần crawl nào (days_since_prev_obs > 1)', 'completeness', 'days_since_prev_obs', f_gap, 1, 5, s_gap,
        'units_sold_est của ngày sau khoảng trống là tổng nhiều ngày'),
    ('G06f', 'n_observations / first_observed_at / last_observed_at không khớp ngày', 'consistency', 'n_observations', f_obs, 0, 0, s_obs, '')
] AS c;



-- ---------------------------------------------------------------- F3: G02 + G07
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'gold', 'gold_fact_price_change', c.1, c.2, c.3, c.4, scope, total, c.5,
    dq_pct(c.5, total), '%', c.6, c.7, dq_sample(c.5, c.8), c.9
FROM (
    SELECT
        dq_scope(product_id) AS scope,
        count() AS total,
        countIf(product_key NOT IN (SELECT product_key FROM gold_dim_product FINAL)) AS fk_product,
        countIf(brand_key NOT IN (SELECT brand_key FROM gold_dim_brand FINAL)) AS fk_brand,
        countIf(seller_key NOT IN (SELECT seller_key FROM gold_dim_seller FINAL)) AS fk_seller,
        countIf(category_key NOT IN (SELECT category_key FROM gold_dim_category FINAL)) AS fk_category,
        countIf(date_key NOT IN (SELECT date_key FROM gold_dim_date FINAL)) AS fk_date,
        countIf(direction != multiIf(new_sell_price > old_sell_price, 'up', new_sell_price < old_sell_price, 'down', 'unchanged')) AS f_direction,
        countIf(old_sell_price = new_sell_price AND old_list_price = new_list_price) AS f_noop,
        countIf(change_amount != toInt64(new_sell_price) - toInt64(old_sell_price)
             OR (old_sell_price > 0 AND abs(change_pct - change_amount / old_sell_price * 100) > 0.01)) AS f_amount,
        countIf(old_price_duration_hours < 0 OR old_price_since > changed_at) AS f_duration,
        countIf(is_list_price_inflated != (new_list_price > old_list_price AND new_discount_pct > old_discount_pct)) AS f_inflated,
        countIf(old_sell_price = 0 OR new_sell_price = 0) AS f_zero,
        toString(anyIf((product_id, changed_at, old_sell_price, new_sell_price, direction),
            direction != multiIf(new_sell_price > old_sell_price, 'up', new_sell_price < old_sell_price, 'down', 'unchanged'))) AS s_direction,
        toString(anyIf((product_id, changed_at), old_sell_price = new_sell_price AND old_list_price = new_list_price)) AS s_noop,
        toString(anyIf((product_id, changed_at, change_amount, change_pct),
            change_amount != toInt64(new_sell_price) - toInt64(old_sell_price)
            OR (old_sell_price > 0 AND abs(change_pct - change_amount / old_sell_price * 100) > 0.01))) AS s_amount,
        toString(anyIf((product_id, changed_at, old_price_duration_hours), old_price_duration_hours < 0 OR old_price_since > changed_at)) AS s_duration,
        toString(anyIf((product_id, changed_at),
            is_list_price_inflated != (new_list_price > old_list_price AND new_discount_pct > old_discount_pct))) AS s_inflated,
        toString(anyIf((product_id, changed_at, old_sell_price, new_sell_price), old_sell_price = 0 OR new_sell_price = 0)) AS s_zero
    FROM gold_fact_price_change FINAL
    GROUP BY scope
)
ARRAY JOIN [
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'product_key -> gold_dim_product', fk_product, 0, 0, '', ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'brand_key -> gold_dim_brand', fk_brand, 0, 0, '', ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'seller_key -> gold_dim_seller', fk_seller, 0, 0, '', ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'category_key -> gold_dim_category', fk_category, 0, 0, '', ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'date_key -> gold_dim_date', fk_date, 0, 0, '', ''),
    ('G07a', 'direction không khớp dấu của thay đổi giá bán', 'consistency', 'direction', f_direction, 0, 0, s_direction, ''),
    ('G07b', 'Sự kiện không có thay đổi nào (giá bán và giá gốc đều giữ nguyên)', 'consistency', 'new_sell_price', f_noop, 0, 0, s_noop, ''),
    ('G07c', 'change_amount / change_pct tính sai', 'consistency', 'change_amount', f_amount, 0, 0, s_amount, ''),
    ('G07d', 'old_price_duration_hours âm / old_price_since sau changed_at', 'consistency', 'old_price_duration_hours', f_duration, 0, 0, s_duration, ''),
    ('G07e', 'is_list_price_inflated không khớp quy tắc', 'consistency', 'is_list_price_inflated', f_inflated, 0, 0, s_inflated, ''),
    ('G07f', 'Đổi giá giả do giá = 0 (nguồn thiếu giá)', 'validity', 'old_sell_price', f_zero, 1, 101, s_zero,
        'GOLD_LAYER 13.3: lọc old_sell_price > 0 AND new_sell_price > 0 khi phân tích ; WARN để nhớ')
] AS c;



-- ---------------------------------------------------------------- F4: G02 + G08
-- product_key = '' (review của sản phẩm chưa có snapshot) và review_date_key = 0 (không biết ngày) được đo
-- riêng ở G08, nên G02 chỉ xét các khóa có giá trị.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'gold', 'gold_fact_review', c.1, c.2, c.3, c.4, scope, total, c.5,
    dq_pct(c.5, total), '%', c.6, c.7, dq_sample(c.5, c.8), c.9
FROM (
    SELECT
        dq_scope(product_id) AS scope,
        count() AS total,
        countIf(product_key != '' AND product_key NOT IN (SELECT product_key FROM gold_dim_product FINAL)) AS fk_product,
        countIf(brand_key != '' AND brand_key NOT IN (SELECT brand_key FROM gold_dim_brand FINAL)) AS fk_brand,
        countIf(seller_key != '' AND seller_key NOT IN (SELECT seller_key FROM gold_dim_seller FINAL)) AS fk_seller,
        countIf(category_key != '' AND category_key NOT IN (SELECT category_key FROM gold_dim_category FINAL)) AS fk_category,
        countIf(user_key NOT IN (SELECT user_key FROM gold_dim_reviewer FINAL)) AS fk_user,
        countIf(review_date_key != 0 AND review_date_key NOT IN (SELECT date_key FROM gold_dim_date FINAL)) AS fk_review_date,
        countIf(observed_date_key NOT IN (SELECT date_key FROM gold_dim_date FINAL)) AS fk_obs_date,
        countIf(is_positive != (star_score >= 4) OR is_negative != (star_score BETWEEN 1 AND 2)) AS f_flags,
        countIf(has_image != (image_count > 0)) AS f_image,
        countIf(star_score = 0) AS f_no_star,
        countIf(review_date_key = 0) AS f_no_date,
        countIf(product_key = '') AS f_no_product,
        countIf(reviewed_at_est > observed_at) AS f_future,
        countIf(reviewed_at_est IS NOT NULL AND review_date_key != toYYYYMMDD(assumeNotNull(reviewed_at_est))) AS f_date_key,
        toString(anyIf(review_date_key, review_date_key != 0 AND review_date_key NOT IN (SELECT date_key FROM gold_dim_date FINAL))) AS s_review_date,
        toString(anyIf((review_key, star_score, is_positive, is_negative),
            is_positive != (star_score >= 4) OR is_negative != (star_score BETWEEN 1 AND 2))) AS s_flags,
        toString(anyIf(product_id, product_key = '')) AS s_no_product,
        toString(anyIf((review_key, reviewed_at_est, observed_at), reviewed_at_est > observed_at)) AS s_future
    FROM gold_fact_review FINAL
    GROUP BY scope
)
ARRAY JOIN [
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'product_key -> gold_dim_product', fk_product, 0, 0, '', ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'brand_key -> gold_dim_brand', fk_brand, 0, 0, '', ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'seller_key -> gold_dim_seller', fk_seller, 0, 0, '', ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'category_key -> gold_dim_category', fk_category, 0, 0, '', ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'user_key -> gold_dim_reviewer', fk_user, 0, 0, '', ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'review_date_key -> gold_dim_date', fk_review_date, 0, 1, s_review_date,
        'review cũ hơn 2015 nằm ngoài lịch (GOLD_LAYER 13.7)'),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'observed_date_key -> gold_dim_date', fk_obs_date, 0, 0, '', ''),
    ('G08a', 'is_positive / is_negative không khớp star_score', 'consistency', 'star_score', f_flags, 0, 0, s_flags, ''),
    ('G08b', 'has_image không khớp image_count', 'consistency', 'has_image', f_image, 0, 0, '', ''),
    ('G08c', 'Review không có sao (star_score = 0)', 'completeness', 'star_score', f_no_star, 1, 5, '', ''),
    ('G08d', 'Không xác định được ngày viết review (review_date_key = 0)', 'completeness', 'review_date_key', f_no_date, 1, 5, '', ''),
    ('G08e', 'Review chưa gắn được sản phẩm (chưa có snapshot)', 'completeness', 'product_key', f_no_product, 1, 5, s_no_product, ''),
    ('G08f', 'Ngày viết review ước tính sau lúc crawl', 'consistency', 'reviewed_at_est', f_future, 0, 0, s_future, ''),
    ('G08g', 'review_date_key không khớp reviewed_at_est', 'consistency', 'review_date_key', f_date_key, 0, 0, '', '')
] AS c;



-- ---------------------------------------------------------------- F5: G02 + G09
-- F5 lấy active_products từ F1 và lượng bán / doanh thu từ F2 -> tính lại độc lập rồi so.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'gold', 'gold_fact_seller_daily', c.1, c.2, c.3, c.4, 'all', total, c.5,
    dq_pct(c.5, total), '%', c.6, c.7, dq_sample(c.5, c.8), c.9
FROM (
    SELECT
        count() AS total,
        countIf(f5.seller_key NOT IN (SELECT seller_key FROM gold_dim_seller FINAL)) AS fk_seller,
        countIf(f5.date_key NOT IN (SELECT date_key FROM gold_dim_date FINAL)) AS fk_date,
        countIf(f5.active_products != o.ap) AS f_active,
        countIf(f5.units_sold_est != d.u OR f5.revenue_est != d.rv) AS f_sales,
        countIf(f5.seller_rate_eod < 0 OR f5.seller_rate_eod > 5) AS f_rate,
        toString(anyIf((f5.seller_key, f5.date_key, f5.active_products, o.ap), f5.active_products != o.ap)) AS s_active,
        toString(anyIf((f5.seller_key, f5.date_key, f5.units_sold_est, d.u), f5.units_sold_est != d.u OR f5.revenue_est != d.rv)) AS s_sales
    FROM (
        SELECT date_key, seller_key, active_products, units_sold_est, revenue_est, seller_rate_eod
        FROM gold_fact_seller_daily FINAL
    ) AS f5
    LEFT JOIN (
        SELECT date_key, seller_key, count() AS ap
        FROM (SELECT DISTINCT date_key, seller_key, product_id FROM gold_fact_product_snapshot FINAL)
        GROUP BY date_key, seller_key
    ) AS o ON o.date_key = f5.date_key AND o.seller_key = f5.seller_key
    LEFT JOIN (
        SELECT date_key, seller_key, toUInt64(ifNull(sum(units_sold_est), 0)) AS u, toUInt64(ifNull(sum(revenue_est), 0)) AS rv
        FROM gold_fact_product_daily FINAL
        GROUP BY date_key, seller_key
    ) AS d ON d.date_key = f5.date_key AND d.seller_key = f5.seller_key
)
ARRAY JOIN [
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'seller_key -> gold_dim_seller', fk_seller, 0, 0, '', ''),
    ('G02', 'Khóa ngoại không có trong dimension', 'integrity', 'date_key -> gold_dim_date', fk_date, 0, 0, '', ''),
    ('G09a', 'active_products khác số sản phẩm khác nhau của seller trong ngày ở F1', 'reconciliation', 'active_products',
        f_active, 0, 0, s_active, ''),
    ('G09b', 'units_sold_est / revenue_est khác tổng của F2 theo seller x ngày', 'reconciliation', 'units_sold_est',
        f_sales, 0, 0, s_sales, 'F2 gán cả ngày cho seller cuối ngày'),
    ('G09c', 'seller_rate_eod ngoài [0, 5]', 'validity', 'seller_rate_eod', f_rate, 0, 0, '', '')
] AS c;



-- ---------------------------------------------------------------- G03 SCD2 gold_dim_product
-- Loader 05: version_no = row_number theo valid_from ; valid_to = valid_from của version kế tiếp ;
-- is_current = (valid_to IS NULL). Kiểm tra đúng các bất biến đó trên kết quả.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'gold', 'gold_dim_product', c.1, c.2, 'consistency', c.3, scope, products, c.4,
    dq_pct(c.4, products), '%', 0, 0, dq_sample(c.4, c.5), c.6
FROM (
    SELECT
        dq_scope(product_id) AS scope,
        count() AS products,
        countIf(n_current != 1) AS f_current,
        countIf(arrayMap(x -> x.1, v) != arrayMap(i -> toUInt64(i), range(1, toUInt32(n + 1)))) AS f_version,
        countIf(n_bad_valid_to > 0) AS f_valid_to,
        countIf(arrayExists(i -> v[i].3 IS NULL OR assumeNotNull(v[i].3) != v[i + 1].2 OR v[i].2 >= v[i + 1].2, range(1, toUInt32(n)))) AS f_chain,
        countIf(n > 1) AS multi,
        max(n) AS max_versions,
        toString(anyIf(product_id, n_current != 1)) AS s_current,
        toString(anyIf(product_id, arrayMap(x -> x.1, v) != arrayMap(i -> toUInt64(i), range(1, toUInt32(n + 1))))) AS s_version,
        toString(anyIf(product_id, n_bad_valid_to > 0)) AS s_valid_to,
        toString(anyIf(product_id, arrayExists(i -> v[i].3 IS NULL OR assumeNotNull(v[i].3) != v[i + 1].2 OR v[i].2 >= v[i + 1].2, range(1, toUInt32(n))))) AS s_chain
    FROM (
        SELECT
            product_id,
            count() AS n,
            countIf(is_current) AS n_current,
            countIf(is_current != (valid_to IS NULL)) AS n_bad_valid_to,
            arraySort(x -> x.1, groupArray((toUInt64(version_no), valid_from, valid_to))) AS v
        FROM gold_dim_product FINAL
        GROUP BY product_id
    )
    GROUP BY scope
)
ARRAY JOIN [
    ('G03a', 'Sản phẩm không có đúng 1 version is_current', 'is_current', f_current, s_current,
        concat(toString(multi), ' sản phẩm có > 1 version ; nhiều nhất ', toString(max_versions), ' version')),
    ('G03b', 'version_no không liên tục 1..n', 'version_no', f_version, s_version, ''),
    ('G03c', 'is_current không khớp valid_to IS NULL', 'valid_to', f_valid_to, s_valid_to, ''),
    ('G03d', 'valid_to của version không nối tiếp valid_from của version sau', 'valid_to', f_chain, s_chain, 'khoảng hiệu lực bị hở / chồng')
] AS c;

-- G03e. Snapshot (F1) trỏ vào version mà observed_at nằm ngoài [valid_from, valid_to).
-- Xảy ra khi thuộc tính quay lại giá trị cũ (A -> B -> A): product_sk là hash thuộc tính nên lần A thứ 2 dùng lại
-- khóa của version đã đóng. Không sai khóa, nhưng truy vấn "thuộc tính tại thời điểm" theo khoảng hiệu lực sẽ lệch.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'gold', 'gold_fact_product_snapshot', 'G03e',
    'Snapshot nằm ngoài khoảng hiệu lực của version nó trỏ tới (thuộc tính quay lại giá trị cũ)', 'consistency',
    'product_key', dq_scope(f.product_id) AS scope, count() AS total,
    countIf(f.observed_at < p.valid_from OR (p.valid_to IS NOT NULL AND f.observed_at >= assumeNotNull(p.valid_to))) AS bad,
    dq_pct(bad, total), '%', 0, 0.1,
    dq_sample(bad, anyIf((f.product_id, f.observed_at),
        f.observed_at < p.valid_from OR (p.valid_to IS NOT NULL AND f.observed_at >= assumeNotNull(p.valid_to)))),
    concat(toString(uniqExactIf(f.product_id,
        f.observed_at < p.valid_from OR (p.valid_to IS NOT NULL AND f.observed_at >= assumeNotNull(p.valid_to)))), ' sản phẩm')
FROM (SELECT product_id, observed_at, product_key FROM gold_fact_product_snapshot FINAL) AS f
INNER JOIN (SELECT product_key, valid_from, valid_to FROM gold_dim_product FINAL) AS p ON p.product_key = f.product_key
GROUP BY scope;



-- ---------------------------------------------------------------- G04 đối soát silver / bronze -> gold
-- G04a: tập lần crawl của F1 = tập (url_id, observed_at) của silver_snapshot (url_id > 0 như loader 07).
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'gold', 'gold_fact_product_snapshot', c.1, c.2, 'reconciliation', 'product_id, observed_at', scope,
    c.3, c.4, dq_pct(c.4, c.3), '%', 0, 0, dq_sample(c.4, c.5), c.6
FROM (
    SELECT
        dq_scope(uid) AS scope,
        countIf(in_silver) AS silver_keys,
        countIf(in_gold) AS gold_keys,
        countIf(in_silver AND NOT in_gold) AS missing,
        countIf(in_gold AND NOT in_silver) AS orphan,
        anyIf(uid, in_silver AND NOT in_gold) AS s_missing,
        anyIf(uid, in_gold AND NOT in_silver) AS s_orphan
    FROM (
        SELECT kh, max(src = 0) AS in_silver, max(src = 1) AS in_gold, any(uid) AS uid
        FROM (
            SELECT cityHash64(url_id, observed_at) AS kh, 0 AS src, url_id AS uid FROM silver_snapshot WHERE url_id > 0
            UNION ALL
            SELECT cityHash64(product_id, observed_at), 1, product_id FROM gold_fact_product_snapshot FINAL
        )
        GROUP BY kh
    )
    GROUP BY scope
)
ARRAY JOIN [
    ('G04a', 'Lần crawl có ở silver_snapshot nhưng không có ở F1', silver_keys, missing, s_missing,
        concat('silver ', toString(silver_keys), ' -> F1 ', toString(gold_keys))),
    ('G04b', 'Lần crawl ở F1 không có ở silver_snapshot (orphan, gold chưa dựng lại)', gold_keys, orphan, s_orphan, '')
] AS c;

-- G04c: mỗi (sản phẩm, ngày) của F1 có đúng 1 dòng F2 với n_observations = số lần crawl trong ngày.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'gold', 'gold_fact_product_daily', 'G04c',
    'Sản phẩm x ngày của F1 không khớp F2 (thiếu / thừa / n_observations sai)', 'reconciliation', 'n_observations',
    dq_scope(pid) AS scope, count() AS days, countIf(n_f1 != n_f2) AS bad, dq_pct(bad, days), '%', 0, 0,
    dq_sample(bad, anyIf((pid, dk, n_f1, n_f2), n_f1 != n_f2)),
    concat('thiếu ở F2: ', toString(countIf(n_f2 = 0)), ' ; thừa ở F2: ', toString(countIf(n_f1 = 0)))
FROM (
    SELECT pid, dk, sumIf(n, src = 1) AS n_f1, sumIf(n, src = 2) AS n_f2
    FROM (
        SELECT product_id AS pid, date_key AS dk, count() AS n, 1 AS src
        FROM gold_fact_product_snapshot FINAL GROUP BY pid, dk
        UNION ALL
        SELECT product_id, date_key, toUInt64(n_observations), 2 FROM gold_fact_product_daily FINAL
    )
    GROUP BY pid, dk
)
GROUP BY scope;

-- G04d: số dòng dimension = số thực thể ở nguồn theo đúng quy tắc khóa của loader (kể cả member 'unknown').
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'gold', tbl, 'G04d', 'Số dòng dimension / F4 khác số thực thể ở nguồn', 'reconciliation', src, 'all',
    expected, toUInt64(abs(toInt64(actual) - toInt64(expected))),
    dq_pct(abs(toInt64(actual) - toInt64(expected)), expected), '%', 0, 0, '',
    concat('nguồn ', toString(expected), ' -> gold ', toString(actual))
FROM (
    SELECT 'gold_dim_product' AS tbl, 'silver_product.product_sk' AS src,
           (SELECT uniqExact(product_sk) FROM silver_product) AS expected,
           (SELECT count() FROM gold_dim_product FINAL) AS actual
    UNION ALL
    SELECT 'gold_dim_category', 'silver_category.category_sk',
           (SELECT uniqExact(category_sk) FROM silver_category), (SELECT count() FROM gold_dim_category FINAL)
    UNION ALL
    SELECT 'gold_dim_brand', 'silver_brand.brand_normalized + unknown',
           (SELECT uniqExact(k) FROM (SELECT if(brand_normalized = '', 'unknown', brand_normalized) AS k FROM silver_brand
                                      UNION ALL SELECT 'unknown')),
           (SELECT count() FROM gold_dim_brand FINAL)
    UNION ALL
    SELECT 'gold_dim_seller', 'silver_seller.name + unknown',
           (SELECT uniqExact(k) FROM (SELECT if(name = '', 'unknown', name) AS k FROM silver_seller UNION ALL SELECT 'unknown')),
           (SELECT count() FROM gold_dim_seller FINAL)
    UNION ALL
    SELECT 'gold_dim_reviewer', 'bronze_user_review.user_name',
           (SELECT uniqExact(if(user_name = '', 'unknown', user_name)) FROM bronze_user_review),
           (SELECT count() FROM gold_dim_reviewer FINAL)
    UNION ALL
    SELECT 'gold_fact_review', 'bronze_user_review: hash(url_id, user_name, prod_properties, comment)',
           (SELECT uniqExact(concat_ws('||', toString(url_id), user_name, prod_properties, comment)) FROM bronze_user_review),
           (SELECT count() FROM gold_fact_review FINAL)
);



-- ---------------------------------------------------------------- G10 lịch gold_dim_date
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'gold', 'gold_dim_date', c.1, c.2, 'validity', c.3, 'all', total, c.4, dq_pct(c.4, total), '%', 0, 0,
    dq_sample(c.4, c.5), c.6
FROM (
    SELECT
        count() AS total,
        toUInt64(dateDiff('day', min(full_date), max(full_date)) + 1 - count()) AS f_gap,
        countIf(date_key != toYYYYMMDD(full_date) OR day != toDayOfMonth(full_date) OR month != toMonth(full_date)
             OR year != toYear(full_date) OR quarter != toQuarter(full_date) OR year_month != toYYYYMM(full_date)
             OR day_of_week != toDayOfWeek(full_date) OR week_of_year != toISOWeek(full_date)
             OR is_weekend != (toDayOfWeek(full_date) >= 6)) AS f_attr,
        countIf(is_double_day != (toDayOfMonth(full_date) = toMonth(full_date))) AS f_double,
        toString(anyIf(full_date, date_key != toYYYYMMDD(full_date) OR day != toDayOfMonth(full_date) OR month != toMonth(full_date)
             OR year != toYear(full_date) OR quarter != toQuarter(full_date) OR year_month != toYYYYMM(full_date)
             OR day_of_week != toDayOfWeek(full_date) OR week_of_year != toISOWeek(full_date)
             OR is_weekend != (toDayOfWeek(full_date) >= 6))) AS s_attr,
        toString(anyIf(full_date, is_double_day != (toDayOfMonth(full_date) = toMonth(full_date)))) AS s_double,
        concat(toString(min(full_date)), ' -> ', toString(max(full_date))) AS span
    FROM gold_dim_date FINAL
)
ARRAY JOIN [
    ('G10a', 'Lịch bị hở (thiếu ngày giữa min và max)', 'full_date', f_gap, '', span),
    ('G10b', 'Thuộc tính ngày sai (date_key, thứ, tuần ISO, quý, cuối tuần ...)', 'full_date', f_attr, s_attr, ''),
    ('G10c', 'is_double_day sai (ngày = tháng: 1.1 ... 12.12)', 'is_double_day', f_double, s_double, '')
] AS c;
