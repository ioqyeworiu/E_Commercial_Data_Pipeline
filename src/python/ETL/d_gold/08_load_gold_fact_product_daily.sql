-- F2. 1 dòng / sản phẩm / ngày.
-- sold và product_rate_cnt là số LŨY KẾ -> lượng bán trong ngày = chênh lệch với ngày quan sát trước.
-- Ngày đầu tiên của mỗi sản phẩm không có mốc so sánh -> units_sold_est = NULL.
-- Tính lại toàn bộ để chênh lệch ngày đầu kỳ luôn đúng.
--
-- Bộ đếm lũy kế bị TRỐNG ở một lần crawl: bronze (normalize_sold, normalize_rating_count) đổi ô trống
-- thành 0, nên F1 có sold_cum = 0 xen giữa các giá trị dương. Nếu lần đó là lần crawl cuối ngày thì
-- sold_eod = 0 và ngày hôm sau bị tính bán = TOÀN BỘ số lũy kế (dữ liệu test: 34% số bán là ảo vì lỗi này).
-- Cách xử lý: bộ đếm lũy kế chỉ tăng, nên dùng MAX LŨY KẾ (running max) theo thời gian của từng sản phẩm
-- thay cho giá trị thô. Lần crawl bị trống giữ nguyên mức cao nhất đã thấy, không kéo bộ đếm về 0.
-- Đánh đổi: nếu Tiki reset "đã bán" của sản phẩm về số nhỏ hơn thì phần bán sau reset chỉ được tính
-- khi vượt lại mức cũ (tính thiếu, không tính thừa).
INSERT INTO gold_fact_product_daily (
    date_key,
    product_id,
    product_key,
    brand_key,
    seller_key,
    category_key,
    open_sell_price,
    close_sell_price,
    min_sell_price,
    max_sell_price,
    avg_sell_price,
    close_list_price,
    avg_discount_pct,
    max_discount_pct,
    sold_eod,
    units_sold_est,
    revenue_est,
    rating_eod,
    rating_cnt_eod,
    new_ratings,
    days_since_prev_obs,
    n_observations,
    price_change_cnt,
    first_observed_at,
    last_observed_at
)
WITH
obs AS (
    SELECT
        *,
        lagInFrame(toNullable(sell_price), 1) OVER w_obs AS prev_sell_price,
        -- max lũy kế của bộ đếm: bỏ qua các lần crawl đọc ra 0 vì ô bị trống
        max(sold_cum) OVER w_obs AS sold_cum_clean,
        max(product_rate_cnt) OVER w_obs AS rate_cnt_clean
    FROM gold_fact_product_snapshot FINAL
    WINDOW w_obs AS (PARTITION BY product_id ORDER BY observed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
daily AS (
    SELECT
        date_key,
        product_id,
        toDate(min(observed_at)) AS obs_date,
        argMax(product_key, observed_at) AS d_product_key,
        argMax(brand_key, observed_at) AS d_brand_key,
        argMax(seller_key, observed_at) AS d_seller_key,
        argMax(category_key, observed_at) AS d_category_key,
        argMin(sell_price, observed_at) AS d_open_sell_price,
        argMax(sell_price, observed_at) AS d_close_sell_price,
        min(sell_price) AS d_min_sell_price,
        max(sell_price) AS d_max_sell_price,
        avg(sell_price) AS d_avg_sell_price,
        argMax(list_price, observed_at) AS d_close_list_price,
        avg(discount_pct) AS d_avg_discount_pct,
        max(discount_pct) AS d_max_discount_pct,
        max(sold_cum_clean) AS d_sold_eod,                         -- = giá trị max lũy kế ở lần crawl cuối ngày
        -- rating không phải bộ đếm nên không lấy max ; chỉ bỏ lần crawl có rating 0 (ô trống hoặc chưa có đánh giá)
        argMaxIf(product_rate, observed_at, product_rate > 0) AS d_rating_eod,
        max(rate_cnt_clean) AS d_rating_cnt_eod,
        count() AS d_n_observations,
        countIf(prev_sell_price IS NOT NULL AND prev_sell_price != sell_price) AS d_price_change_cnt,
        min(observed_at) AS d_first_observed_at,
        max(observed_at) AS d_last_observed_at
    FROM obs
    GROUP BY date_key, product_id
),
with_prev AS (
    SELECT
        *,
        lagInFrame(toNullable(d_sold_eod), 1) OVER w AS prev_sold_eod,
        lagInFrame(toNullable(d_rating_cnt_eod), 1) OVER w AS prev_rating_cnt_eod,
        lagInFrame(toNullable(obs_date), 1) OVER w AS prev_obs_date
    FROM daily
    WINDOW w AS (PARTITION BY product_id ORDER BY date_key)
)
SELECT
    date_key,
    product_id,
    d_product_key,
    d_brand_key,
    d_seller_key,
    d_category_key,
    d_open_sell_price,
    d_close_sell_price,
    d_min_sell_price,
    d_max_sell_price,
    d_avg_sell_price,
    d_close_list_price,
    toFloat32(d_avg_discount_pct),
    d_max_discount_pct,
    d_sold_eod,
    -- clamp về 0: Tiki đôi khi reset / làm tròn "đã bán"
    if(prev_sold_eod IS NULL, NULL,
       toUInt32(greatest(toInt64(d_sold_eod) - toInt64(prev_sold_eod), 0))) AS units_sold_est,
    if(units_sold_est IS NULL, NULL,
       toUInt64(round(units_sold_est * d_avg_sell_price))) AS revenue_est,
    d_rating_eod,
    d_rating_cnt_eod,
    if(prev_rating_cnt_eod IS NULL, NULL,
       toUInt32(greatest(toInt64(d_rating_cnt_eod) - toInt64(prev_rating_cnt_eod), 0))) AS new_ratings,
    if(prev_obs_date IS NULL, NULL, toUInt16(dateDiff('day', prev_obs_date, obs_date))) AS days_since_prev_obs,
    d_n_observations,
    d_price_change_cnt,
    d_first_observed_at,
    d_last_observed_at
FROM with_prev;
