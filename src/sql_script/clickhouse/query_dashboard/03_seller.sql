-- =====================================================================================
-- DASHBOARD 3: NGƯỜI BÁN (SELLER)
-- Câu hỏi: shop official có bán tốt hơn shop thường không, doanh thu có tập trung vào vài seller không,
-- seller nào dẫn đầu, seller nào có dấu hiệu rủi ro (giảm giá ảo, review tiêu cực).
--
-- Tham số: {date_from:Date}, {date_to:Date}, {category_l1:String} ('' = mọi danh mục ; lọc theo
-- main_category_level_1 = danh mục chính của seller trên cả kỳ).
-- Seller được nhận diện theo tên (gold đã gộp các bản ghi silver cùng tên). "Không rõ người bán" bị loại.
-- =====================================================================================


-- [3.1] Thẻ KPI seller | Biểu đồ: KPI cards
WITH
    {date_from:Date} AS d_from,
    {date_to:Date} AS d_to,
    sellers AS (
        SELECT
            seller_key,
            any(is_official) AS is_official,
            sum(revenue_est) AS revenue,
            argMax(seller_rate_eod, full_date) AS rate_last,
            argMax(seller_rate_cnt_eod, full_date) AS rate_cnt_last,
            avg(products_observed) AS products_per_day
        FROM mart_seller_daily
        WHERE is_reliable_day AND NOT seller_is_unknown
          AND full_date BETWEEN d_from AND d_to
          AND ({category_l1:String} = '' OR main_category_level_1 = {category_l1:String})
        GROUP BY seller_key
    )
SELECT
    count() AS active_sellers,
    countIf(is_official) AS official_sellers,
    round(countIf(is_official) / count() * 100, 1) AS official_sellers_pct,
    round(sumIf(revenue, is_official) / sum(revenue) * 100, 1) AS official_revenue_share_pct,
    countIf(revenue > 0) AS sellers_with_sales,
    round(sum(revenue) / nullIf(countIf(revenue > 0), 0)) AS revenue_per_selling_seller,
    round(median(revenue)) AS median_seller_revenue,
    round(avgIf(rate_last, rate_cnt_last > 0), 2) AS avg_seller_rating,        -- bỏ seller chưa có đánh giá
    round(avg(products_per_day), 1) AS avg_products_per_seller
FROM sellers;


-- [3.2] Official vs seller thường | Biểu đồ: bar nhóm hoặc bảng so sánh 2 cột
-- inflated_per_1k_changes = số lần nghi giảm giá ảo trên 1.000 lần đổi giá.
WITH
    {date_from:Date} AS d_from,
    {date_to:Date} AS d_to,
    (SELECT countIf(full_date BETWEEN d_from AND d_to) FROM mart_kpi_daily WHERE is_reliable_day) AS days_cur
SELECT
    if(is_official, 'Official / chính hãng', 'Seller thường') AS seller_type,
    uniqExact(seller_key) AS sellers,
    sum(revenue_est) AS revenue,
    round(sum(revenue_est) / sum(sum(revenue_est)) OVER () * 100, 1) AS revenue_share_pct,
    round(sum(revenue_est) / uniqExact(seller_key) / nullIf(days_cur, 0)) AS revenue_per_seller_per_day,
    round(sum(products_observed) / uniqExact(seller_key) / nullIf(days_cur, 0), 1) AS products_per_seller,
    round(sum(revenue_est) / nullIf(sum(units_sold_est), 0)) AS avg_selling_price,
    round(sum(units_sold_est) / nullIf(sum(products_sales_known), 0), 2) AS units_per_product_day,
    round(sum(discount_pct_sum) / nullIf(sum(products_observed), 0), 1) AS avg_discount_pct,
    round(sum(rating_sum) / nullIf(sum(rated_products), 0), 2) AS avg_product_rating,
    round(avgIf(seller_rate_eod, seller_rate_cnt_eod > 0), 2) AS avg_seller_rating,
    round(sum(inflated_events) / nullIf(sum(price_changes), 0) * 1000, 1) AS inflated_per_1k_changes
FROM mart_seller_daily
WHERE is_reliable_day AND NOT seller_is_unknown
  AND full_date BETWEEN d_from AND d_to
  AND ({category_l1:String} = '' OR main_category_level_1 = {category_l1:String})
GROUP BY seller_type
ORDER BY revenue DESC;


-- [3.3] Bảng xếp hạng top 20 seller | Biểu đồ: table (conditional formatting cột tăng trưởng, rating,
-- % review tiêu cực). Cột review lấy từ mart_product_scorecard (toàn kỳ, theo seller cuối kỳ của sản phẩm).
WITH
    {date_from:Date} AS d_from,
    {date_to:Date} AS d_to,
    dateDiff('day', d_from, d_to) + 1 AS n_days,
    d_from - n_days AS p_from,
    d_from - 1 AS p_to,
    (SELECT countIf(full_date BETWEEN d_from AND d_to) FROM mart_kpi_daily WHERE is_reliable_day) AS days_cur,
    (SELECT countIf(full_date BETWEEN p_from AND p_to) FROM mart_kpi_daily WHERE is_reliable_day) AS days_prev,
    sales AS (
        SELECT
            seller_key,
            any(seller_name) AS seller_name,
            any(is_official) AS is_official,
            any(main_category_level_1) AS main_category,
            sumIf(revenue_est, full_date >= d_from) AS revenue,
            sumIf(revenue_est, full_date < d_from) AS revenue_prev,
            sumIf(units_sold_est, full_date >= d_from) AS units,
            sumIf(products_observed, full_date >= d_from) AS product_days,
            sumIf(discount_pct_sum, full_date >= d_from) AS discount_sum,
            argMaxIf(seller_rate_eod, full_date, full_date >= d_from) AS seller_rating,
            argMaxIf(seller_rate_cnt_eod, full_date, full_date >= d_from) AS seller_rating_cnt,
            sumIf(new_seller_ratings, full_date >= d_from) AS new_seller_ratings,
            sumIf(price_changes, full_date >= d_from) AS price_changes,
            sumIf(inflated_events, full_date >= d_from) AS inflated_events
        FROM mart_seller_daily
        WHERE is_reliable_day AND NOT seller_is_unknown
          AND full_date BETWEEN p_from AND d_to
          AND ({category_l1:String} = '' OR main_category_level_1 = {category_l1:String})
        GROUP BY seller_key
        ORDER BY revenue DESC
        LIMIT 20
    ),
    reviews AS (
        SELECT
            seller_key,
            sum(n_rated_reviews) AS rated_reviews,
            sum(star_sum) AS star_sum,
            sum(n_negative) AS negative
        FROM mart_product_scorecard
        WHERE seller_key IN (SELECT seller_key FROM sales)
        GROUP BY seller_key
    )
SELECT
    row_number() OVER (ORDER BY s.revenue DESC) AS rank,
    s.seller_name,
    if(s.is_official, 'official', '') AS badge,
    s.main_category,
    s.revenue,
    round(s.revenue / (SELECT sum(revenue_est) FROM mart_seller_daily
                       WHERE is_reliable_day AND full_date BETWEEN d_from AND d_to) * 100, 2) AS market_share_pct,
    if(days_prev = 0, NULL,
       round((s.revenue / days_cur) / nullIf(s.revenue_prev / days_prev, 0) * 100 - 100, 1)) AS growth_pct,
    s.units,
    round(s.product_days / nullIf(days_cur, 0)) AS products_per_day,
    round(s.discount_sum / nullIf(s.product_days, 0), 1) AS avg_discount_pct,
    round(s.seller_rating, 1) AS seller_rating,
    s.seller_rating_cnt,
    s.new_seller_ratings,
    round(r.star_sum / nullIf(r.rated_reviews, 0), 2) AS avg_review_star,
    round(r.negative / nullIf(r.rated_reviews, 0) * 100, 1) AS negative_review_pct,
    s.inflated_events,
    s.seller_key
FROM sales AS s
LEFT JOIN reviews AS r ON r.seller_key = s.seller_key
ORDER BY rank;


-- [3.4] Diễn biến rating và lượt đánh giá mới của 5 seller lớn nhất | Biểu đồ: multi-line (rating)
-- + cột (new_seller_ratings) ; rating là non-additive nên chỉ vẽ từng seller, không cộng.
WITH top5 AS (
    SELECT seller_key
    FROM mart_seller_daily
    WHERE is_reliable_day AND NOT seller_is_unknown
      AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
      AND ({category_l1:String} = '' OR main_category_level_1 = {category_l1:String})
    GROUP BY seller_key
    ORDER BY sum(revenue_est) DESC, seller_key
    LIMIT 5
)
SELECT
    full_date,
    seller_name,
    seller_rate_eod,
    seller_rate_cnt_eod,
    new_seller_ratings,
    revenue_est
FROM mart_seller_daily
WHERE is_reliable_day
  AND seller_key IN (SELECT seller_key FROM top5)
  AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
ORDER BY seller_name, full_date;


-- [3.5] Độ tập trung doanh thu theo seller | Biểu đồ: bar (top 1% / 5% / 10% / 20% / 50% seller
-- chiếm bao nhiêu % doanh thu) ; kèm Gini để so sánh giữa các kỳ (0 = đều nhau, 1 = 1 seller chiếm hết).
WITH ranked AS (
    SELECT
        revenue,
        row_number() OVER (ORDER BY revenue DESC, seller_key) AS rk,
        count() OVER () AS n,
        sum(revenue) OVER () AS total
    FROM (
        SELECT seller_key, sum(revenue_est) AS revenue
        FROM mart_seller_daily
        WHERE is_reliable_day AND NOT seller_is_unknown
          AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
          AND ({category_l1:String} = '' OR main_category_level_1 = {category_l1:String})
        GROUP BY seller_key
    )
)
SELECT
    any(n) AS sellers,
    round(sumIf(revenue, rk <= ceil(n * 0.01)) / any(total) * 100, 1) AS top_1pct_share,
    round(sumIf(revenue, rk <= ceil(n * 0.05)) / any(total) * 100, 1) AS top_5pct_share,
    round(sumIf(revenue, rk <= ceil(n * 0.10)) / any(total) * 100, 1) AS top_10pct_share,
    round(sumIf(revenue, rk <= ceil(n * 0.20)) / any(total) * 100, 1) AS top_20pct_share,
    round(sumIf(revenue, rk <= ceil(n * 0.50)) / any(total) * 100, 1) AS top_50pct_share,
    -- Gini: với i = thứ hạng TĂNG dần (i = n + 1 - rk), G = (2 * sum(i * x) / sum(x) - (n + 1)) / n
    round((2 * sum((n + 1 - rk) * revenue) / any(total) - (any(n) + 1)) / any(n), 3) AS gini
FROM ranked;


-- [3.6] Danh sách seller cần theo dõi (rủi ro) | Biểu đồ: table có cờ cảnh báo
-- Điểm rủi ro 0-3, mỗi điều kiện 1 điểm:
--   (a) có >= 3 lần nghi giảm giá ảo trong kỳ
--   (b) % review 1-2 sao >= 15% (tối thiểu 30 review có sao)
--   (c) rating seller giảm >= 0,2 điểm từ đầu đến cuối kỳ
WITH
    {date_from:Date} AS d_from,
    {date_to:Date} AS d_to,
    sales AS (
        SELECT
            seller_key,
            any(seller_name) AS seller_name,
            any(is_official) AS is_official,
            sum(revenue_est) AS revenue,
            sum(inflated_events) AS inflated_events,
            sum(price_changes) AS price_changes,
            argMinIf(seller_rate_eod, full_date, seller_rate_cnt_eod > 0) AS rating_first,
            argMaxIf(seller_rate_eod, full_date, seller_rate_cnt_eod > 0) AS rating_last
        FROM mart_seller_daily
        WHERE is_reliable_day AND NOT seller_is_unknown
          AND full_date BETWEEN d_from AND d_to
          AND ({category_l1:String} = '' OR main_category_level_1 = {category_l1:String})
        GROUP BY seller_key
    ),
    reviews AS (
        SELECT seller_key, sum(n_rated_reviews) AS rated_reviews, sum(n_negative) AS negative
        FROM mart_product_scorecard
        GROUP BY seller_key
    ),
    scored AS (
        SELECT
            s.*,
            r.rated_reviews,
            if(r.rated_reviews >= 30, round(r.negative / r.rated_reviews * 100, 1), NULL) AS negative_review_pct,
            round(s.rating_last - s.rating_first, 2) AS rating_change,
            s.inflated_events >= 3 AS flag_inflated,
            ifNull(negative_review_pct >= 15, false) AS flag_negative,
            rating_change <= -0.2 AS flag_rating_drop
        FROM sales AS s
        LEFT JOIN reviews AS r ON r.seller_key = s.seller_key
    )
SELECT
    seller_name,
    if(is_official, 'official', '') AS badge,
    toUInt8(flag_inflated) + toUInt8(flag_negative) + toUInt8(flag_rating_drop) AS risk_score,
    arrayStringConcat(arrayFilter(x -> x != '', [
        if(flag_inflated, 'giảm giá ảo', ''),
        if(flag_negative, 'review tiêu cực', ''),
        if(flag_rating_drop, 'rating giảm', '')]), ', ') AS reasons,
    revenue,
    inflated_events,
    price_changes,
    rated_reviews,
    negative_review_pct,
    rating_first,
    rating_last,
    rating_change,
    seller_key
FROM scored
WHERE risk_score > 0
ORDER BY risk_score DESC, revenue DESC
LIMIT 50;
