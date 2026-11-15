-- M6. 1 dòng / sản phẩm (toàn bộ kỳ đã nạp). Chạy sau 01 (lấy as_of_date = ngày tin cậy cuối cùng).
-- Nguồn: F2 (bán, giá, rating), F3 (đổi giá), F4 (review), gold_dim_product (SCD2), dim brand/seller/category.
-- Chỉ gồm sản phẩm có trong F2 (đã có snapshot). Review của sản phẩm chưa có snapshot không vào mart này.
DROP TABLE IF EXISTS mart_product_scorecard__stage;
CREATE TABLE mart_product_scorecard__stage AS mart_product_scorecard;

INSERT INTO mart_product_scorecard__stage (
    product_id,
    title,
    made_in,
    warranty,
    n_versions,
    category_key,
    category_path,
    category_level_1,
    category_level_2,
    leaf_category,
    brand_key,
    brand_name,
    brand_is_unknown,
    seller_key,
    seller_name,
    seller_is_official,
    seller_is_unknown,
    first_seen_date,
    last_seen_date,
    days_observed,
    current_sell_price,
    current_list_price,
    current_discount_pct,
    price_band,
    min_sell_price,
    max_sell_price,
    avg_sell_price,
    units_sold_total,
    revenue_total,
    days_sales_known,
    avg_daily_units,
    units_last_7d,
    units_prev_7d,
    growth_7d_pct,
    sales_trend,
    sold_cum_latest,
    rating_latest,
    rating_cnt_latest,
    new_ratings_total,
    price_changes,
    price_up,
    price_down,
    inflated_events,
    last_price_change_at,
    n_reviews,
    n_rated_reviews,
    star_sum,
    star_1,
    star_2,
    star_3,
    star_4,
    star_5,
    n_positive,
    n_negative,
    n_with_image,
    word_sum,
    avg_star,
    negative_pct,
    revenue_rank,
    revenue_cum_share_pct,
    abc_class,
    as_of_date
)
WITH
    (SELECT max(full_date) FROM mart_kpi_daily WHERE is_reliable_day) AS as_of,
    daily AS (
        SELECT
            product_id AS p_product_id,
            min(toDate(first_observed_at)) AS p_first_seen,
            max(toDate(first_observed_at)) AS p_last_seen,
            toUInt16(count()) AS p_days_observed,
            argMax(category_key, date_key) AS p_category_key,
            argMax(brand_key, date_key) AS p_brand_key,
            argMax(seller_key, date_key) AS p_seller_key,
            -- giá "hiện tại" = ngày cuối cùng có giá > 0 (0 = nguồn thiếu giá)
            argMaxIf(close_sell_price, date_key, close_sell_price > 0) AS p_cur_sell,
            argMaxIf(close_list_price, date_key, close_sell_price > 0) AS p_cur_list,
            argMaxIf(avg_discount_pct, date_key, close_sell_price > 0) AS p_cur_discount,
            minIf(min_sell_price, min_sell_price > 0) AS p_min_sell,
            max(max_sell_price) AS p_max_sell,
            avgIf(toFloat64(close_sell_price), close_sell_price > 0) AS p_avg_sell,
            toUInt64(ifNull(sum(units_sold_est), 0)) AS p_units,
            toUInt64(ifNull(sum(revenue_est), 0)) AS p_revenue,
            toUInt16(countIf(units_sold_est IS NOT NULL)) AS p_days_known,
            toUInt64(ifNull(sumIf(units_sold_est, toDate(first_observed_at) BETWEEN as_of - 6 AND as_of), 0)) AS p_units_7d,
            toUInt64(ifNull(sumIf(units_sold_est, toDate(first_observed_at) BETWEEN as_of - 13 AND as_of - 7), 0)) AS p_units_prev_7d,
            argMax(sold_eod, date_key) AS p_sold_cum,
            argMax(rating_eod, date_key) AS p_rating,
            argMax(rating_cnt_eod, date_key) AS p_rating_cnt,
            toUInt64(ifNull(sum(new_ratings), 0)) AS p_new_ratings
        FROM gold_fact_product_daily FINAL
        GROUP BY product_id
    ),
    pc AS (
        SELECT
            product_id AS c_product_id,
            toUInt32(count()) AS c_changes,
            toUInt32(countIf(direction = 'up')) AS c_up,
            toUInt32(countIf(direction = 'down')) AS c_down,
            toUInt32(countIf(is_list_price_inflated)) AS c_inflated,
            max(changed_at) AS c_last_change
        FROM gold_fact_price_change FINAL
        WHERE old_sell_price > 0 AND new_sell_price > 0
        GROUP BY product_id
    ),
    rv AS (
        SELECT
            product_id AS r_product_id,
            toUInt32(count()) AS r_reviews,
            toUInt32(countIf(star_score > 0)) AS r_rated,
            toUInt64(sum(star_score)) AS r_star_sum,
            toUInt32(countIf(star_score = 1)) AS r_star_1,
            toUInt32(countIf(star_score = 2)) AS r_star_2,
            toUInt32(countIf(star_score = 3)) AS r_star_3,
            toUInt32(countIf(star_score = 4)) AS r_star_4,
            toUInt32(countIf(star_score = 5)) AS r_star_5,
            toUInt32(countIf(is_positive)) AS r_positive,
            toUInt32(countIf(is_negative)) AS r_negative,
            toUInt32(countIf(has_image)) AS r_with_image,
            toUInt64(sum(word_count)) AS r_word_sum
        FROM gold_fact_review FINAL
        GROUP BY product_id
    ),
    prod AS (
        -- thuộc tính version hiện tại (SCD2)
        SELECT
            product_id AS v_product_id,
            argMax(title, version_no) AS v_title,
            argMax(made_in, version_no) AS v_made_in,
            argMax(warranty, version_no) AS v_warranty,
            toUInt16(count()) AS v_n_versions
        FROM gold_dim_product FINAL
        GROUP BY product_id
    ),
    base AS (
        SELECT
            d.*,
            pc.c_changes, pc.c_up, pc.c_down, pc.c_inflated,
            if(pc.c_product_id = 0, NULL, pc.c_last_change) AS c_last_change_n,
            rv.r_reviews, rv.r_rated, rv.r_star_sum,
            rv.r_star_1, rv.r_star_2, rv.r_star_3, rv.r_star_4, rv.r_star_5,
            rv.r_positive, rv.r_negative, rv.r_with_image, rv.r_word_sum,
            v.v_title, v.v_made_in, v.v_warranty, v.v_n_versions
        FROM daily AS d
        LEFT JOIN pc ON pc.c_product_id = d.p_product_id
        LEFT JOIN rv ON rv.r_product_id = d.p_product_id
        LEFT JOIN prod AS v ON v.v_product_id = d.p_product_id
    ),
    ranked AS (
        SELECT
            *,
            row_number() OVER w_rank AS rk_rank,
            sum(p_revenue) OVER w_rank AS rk_cum_revenue,
            sum(p_revenue) OVER () AS rk_total_revenue
        FROM base
        WINDOW w_rank AS (ORDER BY p_revenue DESC, p_product_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    )
SELECT
    r.p_product_id,
    r.v_title,
    r.v_made_in,
    r.v_warranty,
    r.v_n_versions,
    r.p_category_key,
    if(c.category_path = '', 'unknown', c.category_path),
    if(c.category_level_1 = '', 'unknown', c.category_level_1),
    c.category_level_2,
    if(c.leaf_category = '', 'unknown', c.leaf_category),
    r.p_brand_key,
    if(b.brand_name = '', 'Không rõ thương hiệu', b.brand_name),
    b.is_unknown OR b.brand_key = '',
    r.p_seller_key,
    if(s.seller_name = '', 'Không rõ người bán', s.seller_name),
    s.is_official,
    s.is_unknown OR s.seller_key = '',
    r.p_first_seen,
    r.p_last_seen,
    r.p_days_observed,
    r.p_cur_sell,
    r.p_cur_list,
    r.p_cur_discount,
    mart_price_band(r.p_cur_sell),
    r.p_min_sell,
    r.p_max_sell,
    if(isNaN(r.p_avg_sell), 0, r.p_avg_sell),
    r.p_units,
    r.p_revenue,
    r.p_days_known,
    if(r.p_days_known = 0, 0, r.p_units / r.p_days_known),
    r.p_units_7d,
    r.p_units_prev_7d,
    if(r.p_units_prev_7d = 0, NULL,
       toFloat32((toInt64(r.p_units_7d) - toInt64(r.p_units_prev_7d)) / r.p_units_prev_7d * 100)) AS g_growth,
    multiIf(
        r.p_units_7d = 0 AND r.p_units_prev_7d = 0, 'không bán',
        r.p_units_prev_7d = 0, 'mới có doanh số',
        g_growth >= 20, 'tăng',
        g_growth <= -20, 'giảm',
        'ổn định'),
    r.p_sold_cum,
    r.p_rating,
    r.p_rating_cnt,
    r.p_new_ratings,
    r.c_changes,
    r.c_up,
    r.c_down,
    r.c_inflated,
    r.c_last_change_n,
    r.r_reviews,
    r.r_rated,
    r.r_star_sum,
    r.r_star_1,
    r.r_star_2,
    r.r_star_3,
    r.r_star_4,
    r.r_star_5,
    r.r_positive,
    r.r_negative,
    r.r_with_image,
    r.r_word_sum,
    if(r.r_rated = 0, NULL, toFloat32(r.r_star_sum / r.r_rated)),
    if(r.r_rated = 0, NULL, toFloat32(r.r_negative / r.r_rated * 100)),
    toUInt32(r.rk_rank),
    toFloat32(if(r.rk_total_revenue = 0, 0, r.rk_cum_revenue / r.rk_total_revenue * 100)),
    -- ABC (Pareto): xét phần doanh thu lũy kế TRƯỚC sản phẩm này, nên sản phẩm "vượt ngưỡng" 80% vẫn thuộc A
    multiIf(
        r.p_revenue = 0, 'N',
        (r.rk_cum_revenue - r.p_revenue) / r.rk_total_revenue * 100 < 80, 'A',
        (r.rk_cum_revenue - r.p_revenue) / r.rk_total_revenue * 100 < 95, 'B',
        'C'),
    as_of
FROM ranked AS r
LEFT JOIN gold_dim_category AS c FINAL ON c.category_key = r.p_category_key
LEFT JOIN gold_dim_brand AS b FINAL ON b.brand_key = r.p_brand_key
LEFT JOIN gold_dim_seller AS s FINAL ON s.seller_key = r.p_seller_key;

ALTER TABLE mart_product_scorecard REPLACE PARTITION tuple() FROM mart_product_scorecard__stage;
DROP TABLE mart_product_scorecard__stage;
