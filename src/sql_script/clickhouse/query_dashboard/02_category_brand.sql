-- =====================================================================================
-- DASHBOARD 2: DANH MỤC & THƯƠNG HIỆU
-- Câu hỏi: danh mục nào lớn / tăng nhanh, thị trường trong từng danh mục có bị vài brand chi phối không,
-- phân khúc giá nào bán chạy, bao nhiêu sản phẩm tạo ra phần lớn doanh thu.
--
-- Tham số: {date_from:Date}, {date_to:Date}, {category_l1:String} ('' = mọi danh mục ; nhập tên
-- danh mục cấp 1 để drill-down xuống cấp 2).
-- =====================================================================================


-- [2.1] Bảng hiệu suất danh mục (drill-down cấp 1 -> cấp 2) | Biểu đồ: table có thanh dữ liệu (data bar)
-- Để category_l1 = '' thì mỗi dòng là một danh mục cấp 1 ; chọn một danh mục thì ra các danh mục cấp 2 của nó.
WITH
    {date_from:Date} AS d_from,
    {date_to:Date} AS d_to,
    dateDiff('day', d_from, d_to) + 1 AS n_days,
    d_from - n_days AS p_from,
    d_from - 1 AS p_to,
    (SELECT countIf(full_date BETWEEN d_from AND d_to) FROM mart_kpi_daily WHERE is_reliable_day) AS days_cur,
    (SELECT countIf(full_date BETWEEN p_from AND p_to) FROM mart_kpi_daily WHERE is_reliable_day) AS days_prev
SELECT
    if({category_l1:String} = '', category_level_1,
       if(category_level_2 = '', '(không phân cấp)', category_level_2)) AS category,
    sumIf(revenue_est, full_date >= d_from) AS revenue,
    round(revenue / sum(revenue) OVER () * 100, 1) AS revenue_share_pct,
    if(days_prev = 0, NULL,
       round((revenue / days_cur) / nullIf(sumIf(revenue_est, full_date < d_from) / days_prev, 0) * 100 - 100, 1)) AS revenue_per_day_growth_pct,
    sumIf(units_sold_est, full_date >= d_from) AS units,
    round(sumIf(products_observed, full_date >= d_from) / nullIf(days_cur, 0)) AS products_per_day,
    round(sumIf(products_selling, full_date >= d_from) / nullIf(sumIf(products_sales_known, full_date >= d_from), 0) * 100, 1) AS sell_through_pct,
    round(sumIf(sell_price_sum, full_date >= d_from) / nullIf(sumIf(priced_products, full_date >= d_from), 0)) AS avg_sell_price,           -- giá niêm yết TB (không trọng số)
    round(sumIf(discount_pct_sum, full_date >= d_from) / nullIf(sumIf(products_observed, full_date >= d_from), 0), 1) AS avg_discount_pct,
    round(sumIf(rating_sum, full_date >= d_from) / nullIf(sumIf(rated_products, full_date >= d_from), 0), 2) AS avg_rating,
    sumIf(price_changes, full_date >= d_from) AS price_changes,
    sumIf(inflated_events, full_date >= d_from) AS inflated_events
FROM mart_category_daily
WHERE is_reliable_day
  AND full_date BETWEEN p_from AND d_to
  AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
GROUP BY category
ORDER BY revenue DESC;


-- [2.2] Xu hướng doanh thu theo ngày của 5 danh mục lớn nhất, còn lại gộp 'khác' | Biểu đồ: stacked area
-- (hoặc multi-line). Cùng logic drill-down với [2.1]: cấp 1, hoặc cấp 2 khi đã chọn category_l1.
WITH top_cat AS (
    SELECT if({category_l1:String} = '', category_level_1, category_level_2) AS top_category
    FROM mart_category_daily
    WHERE is_reliable_day
      AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
      AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
    GROUP BY top_category
    ORDER BY sum(revenue_est) DESC, top_category
    LIMIT 5
)
SELECT
    full_date,
    sale_event,
    if(if({category_l1:String} = '', category_level_1, category_level_2) IN (SELECT top_category FROM top_cat),
       if({category_l1:String} = '', category_level_1, category_level_2), 'khác') AS category,
    sum(revenue_est) AS revenue
FROM mart_category_daily
WHERE is_reliable_day
  AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
  AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
GROUP BY full_date, sale_event, category
ORDER BY full_date, revenue DESC;


-- [2.3] Top 15 thương hiệu theo doanh thu | Biểu đồ: bar ngang + bảng phụ (rating, % giảm, tăng trưởng)
-- Thị phần tính trong phạm vi đang lọc (toàn sàn hoặc 1 danh mục cấp 1), không tính "Không rõ thương hiệu".
WITH
    {date_from:Date} AS d_from,
    {date_to:Date} AS d_to,
    dateDiff('day', d_from, d_to) + 1 AS n_days,
    d_from - n_days AS p_from,
    d_from - 1 AS p_to,
    (SELECT countIf(full_date BETWEEN d_from AND d_to) FROM mart_kpi_daily WHERE is_reliable_day) AS days_cur,
    (SELECT countIf(full_date BETWEEN p_from AND p_to) FROM mart_kpi_daily WHERE is_reliable_day) AS days_prev
SELECT
    any(brand_name) AS brand_name,
    any(brand_origin) AS brand_origin,
    sumIf(revenue_est, full_date >= d_from) AS revenue,
    round(revenue / sum(revenue) OVER () * 100, 2) AS market_share_pct,
    sumIf(units_sold_est, full_date >= d_from) AS units,
    round(sumIf(products_observed, full_date >= d_from) / nullIf(days_cur, 0)) AS products_per_day,
    round(revenue / nullIf(units, 0)) AS avg_selling_price,
    round(sumIf(discount_pct_sum, full_date >= d_from) / nullIf(sumIf(products_observed, full_date >= d_from), 0), 1) AS avg_discount_pct,
    round(sumIf(rating_sum, full_date >= d_from) / nullIf(sumIf(rated_products, full_date >= d_from), 0), 2) AS avg_rating,
    if(days_prev = 0, NULL,
       round((revenue / days_cur) / nullIf(sumIf(revenue_est, full_date < d_from) / days_prev, 0) * 100 - 100, 1)) AS revenue_per_day_growth_pct,
    brand_key                                                                    -- để drill-through
FROM mart_brand_daily
WHERE is_reliable_day
  AND NOT brand_is_unknown
  AND full_date BETWEEN p_from AND d_to
  AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
GROUP BY brand_key
ORDER BY revenue DESC
LIMIT 15;


-- [2.4] Mức độ tập trung thị trường theo danh mục cấp 1 | Biểu đồ: bar (HHI) + bảng (top-1, top-5 share)
-- HHI = tổng (thị phần %)^2 của các brand, thang 0-10.000. Ngưỡng theo hướng dẫn sáp nhập của
-- Bộ Tư pháp Mỹ (2023): < 1.000 phân tán, 1.000-1.800 tập trung vừa, > 1.800 tập trung cao.
-- Không tính "Không rõ thương hiệu" (không phải một brand thật).
WITH brand_rev AS (
    SELECT category_level_1, brand_key, sum(revenue_est) AS revenue
    FROM mart_brand_daily
    WHERE is_reliable_day AND NOT brand_is_unknown
      AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
    GROUP BY category_level_1, brand_key
    HAVING revenue > 0
),
shares AS (
    SELECT
        category_level_1,
        brand_key,
        revenue,
        revenue / sum(revenue) OVER (PARTITION BY category_level_1) * 100 AS share_pct,
        row_number() OVER (PARTITION BY category_level_1 ORDER BY revenue DESC, brand_key) AS rk
    FROM brand_rev
)
SELECT
    category_level_1,
    count() AS brands,
    sum(revenue) AS revenue,
    round(sum(share_pct * share_pct)) AS hhi,
    multiIf(hhi < 1000, 'phân tán', hhi <= 1800, 'tập trung vừa', 'tập trung cao') AS market_structure,
    round(maxIf(share_pct, rk = 1), 1) AS top1_share_pct,
    round(sumIf(share_pct, rk <= 5), 1) AS top5_share_pct,
    round(sumIf(share_pct, rk <= 10), 1) AS top10_share_pct
FROM shares
GROUP BY category_level_1
ORDER BY revenue DESC;


-- [2.5] Ma trận thương hiệu: tăng trưởng x rating x quy mô | Biểu đồ: bubble/scatter
-- X = tăng trưởng doanh thu TB/ngày so với kỳ trước, Y = rating TB, kích thước = doanh thu.
-- Đường chia: X = 0, Y = rating TB của nhóm -> 4 góc "ngôi sao / tiềm năng / cần cải thiện / suy giảm".
WITH
    {date_from:Date} AS d_from,
    {date_to:Date} AS d_to,
    dateDiff('day', d_from, d_to) + 1 AS n_days,
    d_from - n_days AS p_from,
    d_from - 1 AS p_to,
    (SELECT countIf(full_date BETWEEN d_from AND d_to) FROM mart_kpi_daily WHERE is_reliable_day) AS days_cur,
    (SELECT countIf(full_date BETWEEN p_from AND p_to) FROM mart_kpi_daily WHERE is_reliable_day) AS days_prev,
    brands AS (
        SELECT
            brand_key,
            any(brand_name) AS brand_name,
            sumIf(revenue_est, full_date >= d_from) AS revenue,
            sumIf(revenue_est, full_date < d_from) AS revenue_prev,
            sumIf(rating_sum, full_date >= d_from) / nullIf(sumIf(rated_products, full_date >= d_from), 0) AS avg_rating
        FROM mart_brand_daily
        WHERE is_reliable_day AND NOT brand_is_unknown
          AND full_date BETWEEN p_from AND d_to
          AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
        GROUP BY brand_key
        ORDER BY revenue DESC
        LIMIT 30
    )
SELECT
    brand_name,
    revenue,
    round((revenue / days_cur) / nullIf(revenue_prev / days_prev, 0) * 100 - 100, 1) AS growth_pct,
    round(avg_rating, 2) AS avg_rating,
    multiIf(growth_pct >= 0 AND avg_rating >= avg(avg_rating) OVER (), 'ngôi sao',
            growth_pct >= 0, 'tăng nhưng rating thấp',
            avg_rating >= avg(avg_rating) OVER (), 'rating tốt nhưng chững lại',
            'cần chú ý') AS quadrant
FROM brands
WHERE days_prev > 0
ORDER BY revenue DESC;


-- [2.6] Cơ cấu phân khúc giá theo danh mục cấp 1 | Biểu đồ: 100% stacked bar (mỗi cột = 1 danh mục,
-- mỗi màu = 1 phân khúc giá). price_band có tiền tố số để sắp đúng thứ tự.
SELECT
    category_level_1,
    price_band,
    sum(revenue_est) AS revenue,
    round(sum(revenue_est) / sum(sum(revenue_est)) OVER (PARTITION BY category_level_1) * 100, 1) AS revenue_share_in_category_pct,
    sum(product_days) AS product_days
FROM mart_price_discount_daily
WHERE is_reliable_day
  AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
  AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
  AND price_band != '0. không rõ giá'
GROUP BY category_level_1, price_band
ORDER BY category_level_1, price_band;


-- [2.7] Phân loại ABC sản phẩm (Pareto) | Biểu đồ: bar kép (% sản phẩm vs % doanh thu theo nhóm A/B/C/N)
-- Tính trên TOÀN KỲ đã nạp (mart_product_scorecard), không theo date_from/date_to.
-- A = nhóm tạo 80% doanh thu đầu tiên, B = 80-95%, C = phần còn lại, N = không có doanh thu.
SELECT
    abc_class,
    count() AS products,
    round(count() / sum(count()) OVER () * 100, 1) AS products_pct,
    sum(revenue_total) AS revenue,
    round(sum(revenue_total) / sum(sum(revenue_total)) OVER () * 100, 1) AS revenue_pct,
    round(avg(current_sell_price)) AS avg_current_price,
    round(avgIf(avg_star, n_rated_reviews > 0), 2) AS avg_review_star
FROM mart_product_scorecard
WHERE {category_l1:String} = '' OR category_level_1 = {category_l1:String}
GROUP BY abc_class
ORDER BY abc_class;


-- [2.8] Đường cong Pareto | Biểu đồ: line (X = % sản phẩm theo thứ hạng doanh thu, Y = % doanh thu lũy kế)
-- Đọc: "x% sản phẩm tạo ra y% doanh thu". Tính lại thứ hạng trong phạm vi lọc danh mục.
WITH ranked AS (
    SELECT
        revenue_total,
        row_number() OVER (ORDER BY revenue_total DESC, product_id) AS rk,
        count() OVER () AS n,
        sum(revenue_total) OVER (ORDER BY revenue_total DESC, product_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_rev,
        sum(revenue_total) OVER () AS total_rev
    FROM mart_product_scorecard
    WHERE {category_l1:String} = '' OR category_level_1 = {category_l1:String}
)
SELECT
    toUInt8(ceil(rk / n * 100)) AS product_percentile,
    round(max(cum_rev) / any(total_rev) * 100, 1) AS cum_revenue_pct
FROM ranked
GROUP BY product_percentile
ORDER BY product_percentile;
