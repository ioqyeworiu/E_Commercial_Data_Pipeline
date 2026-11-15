-- =====================================================================================
-- DASHBOARD 1: TỔNG QUAN THỊ TRƯỜNG
-- Người xem: quản lý / giảng viên. Câu hỏi: thị trường lớn cỡ nào, đang tăng hay giảm,
-- danh mục nào kéo doanh thu, ngày sale có hiệu quả không.
--
-- Tham số (cú pháp query parameter của ClickHouse, xem query_dashboard/README.md):
--   {date_from:Date}, {date_to:Date}   khoảng ngày đang xem, ví dụ 2026-10-27 .. 2026-11-11
-- Mọi truy vấn chỉ lấy ngày is_reliable_day (bỏ ngày crawl chưa đủ ở đầu/cuối kỳ).
-- Doanh thu, số bán là ƯỚC TÍNH từ chênh lệch "đã bán" lũy kế (GOLD_LAYER.md mục 13).
-- =====================================================================================


-- [1.1] Thẻ KPI có so sánh kỳ trước | Biểu đồ: KPI cards (số lớn + mũi tên % thay đổi)
-- Kỳ trước = cùng số ngày, liền trước date_from. So sánh theo TRUNG BÌNH / NGÀY để kỳ trước
-- thiếu ngày (đầu dữ liệu) không làm % tăng trưởng bị phóng đại.
WITH
    {date_from:Date} AS d_from,
    {date_to:Date} AS d_to,
    dateDiff('day', d_from, d_to) + 1 AS n_days,
    d_from - n_days AS p_from,
    d_from - 1 AS p_to
SELECT
    sumIf(revenue_est, is_cur) AS revenue,
    sumIf(units_sold_est, is_cur) AS units,
    countIf(is_cur) AS days,
    countIf(is_prev) AS days_prev,
    round(revenue / nullIf(days, 0)) AS revenue_per_day,
    round((revenue_per_day / nullIf(sumIf(revenue_est, is_prev) / nullIf(days_prev, 0), 0) - 1) * 100, 1) AS revenue_per_day_growth_pct,
    round(units / nullIf(days, 0)) AS units_per_day,
    round((units_per_day / nullIf(sumIf(units_sold_est, is_prev) / nullIf(days_prev, 0), 0) - 1) * 100, 1) AS units_per_day_growth_pct,
    round(revenue / nullIf(units, 0)) AS avg_selling_price,                      -- giá bán bình quân gia quyền theo số bán
    round(avgIf(products_selling, is_cur)) AS products_selling_per_day,
    round(sumIf(discount_pct_sum, is_cur) / nullIf(sumIf(products_observed, is_cur), 0), 1) AS avg_discount_pct,
    round(sumIf(discounted_products, is_cur) / nullIf(sumIf(products_observed, is_cur), 0) * 100, 1) AS discounted_share_pct,
    round(sumIf(rating_sum, is_cur) / nullIf(sumIf(rated_products, is_cur), 0), 2) AS avg_rating,
    sumIf(new_ratings, is_cur) AS new_ratings,
    sumIf(price_changes, is_cur) AS price_changes,
    sumIf(inflated_events, is_cur) AS inflated_events,
    (SELECT uniqExact(seller_key) FROM mart_seller_daily
     WHERE is_reliable_day AND NOT seller_is_unknown AND full_date BETWEEN d_from AND d_to) AS active_sellers,
    (SELECT uniqExact(brand_key) FROM mart_brand_daily
     WHERE is_reliable_day AND NOT brand_is_unknown AND full_date BETWEEN d_from AND d_to) AS active_brands
FROM (
    SELECT
        *,
        full_date BETWEEN d_from AND d_to AS is_cur,
        full_date BETWEEN p_from AND p_to AS is_prev
    FROM mart_kpi_daily
    WHERE is_reliable_day AND full_date BETWEEN p_from AND d_to
);


-- [1.2] Xu hướng doanh thu theo ngày | Biểu đồ: combo (cột = doanh thu, đường = TB trượt 7 ngày,
-- trục phụ = % giảm TB) ; đánh dấu điểm ngày sale bằng cột sale_event
SELECT
    full_date,
    day_name,
    sale_event,
    revenue_est,
    units_sold_est,
    products_selling,
    round(avg(revenue_est) OVER (ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS revenue_ma7,
    round(discount_pct_sum / products_observed, 1) AS avg_discount_pct,
    round(revenue_est / nullIf(units_sold_est, 0)) AS avg_selling_price
FROM mart_kpi_daily
WHERE is_reliable_day AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
ORDER BY full_date;


-- [1.3] Doanh thu theo danh mục cấp 1, thị phần và tăng trưởng | Biểu đồ: bar ngang (sắp theo doanh thu),
-- nhãn = % thị phần, màu = tăng trưởng TB/ngày so với kỳ trước
WITH
    {date_from:Date} AS d_from,
    {date_to:Date} AS d_to,
    dateDiff('day', d_from, d_to) + 1 AS n_days,
    d_from - n_days AS p_from,
    d_from - 1 AS p_to,
    (SELECT countIf(full_date BETWEEN d_from AND d_to) FROM mart_kpi_daily WHERE is_reliable_day) AS days_cur,
    (SELECT countIf(full_date BETWEEN p_from AND p_to) FROM mart_kpi_daily WHERE is_reliable_day) AS days_prev
SELECT
    category_level_1,
    sumIf(revenue_est, full_date >= d_from) AS revenue,
    round(revenue / sum(revenue) OVER () * 100, 1) AS revenue_share_pct,
    sumIf(units_sold_est, full_date >= d_from) AS units,
    round(revenue / nullIf(units, 0)) AS avg_selling_price,
    round(sumIf(products_observed, full_date >= d_from) / nullIf(days_cur, 0)) AS products_per_day,
    if(days_prev = 0, NULL,
       round((revenue / days_cur) / nullIf(sumIf(revenue_est, full_date < d_from) / days_prev, 0) * 100 - 100, 1)) AS revenue_per_day_growth_pct
FROM mart_category_daily
WHERE is_reliable_day AND full_date BETWEEN p_from AND d_to
GROUP BY category_level_1
ORDER BY revenue DESC;


-- [1.4] Cơ cấu doanh thu danh mục cấp 1 > cấp 2 | Biểu đồ: treemap (hoặc sunburst)
SELECT
    category_level_1,
    if(category_level_2 = '', '(không phân cấp)', category_level_2) AS category_level_2,
    sum(revenue_est) AS revenue,
    sum(units_sold_est) AS units,
    round(sum(revenue_est) / sum(sum(revenue_est)) OVER () * 100, 2) AS revenue_share_pct
FROM mart_category_daily
WHERE is_reliable_day AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
GROUP BY category_level_1, category_level_2
HAVING revenue > 0
ORDER BY revenue DESC;


-- [1.5] Hiệu ứng ngày sale | Biểu đồ: bar nhóm (doanh thu TB/ngày và bán TB / sản phẩm theo loại ngày)
-- Tách riêng "ngày ngay sau sale": chu kỳ crawl ~4,2 giờ nên một phần số bán của ngày sale bị ghi
-- sang hôm sau. uplift_vs_normal = chỉ số / chỉ số ngày thường.
WITH
    (SELECT groupArray(full_date) FROM mart_kpi_daily WHERE sale_event != '') AS sale_dates,
    typed AS (
        SELECT
            *,
            multiIf(sale_event != '', concat('Ngày sale ', sale_event),
                    has(sale_dates, full_date - 1), 'Ngày sau sale',
                    'Ngày thường') AS day_type
        FROM mart_kpi_daily
        WHERE is_reliable_day AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
    ),
    agg AS (
        SELECT
            day_type,
            count() AS days,
            round(avg(revenue_est)) AS revenue_per_day,
            round(sum(units_sold_est) / sum(products_sales_known), 2) AS units_per_product_day,
            round(sum(products_selling) / sum(products_sales_known) * 100, 1) AS selling_products_pct,
            round(sum(discount_pct_sum) / sum(products_observed), 1) AS avg_discount_pct,
            round(sum(deep_discount_products) / sum(products_observed) * 100, 1) AS deep_discount_share_pct
        FROM typed
        GROUP BY day_type
    )
SELECT
    *,
    round(revenue_per_day / nullIf(anyIf(revenue_per_day, day_type = 'Ngày thường') OVER (), 0), 2) AS revenue_uplift_vs_normal,
    round(units_per_product_day / nullIf(anyIf(units_per_product_day, day_type = 'Ngày thường') OVER (), 0), 2) AS units_uplift_vs_normal
FROM agg
ORDER BY revenue_per_day DESC;


-- [1.6] Mùa vụ theo thứ trong tuần (bỏ ngày sale và ngày ngay sau sale) | Biểu đồ: column chart
WITH (SELECT groupArray(full_date) FROM mart_kpi_daily WHERE sale_event != '') AS sale_dates
SELECT
    day_of_week,
    any(day_name) AS day_name,
    count() AS days,
    round(avg(revenue_est)) AS revenue_per_day,
    round(avg(units_sold_est)) AS units_per_day,
    round(avg(revenue_est) / avg(avg(revenue_est)) OVER () * 100 - 100, 1) AS vs_week_avg_pct
FROM mart_kpi_daily
WHERE is_reliable_day
  AND sale_event = '' AND NOT has(sale_dates, full_date - 1)
  AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
GROUP BY day_of_week
ORDER BY day_of_week;
