-- =====================================================================================
-- Kiểm tra lớp data mart (chạy sau e_mart/01..08). Được run_test_pipeline.sh gọi ở bước "check".
--   docker exec -i clickhouse_server clickhouse-client --user=clickhouse --password=<pw> \
--       -d ecommercial_warehouse --multiquery < src/sql_script/clickhouse/test_data/check_mart.sql
-- Mỗi kiểm tra in cột ok = 1 nếu đạt.
-- =====================================================================================

SELECT 'M1. SỐ DÒNG VÀ DUNG LƯỢNG CÁC MART';
SELECT name, total_rows, formatReadableSize(total_bytes) AS size
FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'mart\_%' AND name NOT LIKE '%\_\_stage'
ORDER BY name
FORMAT PrettyCompactMonoBlock;

SELECT 'M2. ĐỐI CHIẾU VỚI GOLD: doanh thu, số bán, sản phẩm-ngày của mọi mart theo ngày = F2 (tính cả ngày không tin cậy)';
WITH
    (SELECT sum(revenue_est) FROM gold_fact_product_daily FINAL) AS g_revenue,
    (SELECT sum(units_sold_est) FROM gold_fact_product_daily FINAL) AS g_units,
    (SELECT count() FROM gold_fact_product_daily FINAL) AS g_rows
SELECT mart, revenue, units, product_days,
       revenue = g_revenue AND units = g_units AND product_days = g_rows AS ok
FROM (
    SELECT 'mart_kpi_daily' AS mart, sum(revenue_est) AS revenue, sum(units_sold_est) AS units, sum(products_observed) AS product_days FROM mart_kpi_daily
    UNION ALL SELECT 'mart_category_daily', sum(revenue_est), sum(units_sold_est), sum(products_observed) FROM mart_category_daily
    UNION ALL SELECT 'mart_brand_daily', sum(revenue_est), sum(units_sold_est), sum(products_observed) FROM mart_brand_daily
    UNION ALL SELECT 'mart_seller_daily', sum(revenue_est), sum(units_sold_est), sum(products_observed) FROM mart_seller_daily
    UNION ALL SELECT 'mart_price_discount_daily', sum(revenue_est), sum(units_sold_est), sum(product_days) FROM mart_price_discount_daily
    UNION ALL SELECT 'mart_product_scorecard', sum(revenue_total), sum(units_sold_total), sum(days_observed) FROM mart_product_scorecard
)
ORDER BY mart
FORMAT PrettyCompactMonoBlock;

SELECT 'M3. ĐỐI CHIẾU ĐỔI GIÁ (F3 đã bỏ giá 0) VÀ REVIEW (F4)';
SELECT
    (SELECT count() FROM gold_fact_price_change FINAL WHERE old_sell_price > 0 AND new_sell_price > 0) AS f3_changes,
    (SELECT sum(price_changes) FROM mart_kpi_daily) AS kpi_changes,
    (SELECT sum(price_changes) FROM mart_category_daily) AS category_changes,
    (SELECT sum(price_changes) FROM mart_brand_daily) AS brand_changes,
    (SELECT sum(price_changes) FROM mart_seller_daily) AS seller_changes,
    (SELECT sum(price_changes) FROM mart_product_scorecard) AS product_changes,
    (SELECT count() FROM gold_fact_review FINAL) AS f4_reviews,
    (SELECT sum(n_reviews) FROM mart_review_monthly) AS review_mart_reviews,
    (SELECT sum(n_reviews) FROM mart_product_scorecard) AS product_mart_reviews,   -- thiếu review chưa gắn được sản phẩm
    (SELECT countIf(product_key = '') FROM gold_fact_review FINAL) AS f4_reviews_without_product,
    f3_changes = kpi_changes AND kpi_changes = category_changes AND kpi_changes = brand_changes
        AND kpi_changes = seller_changes AND kpi_changes = product_changes
        AND f4_reviews = review_mart_reviews
        AND product_mart_reviews + f4_reviews_without_product = f4_reviews AS ok
FORMAT Vertical;

SELECT 'M4. GRAIN KHÔNG TRÙNG (số dòng = số khóa grain khác nhau)';
SELECT mart, n_rows, n_keys, n_rows = n_keys AS ok
FROM (
    SELECT 'mart_kpi_daily' AS mart, count() AS n_rows, uniqExact(full_date) AS n_keys FROM mart_kpi_daily
    UNION ALL SELECT 'mart_category_daily', count(), uniqExact(full_date, category_key) FROM mart_category_daily
    UNION ALL SELECT 'mart_brand_daily', count(), uniqExact(full_date, brand_key, category_level_1) FROM mart_brand_daily
    UNION ALL SELECT 'mart_seller_daily', count(), uniqExact(full_date, seller_key) FROM mart_seller_daily
    UNION ALL SELECT 'mart_price_discount_daily', count(), uniqExact(full_date, category_level_1, discount_band, price_band) FROM mart_price_discount_daily
    UNION ALL SELECT 'mart_product_scorecard', count(), uniqExact(product_id) FROM mart_product_scorecard
    UNION ALL SELECT 'mart_sale_event_product', count(), uniqExact(event_date, product_id) FROM mart_sale_event_product
    UNION ALL SELECT 'mart_review_monthly', count(), uniqExact(review_month, category_key, tenure_band) FROM mart_review_monthly
)
ORDER BY mart
FORMAT PrettyCompactMonoBlock;

SELECT 'M5. NGÀY TIN CẬY (is_reliable_day): các ngày bị loại và lý do';
SELECT full_date, products_observed, sales_coverage_pct, is_reliable_day
FROM mart_kpi_daily
WHERE NOT is_reliable_day
ORDER BY full_date
FORMAT PrettyCompactMonoBlock;

SELECT 'M6. ABC: nhóm A tạo ~80% doanh thu, A+B ~95%';
SELECT abc_class, count() AS products, round(sum(revenue_total) / (SELECT sum(revenue_total) FROM mart_product_scorecard) * 100, 1) AS revenue_pct
FROM mart_product_scorecard
GROUP BY abc_class
ORDER BY abc_class
FORMAT PrettyCompactMonoBlock;

SELECT 'M7. GIẢM GIÁ ẢO 11.11: discount_verdict của mart so với "đáp án" của bộ sinh (chỉ dữ liệu test) và với cờ F3 của gold';
SELECT
    countIf(is_truth) AS truly_fake,
    countIf(mart_flag) AS mart_detected,
    round(countIf(is_truth AND mart_flag) / greatest(countIf(mart_flag), 1), 3) AS mart_precision,
    round(countIf(is_truth AND mart_flag) / greatest(countIf(is_truth), 1), 3) AS mart_recall,
    countIf(gold_flag) AS gold_f3_detected,
    round(countIf(is_truth AND gold_flag) / greatest(countIf(gold_flag), 1), 3) AS gold_f3_precision,
    round(countIf(is_truth AND gold_flag) / greatest(countIf(is_truth), 1), 3) AS gold_f3_recall
FROM (
    SELECT
        product_id,
        cityHash64(product_id - 1000000, 'fake') % 100 < 10 AS is_truth,
        discount_verdict = 'giảm giá ảo' AS mart_flag,
        product_id IN (SELECT product_id FROM gold_fact_price_change FINAL
                       WHERE is_list_price_inflated AND changed_at BETWEEN '2026-11-01 00:00:00' AND '2026-11-11 23:59:59') AS gold_flag
    FROM mart_sale_event_product
    WHERE event_date = '2026-11-11' AND product_id >= 1000000
)
FORMAT PrettyCompactMonoBlock;
