-- M1. 1 dòng / ngày. Chạy ĐẦU TIÊN: các mart sau lấy full_date, sale_event, is_reliable_day từ bảng này.
-- Nguồn: F2 (bán, giá, rating), F3 (đổi giá), gold_dim_date.
-- Nạp kiểu stage + REPLACE PARTITION: tính lại toàn bộ, chạy lại N lần vẫn ra cùng kết quả.
DROP TABLE IF EXISTS mart_kpi_daily__stage;
CREATE TABLE mart_kpi_daily__stage AS mart_kpi_daily;

INSERT INTO mart_kpi_daily__stage (
    full_date,
    date_key,
    day_of_week,
    day_name,
    is_weekend,
    sale_event,
    holiday_name,
    products_observed,
    products_sales_known,
    products_selling,
    sales_coverage_pct,
    is_reliable_day,
    active_brands,
    active_sellers,
    units_sold_est,
    revenue_est,
    priced_products,
    sell_price_sum,
    discount_pct_sum,
    discounted_products,
    deep_discount_products,
    rated_products,
    rating_sum,
    new_ratings,
    price_changes,
    price_up,
    price_down,
    inflated_events
)
WITH
    lower(hex(SHA256('unknown'))) AS unknown_key,
    f2 AS (
        SELECT
            date_key AS k_date_key,
            toUInt32(count()) AS k_products_observed,
            toUInt32(countIf(units_sold_est IS NOT NULL)) AS k_products_sales_known,
            toUInt32(countIf(units_sold_est > 0)) AS k_products_selling,
            toUInt32(uniqExactIf(brand_key, brand_key != unknown_key)) AS k_active_brands,
            toUInt32(uniqExactIf(seller_key, seller_key != unknown_key)) AS k_active_sellers,
            toUInt64(ifNull(sum(units_sold_est), 0)) AS k_units,
            toUInt64(ifNull(sum(revenue_est), 0)) AS k_revenue,
            toUInt32(countIf(close_sell_price > 0)) AS k_priced,
            toFloat64(sumIf(close_sell_price, close_sell_price > 0)) AS k_price_sum,
            sum(toFloat64(avg_discount_pct)) AS k_discount_sum,
            toUInt32(countIf(avg_discount_pct > 0)) AS k_discounted,
            toUInt32(countIf(max_discount_pct >= 30)) AS k_deep_discount,
            toUInt32(countIf(rating_cnt_eod > 0)) AS k_rated,
            sumIf(toFloat64(rating_eod), rating_cnt_eod > 0) AS k_rating_sum,
            toUInt64(ifNull(sum(new_ratings), 0)) AS k_new_ratings
        FROM gold_fact_product_daily FINAL
        GROUP BY date_key
    ),
    f3 AS (
        -- bỏ "đổi giá giả" do nguồn thiếu giá: giá -> 0 rồi 0 -> giá (GOLD_LAYER.md mục 13.3)
        SELECT
            date_key AS c_date_key,
            toUInt32(count()) AS c_changes,
            toUInt32(countIf(direction = 'up')) AS c_up,
            toUInt32(countIf(direction = 'down')) AS c_down,
            toUInt32(countIf(is_list_price_inflated)) AS c_inflated
        FROM gold_fact_price_change FINAL
        WHERE old_sell_price > 0 AND new_sell_price > 0
        GROUP BY date_key
    ),
    joined AS (
        SELECT
            f2.*,
            d.full_date AS j_full_date,
            d.day_of_week AS j_day_of_week,
            d.day_name AS j_day_name,
            d.is_weekend AS j_is_weekend,
            d.sale_event AS j_sale_event,
            d.holiday_name AS j_holiday_name,
            toFloat32(f2.k_products_sales_known / f2.k_products_observed * 100) AS j_coverage_pct,
            max(f2.k_products_observed) OVER () AS j_max_products,
            f3.c_changes,
            f3.c_up,
            f3.c_down,
            f3.c_inflated
        FROM f2
        INNER JOIN gold_dim_date AS d FINAL ON d.date_key = f2.k_date_key
        LEFT JOIN f3 ON f3.c_date_key = f2.k_date_key
    )
SELECT
    j_full_date,
    k_date_key,
    j_day_of_week,
    j_day_name,
    j_is_weekend,
    j_sale_event,
    j_holiday_name,
    k_products_observed,
    k_products_sales_known,
    k_products_selling,
    j_coverage_pct,
    -- ngày đầu/cuối kỳ chỉ crawl được vài trăm sản phẩm ; ngày đầu tiên của đa số sản phẩm chưa có
    -- mốc để tính units_sold_est -> cả hai loại ngày đều làm số theo ngày thấp giả tạo
    k_products_observed >= 0.9 * j_max_products AND j_coverage_pct >= 90 AS is_reliable_day,
    k_active_brands,
    k_active_sellers,
    k_units,
    k_revenue,
    k_priced,
    k_price_sum,
    k_discount_sum,
    k_discounted,
    k_deep_discount,
    k_rated,
    k_rating_sum,
    k_new_ratings,
    c_changes,
    c_up,
    c_down,
    c_inflated
FROM joined;

ALTER TABLE mart_kpi_daily REPLACE PARTITION tuple() FROM mart_kpi_daily__stage;
DROP TABLE mart_kpi_daily__stage;
