-- M5. 1 dòng / ngày / category cấp 1 / mức giảm giá / phân khúc giá. Chạy sau 01.
-- Nguồn: F2, gold_dim_category, mart_kpi_daily. Phân khúc dùng hàm mart_price_band / mart_discount_band (mart_ddl.sql).
-- Mỗi sản phẩm-ngày rơi vào đúng 1 ô (discount_band theo avg_discount_pct, price_band theo close_sell_price).
DROP TABLE IF EXISTS mart_price_discount_daily__stage;
CREATE TABLE mart_price_discount_daily__stage AS mart_price_discount_daily;

INSERT INTO mart_price_discount_daily__stage (
    full_date,
    date_key,
    sale_event,
    is_reliable_day,
    category_level_1,
    discount_band,
    price_band,
    product_days,
    product_days_sales_known,
    products_selling,
    units_sold_est,
    revenue_est,
    discount_pct_sum
)
WITH
    f2 AS (
        SELECT
            f.date_key AS k_date_key,
            if(c.category_level_1 = '', 'unknown', c.category_level_1) AS k_category_level_1,
            mart_discount_band(f.avg_discount_pct) AS k_discount_band,
            mart_price_band(f.close_sell_price) AS k_price_band,
            toUInt32(count()) AS k_product_days,
            toUInt32(countIf(f.units_sold_est IS NOT NULL)) AS k_sales_known,
            toUInt32(countIf(f.units_sold_est > 0)) AS k_selling,
            toUInt64(ifNull(sum(f.units_sold_est), 0)) AS k_units,
            toUInt64(ifNull(sum(f.revenue_est), 0)) AS k_revenue,
            sum(toFloat64(f.avg_discount_pct)) AS k_discount_sum
        FROM gold_fact_product_daily AS f FINAL
        LEFT JOIN gold_dim_category AS c FINAL ON c.category_key = f.category_key
        GROUP BY k_date_key, k_category_level_1, k_discount_band, k_price_band
    )
SELECT
    kd.full_date,
    f2.k_date_key,
    kd.sale_event,
    kd.is_reliable_day,
    f2.k_category_level_1,
    f2.k_discount_band,
    f2.k_price_band,
    f2.k_product_days,
    f2.k_sales_known,
    f2.k_selling,
    f2.k_units,
    f2.k_revenue,
    f2.k_discount_sum
FROM f2
INNER JOIN mart_kpi_daily AS kd ON kd.date_key = f2.k_date_key;

ALTER TABLE mart_price_discount_daily REPLACE PARTITION tuple() FROM mart_price_discount_daily__stage;
DROP TABLE mart_price_discount_daily__stage;
