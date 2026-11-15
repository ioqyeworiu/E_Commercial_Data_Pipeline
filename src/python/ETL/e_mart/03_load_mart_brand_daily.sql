-- M3. 1 dòng / ngày / brand / category cấp 1. Chạy sau 01.
-- Nguồn: F2, F3, gold_dim_brand, gold_dim_category, mart_kpi_daily.
-- brand_key của gold đã gộp các cách viết khác nhau (entity resolution theo brand_normalized).
-- Sản phẩm được gán cho brand và category CUỐI NGÀY (khóa của F2).
DROP TABLE IF EXISTS mart_brand_daily__stage;
CREATE TABLE mart_brand_daily__stage AS mart_brand_daily;

INSERT INTO mart_brand_daily__stage (
    full_date,
    date_key,
    sale_event,
    is_reliable_day,
    brand_key,
    brand_name,
    brand_origin,
    brand_is_unknown,
    category_level_1,
    products_observed,
    products_sales_known,
    products_selling,
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
    inflated_events,
    active_sellers
)
WITH
    lower(hex(SHA256('unknown'))) AS unknown_key,
    f2 AS (
        SELECT
            f.date_key AS k_date_key,
            f.brand_key AS k_brand_key,
            if(c.category_level_1 = '', 'unknown', c.category_level_1) AS k_category_level_1,
            toUInt32(count()) AS k_products_observed,
            toUInt32(countIf(f.units_sold_est IS NOT NULL)) AS k_products_sales_known,
            toUInt32(countIf(f.units_sold_est > 0)) AS k_products_selling,
            toUInt64(ifNull(sum(f.units_sold_est), 0)) AS k_units,
            toUInt64(ifNull(sum(f.revenue_est), 0)) AS k_revenue,
            toUInt32(countIf(f.close_sell_price > 0)) AS k_priced,
            toFloat64(sumIf(f.close_sell_price, f.close_sell_price > 0)) AS k_price_sum,
            sum(toFloat64(f.avg_discount_pct)) AS k_discount_sum,
            toUInt32(countIf(f.avg_discount_pct > 0)) AS k_discounted,
            toUInt32(countIf(f.max_discount_pct >= 30)) AS k_deep_discount,
            toUInt32(countIf(f.rating_cnt_eod > 0)) AS k_rated,
            sumIf(toFloat64(f.rating_eod), f.rating_cnt_eod > 0) AS k_rating_sum,
            toUInt64(ifNull(sum(f.new_ratings), 0)) AS k_new_ratings,
            toUInt32(uniqExactIf(f.seller_key, f.seller_key != unknown_key)) AS k_active_sellers
        FROM gold_fact_product_daily AS f FINAL
        LEFT JOIN gold_dim_category AS c FINAL ON c.category_key = f.category_key
        GROUP BY k_date_key, k_brand_key, k_category_level_1
    ),
    f3 AS (
        SELECT
            p.date_key AS c_date_key,
            p.brand_key AS c_brand_key,
            if(c.category_level_1 = '', 'unknown', c.category_level_1) AS c_category_level_1,
            toUInt32(count()) AS c_changes,
            toUInt32(countIf(p.direction = 'up')) AS c_up,
            toUInt32(countIf(p.direction = 'down')) AS c_down,
            toUInt32(countIf(p.is_list_price_inflated)) AS c_inflated
        FROM gold_fact_price_change AS p FINAL
        LEFT JOIN gold_dim_category AS c FINAL ON c.category_key = p.category_key
        WHERE p.old_sell_price > 0 AND p.new_sell_price > 0
        GROUP BY c_date_key, c_brand_key, c_category_level_1
    )
SELECT
    kd.full_date,
    f2.k_date_key,
    kd.sale_event,
    kd.is_reliable_day,
    f2.k_brand_key,
    if(b.brand_name = '', 'Không rõ thương hiệu', b.brand_name),
    b.brand_origin,
    b.is_unknown OR b.brand_key = '',
    f2.k_category_level_1,
    f2.k_products_observed,
    f2.k_products_sales_known,
    f2.k_products_selling,
    f2.k_units,
    f2.k_revenue,
    f2.k_priced,
    f2.k_price_sum,
    f2.k_discount_sum,
    f2.k_discounted,
    f2.k_deep_discount,
    f2.k_rated,
    f2.k_rating_sum,
    f2.k_new_ratings,
    f3.c_changes,
    f3.c_up,
    f3.c_down,
    f3.c_inflated,
    f2.k_active_sellers
FROM f2
INNER JOIN mart_kpi_daily AS kd ON kd.date_key = f2.k_date_key
LEFT JOIN gold_dim_brand AS b FINAL ON b.brand_key = f2.k_brand_key
LEFT JOIN f3 ON f3.c_date_key = f2.k_date_key
            AND f3.c_brand_key = f2.k_brand_key
            AND f3.c_category_level_1 = f2.k_category_level_1;

ALTER TABLE mart_brand_daily REPLACE PARTITION tuple() FROM mart_brand_daily__stage;
DROP TABLE mart_brand_daily__stage;
