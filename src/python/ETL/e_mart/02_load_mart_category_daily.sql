-- M2. 1 dòng / ngày / category (leaf). Chạy sau 01.
-- Nguồn: F2 (bán, giá, rating), F3 (đổi giá), gold_dim_category, mart_kpi_daily (lịch + is_reliable_day).
-- Sản phẩm được gán cho category CUỐI NGÀY (category_key của F2).
DROP TABLE IF EXISTS mart_category_daily__stage;
CREATE TABLE mart_category_daily__stage AS mart_category_daily;

INSERT INTO mart_category_daily__stage (
    full_date,
    date_key,
    sale_event,
    is_weekend,
    is_reliable_day,
    category_key,
    category_path,
    category_level_1,
    category_level_2,
    category_level_3,
    leaf_category,
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
    active_brands,
    active_sellers
)
WITH
    lower(hex(SHA256('unknown'))) AS unknown_key,
    f2 AS (
        SELECT
            date_key AS k_date_key,
            category_key AS k_category_key,
            toUInt32(count()) AS k_products_observed,
            toUInt32(countIf(units_sold_est IS NOT NULL)) AS k_products_sales_known,
            toUInt32(countIf(units_sold_est > 0)) AS k_products_selling,
            toUInt64(ifNull(sum(units_sold_est), 0)) AS k_units,
            toUInt64(ifNull(sum(revenue_est), 0)) AS k_revenue,
            toUInt32(countIf(close_sell_price > 0)) AS k_priced,
            toFloat64(sumIf(close_sell_price, close_sell_price > 0)) AS k_price_sum,
            sum(toFloat64(avg_discount_pct)) AS k_discount_sum,
            toUInt32(countIf(avg_discount_pct > 0)) AS k_discounted,
            toUInt32(countIf(max_discount_pct >= 30)) AS k_deep_discount,
            toUInt32(countIf(rating_cnt_eod > 0)) AS k_rated,
            sumIf(toFloat64(rating_eod), rating_cnt_eod > 0) AS k_rating_sum,
            toUInt64(ifNull(sum(new_ratings), 0)) AS k_new_ratings,
            toUInt32(uniqExactIf(brand_key, brand_key != unknown_key)) AS k_active_brands,
            toUInt32(uniqExactIf(seller_key, seller_key != unknown_key)) AS k_active_sellers
        FROM gold_fact_product_daily FINAL
        GROUP BY date_key, category_key
    ),
    f3 AS (
        SELECT
            date_key AS c_date_key,
            category_key AS c_category_key,
            toUInt32(count()) AS c_changes,
            toUInt32(countIf(direction = 'up')) AS c_up,
            toUInt32(countIf(direction = 'down')) AS c_down,
            toUInt32(countIf(is_list_price_inflated)) AS c_inflated
        FROM gold_fact_price_change FINAL
        WHERE old_sell_price > 0 AND new_sell_price > 0
        GROUP BY date_key, category_key
    )
SELECT
    kd.full_date,
    f2.k_date_key,
    kd.sale_event,
    kd.is_weekend,
    kd.is_reliable_day,
    f2.k_category_key,
    if(c.category_path = '', 'unknown', c.category_path),
    if(c.category_level_1 = '', 'unknown', c.category_level_1),
    c.category_level_2,
    c.category_level_3,
    if(c.leaf_category = '', 'unknown', c.leaf_category),
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
    f2.k_active_brands,
    f2.k_active_sellers
FROM f2
INNER JOIN mart_kpi_daily AS kd ON kd.date_key = f2.k_date_key
LEFT JOIN gold_dim_category AS c FINAL ON c.category_key = f2.k_category_key
LEFT JOIN f3 ON f3.c_date_key = f2.k_date_key AND f3.c_category_key = f2.k_category_key;

ALTER TABLE mart_category_daily REPLACE PARTITION tuple() FROM mart_category_daily__stage;
DROP TABLE mart_category_daily__stage;
