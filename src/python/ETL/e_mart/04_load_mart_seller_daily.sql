-- M4. 1 dòng / ngày / seller. Chạy sau 01.
-- Nguồn: F2 (bán, giá: cùng định nghĩa với M2, M3), F5 (rating seller), F3, gold_dim_seller, mart_kpi_daily.
-- Không lấy units/revenue/giá từ F5: F5 tính giá TB có cả lần crawl thiếu giá (sell_price = 0).
DROP TABLE IF EXISTS mart_seller_daily__stage;
CREATE TABLE mart_seller_daily__stage AS mart_seller_daily;

INSERT INTO mart_seller_daily__stage (
    full_date,
    date_key,
    sale_event,
    is_reliable_day,
    seller_key,
    seller_name,
    is_official,
    seller_is_unknown,
    main_category_level_1,
    seller_rate_eod,
    seller_rate_cnt_eod,
    new_seller_ratings,
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
    active_brands
)
WITH
    lower(hex(SHA256('unknown'))) AS unknown_key,
    f2 AS (
        SELECT
            date_key AS k_date_key,
            seller_key AS k_seller_key,
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
            toUInt32(uniqExactIf(brand_key, brand_key != unknown_key)) AS k_active_brands
        FROM gold_fact_product_daily FINAL
        GROUP BY date_key, seller_key
    ),
    f3 AS (
        SELECT
            date_key AS c_date_key,
            seller_key AS c_seller_key,
            toUInt32(count()) AS c_changes,
            toUInt32(countIf(direction = 'up')) AS c_up,
            toUInt32(countIf(direction = 'down')) AS c_down,
            toUInt32(countIf(is_list_price_inflated)) AS c_inflated
        FROM gold_fact_price_change FINAL
        WHERE old_sell_price > 0 AND new_sell_price > 0
        GROUP BY date_key, seller_key
    ),
    main_cat AS (
        SELECT
            m_seller_key,
            argMax(m_category_level_1, (m_cnt, m_category_level_1)) AS m_main_category_level_1
        FROM (
            SELECT
                f.seller_key AS m_seller_key,
                if(c.category_level_1 = '', 'unknown', c.category_level_1) AS m_category_level_1,
                count() AS m_cnt
            FROM gold_fact_product_daily AS f FINAL
            LEFT JOIN gold_dim_category AS c FINAL ON c.category_key = f.category_key
            GROUP BY m_seller_key, m_category_level_1
        )
        GROUP BY m_seller_key
    ),
    f5 AS (
        SELECT
            date_key AS r_date_key,
            seller_key AS r_seller_key,
            seller_rate_eod AS r_rate,
            seller_rate_cnt_eod AS r_rate_cnt,
            ifNull(new_seller_ratings, 0) AS r_new_ratings
        FROM gold_fact_seller_daily FINAL
    )
SELECT
    kd.full_date,
    f2.k_date_key,
    kd.sale_event,
    kd.is_reliable_day,
    f2.k_seller_key,
    if(s.seller_name = '', 'Không rõ người bán', s.seller_name),
    s.is_official,
    s.is_unknown OR s.seller_key = '',
    mc.m_main_category_level_1,
    f5.r_rate,
    f5.r_rate_cnt,
    f5.r_new_ratings,
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
    f2.k_active_brands
FROM f2
INNER JOIN mart_kpi_daily AS kd ON kd.date_key = f2.k_date_key
LEFT JOIN gold_dim_seller AS s FINAL ON s.seller_key = f2.k_seller_key
LEFT JOIN main_cat AS mc ON mc.m_seller_key = f2.k_seller_key
LEFT JOIN f5 ON f5.r_date_key = f2.k_date_key AND f5.r_seller_key = f2.k_seller_key
LEFT JOIN f3 ON f3.c_date_key = f2.k_date_key AND f3.c_seller_key = f2.k_seller_key;

ALTER TABLE mart_seller_daily REPLACE PARTITION tuple() FROM mart_seller_daily__stage;
DROP TABLE mart_seller_daily__stage;
