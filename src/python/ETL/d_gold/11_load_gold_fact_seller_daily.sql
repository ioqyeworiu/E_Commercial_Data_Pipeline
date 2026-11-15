-- F5. 1 dòng / seller / ngày. Chạy sau 07 và 08.
INSERT INTO gold_fact_seller_daily (
    date_key,
    seller_key,
    seller_rate_eod,
    seller_rate_cnt_eod,
    new_seller_ratings,
    active_products,
    avg_sell_price,
    avg_discount_pct,
    units_sold_est,
    revenue_est
)
WITH
seller_obs AS (
    SELECT
        date_key AS o_date_key,
        seller_key AS o_seller_key,
        argMax(seller_rate, observed_at) AS o_seller_rate_eod,
        argMax(seller_rate_cnt, observed_at) AS o_seller_rate_cnt_eod,
        toUInt32(uniqExact(product_id)) AS o_active_products,
        avg(sell_price) AS o_avg_sell_price,
        toFloat32(avg(discount_pct)) AS o_avg_discount_pct
    FROM gold_fact_product_snapshot FINAL
    GROUP BY date_key, seller_key
),
seller_sales AS (
    SELECT
        date_key AS s_date_key,
        seller_key AS s_seller_key,
        ifNull(sum(units_sold_est), 0) AS s_units_sold_est,
        ifNull(sum(revenue_est), 0) AS s_revenue_est
    FROM gold_fact_product_daily FINAL
    GROUP BY date_key, seller_key
),
joined AS (
    SELECT
        o.*,
        s.s_units_sold_est,
        s.s_revenue_est,
        lagInFrame(toNullable(o.o_seller_rate_cnt_eod), 1)
            OVER (PARTITION BY o.o_seller_key ORDER BY o.o_date_key) AS prev_rate_cnt
    FROM seller_obs AS o
    LEFT JOIN seller_sales AS s
        ON s.s_date_key = o.o_date_key AND s.s_seller_key = o.o_seller_key
)
SELECT
    o_date_key,
    o_seller_key,
    o_seller_rate_eod,
    o_seller_rate_cnt_eod,
    if(prev_rate_cnt IS NULL, NULL,
       toUInt32(greatest(toInt64(o_seller_rate_cnt_eod) - toInt64(prev_rate_cnt), 0))) AS new_seller_ratings,
    o_active_products,
    o_avg_sell_price,
    o_avg_discount_pct,
    s_units_sold_est,
    s_revenue_est
FROM joined;
