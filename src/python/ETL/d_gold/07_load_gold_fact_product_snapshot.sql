-- F1. 1 dòng / sản phẩm / lần crawl.
-- Đổi khóa silver sang khóa gold:
--   brand : silver brand_sk (hash brand_normalized + brand_origin) -> brand_normalized -> gold brand_key
--   seller: silver seller_sk (hash tên + rating) -> tên + rating tại thời điểm crawl -> gold seller_key
-- silver_snapshot là MergeTree thường nên có thể có dòng lặp khi nạp lại ; bảng đích là
-- ReplacingMergeTree ORDER BY (product_id, observed_at) nên dòng lặp được gộp khi merge / FINAL.
-- KHÔNG dùng ORDER BY ... LIMIT 1 BY ở đây: với 10 triệu dòng, LIMIT BY giữ hash set của mọi
-- (url_id, observed_at) trong RAM và làm ClickHouse (~2,7 GB) hết bộ nhớ.
INSERT INTO gold_fact_product_snapshot (
    product_id,
    observed_at,
    date_key,
    hour_of_day,
    product_key,
    brand_key,
    seller_key,
    category_key,
    list_price,
    sell_price,
    discount_amount,
    discount_pct,
    sold_cum,
    product_rate,
    product_rate_cnt,
    seller_rate,
    seller_rate_cnt
)
SELECT
    product_id,
    observed_at,
    toYYYYMMDD(observed_at) AS date_key,
    toHour(observed_at) AS hour_of_day,
    product_key,
    lower(hex(SHA256(if(src_brand_normalized = '', 'unknown', src_brand_normalized)))) AS brand_key,
    lower(hex(SHA256(if(src_seller_name = '', 'unknown', src_seller_name)))) AS seller_key,
    category_key,
    toUInt64(greatest(round(if(src_price > 0, src_price, src_sell_price)), 0)) AS list_price,
    toUInt64(greatest(round(src_sell_price), 0)) AS sell_price,
    if(list_price > sell_price, list_price - sell_price, 0) AS discount_amount,
    toFloat32(multiIf(
        src_discount > 0, src_discount,
        list_price > sell_price, (list_price - sell_price) / list_price * 100,
        0
    )) AS discount_pct,
    toUInt32(greatest(src_sold, 0)) AS sold_cum,
    src_product_rate AS product_rate,
    toUInt32(greatest(src_product_rate_cnt, 0)) AS product_rate_cnt,
    src_seller_rate AS seller_rate,
    toUInt32(greatest(src_seller_rate_cnt, 0)) AS seller_rate_cnt
FROM (
    SELECT
        s.url_id AS product_id,
        s.observed_at AS observed_at,
        s.product_sk AS product_key,
        s.category_sk AS category_key,
        b.brand_normalized AS src_brand_normalized,   -- '' khi không join được -> unknown
        se.name AS src_seller_name,
        se.seller_rate AS src_seller_rate,
        se.seller_rate_cnt AS src_seller_rate_cnt,
        s.price AS src_price,
        s.sell_price AS src_sell_price,
        s.discount AS src_discount,
        s.sold AS src_sold,
        s.product_rate AS src_product_rate,
        s.product_rate_cnt AS src_product_rate_cnt
    FROM silver_snapshot AS s
    LEFT JOIN (
        SELECT brand_sk, brand_normalized FROM silver_brand LIMIT 1 BY brand_sk
    ) AS b ON b.brand_sk = s.brand_sk
    LEFT JOIN (
        SELECT seller_sk, name, seller_rate, seller_rate_cnt FROM silver_seller LIMIT 1 BY seller_sk
    ) AS se ON se.seller_sk = s.seller_sk
    WHERE s.url_id > 0
    -- nạp tăng dần (tùy chọn):
    -- AND s.observed_at >= toDateTime({date_from:String}) AND s.observed_at < toDateTime({date_to:String})
);
