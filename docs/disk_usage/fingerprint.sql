SELECT count() AS rows, uniqExact(idempotency_key) AS uniq_keys,
       sum(cityHash64(idempotency_key, url_id, title, brand, ifNull(brand_normalized, '<NULL>'), price, discount, sell_price, sold,
           product_rate, product_rate_cnt, category_tree, brand_origin, made_in, warranty, description, seller,
           seller_rate, seller_rate_cnt, additional_info, observed_at)) AS content_hash
FROM {tbl:Identifier}
