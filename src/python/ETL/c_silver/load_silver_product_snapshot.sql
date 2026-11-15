INSERT INTO silver_snapshot (
    product_sk,
    url_id,
    product_sk_2,
    brand_sk,
    seller_sk,
    category_sk,
    price,
    discount,
    sell_price,
    sold,
    product_rate,
    product_rate_cnt,
    observed_at
)
SELECT 
    lower(HEX(SHA256(ifnull(concat_ws('||', url_id, title, made_in, warranty), '')))) AS product_sk,
    url_id,
    lower(HEX(SHA256(ifnull(concat_ws('||', url_id, title, price, discount, sell_price), '')))) AS product_sk_2,
    lower(hex(SHA256(ifnull(concat_ws('||', brand_normalized, brand_origin), '')))) AS brand_sk,
    lower(hex(SHA256(ifnull(concat_ws('||', seller, seller_rate, seller_rate_cnt), '')))) AS seller_sk,
    lower(HEX(SHA256(category_tree))) AS category_sk,
    price,
    discount,
    sell_price,
    sold,
    product_rate,
    product_rate_cnt,
    observed_at
FROM bronze_product_snapshot