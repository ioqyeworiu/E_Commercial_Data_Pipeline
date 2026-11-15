-- F4. 1 dòng / review. Đọc bronze_user_review vì silver_review chưa được triển khai.
-- Nguồn không có review_id -> review_key = hash(url_id, user_name, prod_properties, comment).
-- idempotency_key của bronze = hash(url_id, observed_at) nên cùng 1 review crawl 2 lần sẽ có 2 key khác nhau ;
-- review_key ở đây gộp được các lần crawl đó (giữ lần quan sát mới nhất).
INSERT INTO gold_fact_review (
    review_key,
    product_id,
    product_key,
    brand_key,
    seller_key,
    category_key,
    user_key,
    reviewed_at_est,
    review_date_key,
    observed_at,
    observed_date_key,
    star_score,
    is_positive,
    is_negative,
    comment_length,
    word_count,
    image_count,
    has_image,
    use_time_hours,
    review_status,
    prod_properties
)
WITH
reviews AS (
    SELECT
        lower(hex(SHA256(concat_ws('||', toString(url_id), user_name, prod_properties, comment)))) AS r_review_key,
        url_id AS r_product_id,
        if(user_name = '', 'unknown', user_name) AS r_user_name,
        rated_at AS r_rated_at,              -- số giờ tính từ lúc review đến lúc crawl ; 0 = không parse được
        observed_at AS r_observed_at,
        star_score AS r_star_score,
        comment AS r_comment,
        images_urls AS r_images_urls,
        use_time AS r_use_time,
        status AS r_status,
        prod_properties AS r_prod_properties
    FROM bronze_user_review
    ORDER BY observed_at DESC
    LIMIT 1 BY r_review_key
),
-- Gắn review với version hiện tại của sản phẩm (review nói về sản phẩm nói chung, không theo version)
product_keys AS (
    SELECT
        product_id,
        argMax(product_key, observed_at) AS p_product_key,
        argMax(brand_key, observed_at) AS p_brand_key,
        argMax(seller_key, observed_at) AS p_seller_key,
        argMax(category_key, observed_at) AS p_category_key
    FROM gold_fact_product_snapshot FINAL
    GROUP BY product_id
)
SELECT
    r.r_review_key AS review_key,
    r.r_product_id AS product_id,
    p.p_product_key AS product_key,
    p.p_brand_key AS brand_key,
    p.p_seller_key AS seller_key,
    p.p_category_key AS category_key,
    lower(hex(SHA256(r.r_user_name))) AS user_key,
    if(r.r_rated_at > 0, r.r_observed_at - toIntervalSecond(toInt64(r.r_rated_at * 3600)), NULL) AS reviewed_at_est,
    if(reviewed_at_est IS NULL, 0, toYYYYMMDD(assumeNotNull(reviewed_at_est))) AS review_date_key,
    r.r_observed_at AS observed_at,
    toYYYYMMDD(r.r_observed_at) AS observed_date_key,
    toUInt8(greatest(least(round(r.r_star_score), 5), 0)) AS star_score,
    star_score >= 4 AS is_positive,
    star_score BETWEEN 1 AND 2 AS is_negative,
    lengthUTF8(r.r_comment) AS comment_length,
    toUInt32(length(splitByWhitespace(r.r_comment))) AS word_count,
    toUInt16(if(isValidJSON(r.r_images_urls), JSONLength(r.r_images_urls), 0)) AS image_count,
    image_count > 0 AS has_image,
    if(r.r_use_time > 0, toFloat32(r.r_use_time), NULL) AS use_time_hours,
    r.r_status AS review_status,
    r.r_prod_properties AS prod_properties
FROM reviews AS r
LEFT JOIN product_keys AS p ON p.product_id = r.r_product_id;
