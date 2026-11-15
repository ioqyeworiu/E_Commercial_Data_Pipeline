INSERT INTO silver_product (
    product_sk,
    url_id,
    category_sk,
    title,
    made_in,
    warranty,
    description,
    valid_from
    )
WITH silver_product AS
(SELECT * EXCEPT(ranked)
FROM
    (SELECT 
        lower(HEX(SHA256(ifnull(concat_ws('||', url_id, title, made_in, warranty), '')))) AS product_sk,
        url_id,
        lower(HEX(SHA256(category_tree))) AS category_sk,
        title,
        made_in,
        warranty,
        description,
        observed_at,
        rank() OVER (PARTITION BY product_sk ORDER BY observed_at DESC) AS ranked
    FROM bronze_product_snapshot) AS brz_prod_snap
    -- WHERE observed_at >= toDate({date_from:String}) AND observed_at <= toDate({date_to:String})
LEFT JOIN (SELECT * FROM silver_product WHERE silver_product.is_current = TRUE) AS sil_prod 
ON sil_prod.product_sk = brz_prod_snap.product_sk
WHERE sil_prod.product_sk LIKE '' AND ranked = 1
)
SELECT
    product_sk,
    url_id,
    category_sk,
    title,
    made_in,
    warranty,
    description,
    observed_at
FROM silver_product;




-- ALTER TABLE silver_product
-- UPDATE is_current = FALSE
-- WHERE (url_id, product_sk) IN (
--     SELECT url_id, product_sk
--     FROM (
--         SELECT url_id, product_sk, rank() OVER (PARTITION BY url_id ORDER BY valid_from DESC) AS rank
--         FROM silver_product
--     ) AS ranked_products
--     WHERE rank > 1
-- );