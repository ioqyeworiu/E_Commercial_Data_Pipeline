INSERT INTO silver_seller (
    name,
    seller_rate,
    seller_rate_cnt,
    seller_sk,
    valid_from
    )
WITH bronze_seller AS (
    SELECT * EXCEPT(ranked)
    FROM
    (
        SELECT
            seller,
            seller_rate,
            seller_rate_cnt,
            lower(hex(SHA256(ifnull(concat_ws('||', seller, seller_rate, seller_rate_cnt), '')))) AS seller_sk,
            observed_at,
            rank() OVER (PARTITION BY seller_sk ORDER BY observed_at DESC) AS ranked
        FROM bronze_product_snapshot
        -- WHERE observed_at >= toDate({date_from:String}) AND observed_at <= toDate({date_to:String})
    ) AS brz_sel
    LEFT JOIN (SELECT * FROM silver_seller WHERE silver_seller.is_current = TRUE) AS sil_sel
    ON sil_sel.seller_sk = brz_sel.seller_sk
    WHERE sil_sel.seller_sk LIKE '' AND ranked = 1
)
SELECT 
    seller,
    seller_rate,
    seller_rate_cnt,
    seller_sk,
    observed_at
FROM bronze_seller;