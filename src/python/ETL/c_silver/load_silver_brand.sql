INSERT INTO silver_brand (
    name,
    brand_normalized,
    brand_origin,
    brand_sk,
    valid_from
    )
WITH bronze_brand AS (
    SELECT * EXCEPT(ranked)
    FROM
    (
        SELECT
            brand,
            brand_normalized,
            lower(hex(SHA256(ifnull(concat_ws('||', brand_normalized, brand_origin), '')))) AS brand_sk,
            brand_origin,
            observed_at,
            rank() OVER (PARTITION BY brand_sk ORDER BY observed_at DESC) AS ranked
        FROM bronze_product_snapshot
        -- WHERE observed_at >= toDate({date_from:String}) AND observed_at <= toDate({date_to:String})
    ) AS brz_bra
    LEFT JOIN (SELECT * FROM silver_brand WHERE silver_brand.is_current = TRUE) AS sil_bra
    ON sil_bra.brand_sk = brz_bra.brand_sk
    WHERE sil_bra.brand_sk LIKE '' AND ranked = 1
)
SELECT 
    brand,
    brand_normalized,
    brand_origin,
    brand_sk,
    observed_at
FROM bronze_brand;