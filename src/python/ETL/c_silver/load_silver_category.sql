INSERT INTO silver_category (
    category_sk,
    category_level_1,
    category_level_2,
    category_level_3,
    category_level_4,
    category_level_5,
    category_level_6,
    valid_from
    )
WITH bronze_category AS (
    SELECT * EXCEPT(ranked)
    FROM
    (
        SELECT
            lower(hex(SHA256(ifnull(category_tree, '')))) AS category_sk,
            splitByRegexp('\\s*>\\s*', category_tree) AS category_levels,
            observed_at,
            rank() OVER (PARTITION BY lower(category_sk) ORDER BY observed_at DESC) AS ranked
        FROM bronze_product_snapshot
        -- WHERE observed_at >= toDate({date_from:String}) AND observed_at <= toDate({date_to:String})
    ) AS brz_cat
    LEFT JOIN (SELECT * FROM silver_category WHERE silver_category.is_current = TRUE) AS sil_cat
    ON sil_cat.category_sk = brz_cat.category_sk
    WHERE sil_cat.category_sk LIKE '' AND ranked = 1
)
SELECT 
    category_sk,
    category_levels[1] AS category_level_1,
    category_levels[2] AS category_level_2,
    category_levels[3] AS category_level_3,
    category_levels[4] AS category_level_4,
    category_levels[5] AS category_level_5,
    category_levels[6] AS category_level_6,
    observed_at
FROM bronze_category;



-- ALTER TABLE silver_category
-- UPDATE is_current = FALSE
-- WHERE (url_id, category_sk) IN (
--     SELECT url_id, category_sk
--     FROM (
--         SELECT url_id, category_sk, rank() OVER (PARTITION BY url_id ORDER BY valid_from DESC) AS rank
--         FROM silver_category
--     ) AS ranked_categories
--     WHERE rank > 1
-- ); 