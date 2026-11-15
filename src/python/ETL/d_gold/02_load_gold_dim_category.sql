INSERT INTO gold_dim_category (
    category_key,
    category_path,
    category_level_1,
    category_level_2,
    category_level_3,
    category_level_4,
    category_level_5,
    category_level_6,
    category_depth,
    leaf_category
)
SELECT
    category_key,
    if(empty(levels), 'unknown', arrayStringConcat(levels, ' > ')) AS category_path,
    category_level_1,
    category_level_2,
    category_level_3,
    category_level_4,
    category_level_5,
    category_level_6,
    toUInt8(length(levels)) AS category_depth,
    if(empty(levels), 'unknown', levels[-1]) AS leaf_category
FROM (
    SELECT
        category_sk AS category_key,
        category_level_1,
        category_level_2,
        category_level_3,
        category_level_4,
        category_level_5,
        category_level_6,
        arrayFilter(x -> x != '', [
            category_level_1, category_level_2, category_level_3,
            category_level_4, category_level_5, category_level_6
        ]) AS levels
    FROM silver_category
    ORDER BY valid_from DESC
    LIMIT 1 BY category_sk
);
