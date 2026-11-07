--check missing value
SELECT
    kv.1                                                         AS column_name,
    count()                                                      AS total_rows,
    countIf(upper(trim(kv.2)) IN ('', 'NULL', 'NAN', 'NONE', 'N/A')) AS missing_rows,
    round(missing_rows * 100.0 / total_rows, 2)                  AS missing_pct
FROM raw_product_snapshot
ARRAY JOIN [
    ('url_id', url_id), ('title', title), ('brand', brand), ('price', price),
    ('discount', discount), ('sell_price', sell_price), ('sold', sold),
    ('product_rate', product_rate), ('product_rate_cnt', product_rate_cnt),
    ('category_tree', category_tree), ('brand_origin', brand_origin),
    ('made_in', made_in), ('warranty', warranty), ('description', description),
    ('seller', seller), ('seller_rate', seller_rate),
    ('seller_rate_cnt', seller_rate_cnt), ('observed_at', observed_at)
] AS kv
GROUP BY column_name
ORDER BY missing_pct DESC;


--check malformed numeric
SELECT
    kv.1 AS column_name,
    kv.3 AS expected_type,
    count() AS total_rows,
    countIf(kv.2 = '')                                   AS empty_rows,
    countIf(kv.2 != '' AND kv.2 != trim(kv.2))           AS has_whitespace,
    countIf(kv.2 != ''
        AND ((kv.3 = 'int'     AND NOT match(trim(kv.2), '^[0-9]+$'))
          OR (kv.3 = 'decimal' AND NOT match(trim(kv.2), '^[0-9]+([.][0-9]+)?$')))) AS malformed_rows,
    countIf(match(kv.2, '^0[0-9]'))                      AS leading_zero,
    anyIf(kv.2, kv.2 != ''
        AND ((kv.3 = 'int'     AND NOT match(trim(kv.2), '^[0-9]+$'))
          OR (kv.3 = 'decimal' AND NOT match(trim(kv.2), '^[0-9]+([.][0-9]+)?$')))) AS sample_bad_value
FROM raw_product_snapshot
ARRAY JOIN [
    ('url_id',          url_id,          'int'),
    ('price',           price,           'int'),
    ('discount',        discount,        'int'),
    ('sell_price',      sell_price,      'int'),
    ('sold',            sold,            'int'),
    ('product_rate',    product_rate,    'decimal'),
    ('product_rate_cnt',product_rate_cnt,'int'),
    ('seller_rate',     seller_rate,     'decimal'),
    ('seller_rate_cnt', seller_rate_cnt, 'int')
] AS kv
GROUP BY column_name, expected_type
ORDER BY malformed_rows DESC, empty_rows DESC;


--check overview duplicate
SELECT
    count()                                   AS total_rows,
    uniqExact(url_id)                         AS distinct_products,
    uniqExact(url_id, observed_at)            AS distinct_snapshots,
    total_rows - distinct_snapshots           AS duplicate_rows,
    round(duplicate_rows * 100.0 / total_rows, 2) AS duplicate_pct
FROM raw_product_snapshot;


--check whether exact_duplicate or conflict
SELECT
    dup_type,
    count()          AS dup_keys,
    sum(cnt)         AS rows_involved,
    max(cnt)         AS max_copies
FROM (
    SELECT
        url_id,
        observed_at,
        count() AS cnt,
        uniqExact(cityHash64(
            title, brand, price, discount, sell_price, sold, product_rate, product_rate_cnt,
            category_tree, brand_origin, made_in, warranty,
            description, seller, seller_rate, seller_rate_cnt
        )) AS versions,
        if(versions = 1, 'exact_duplicate', 'conflicting') AS dup_type
    FROM raw_product_snapshot
    GROUP BY url_id, observed_at
    HAVING cnt > 1
)
GROUP BY dup_type;


