-- Entity resolution brand: gộp theo brand_normalized.
-- silver_brand tách 1 brand thành nhiều dòng theo brand_origin (vd 'novelty' có '' và 'việt nam'),
-- ở gold các dòng đó được gộp lại thành 1 brand_key.
INSERT INTO gold_dim_brand (
    brand_key,
    brand_normalized,
    brand_name,
    brand_origin,
    brand_origins,
    name_aliases,
    is_unknown,
    first_seen_at
)
SELECT
    lower(hex(SHA256(norm))) AS brand_key,
    norm AS brand_normalized,
    if(norm = 'unknown', 'Không rõ thương hiệu',
       if(empty(topKIf(1)(name, name != '')), norm, topKIf(1)(name, name != '')[1])) AS brand_name,
    if(empty(topKIf(1)(origin, origin != '')), '', topKIf(1)(origin, origin != '')[1]) AS brand_origin,
    groupUniqArrayIf(origin, origin != '') AS brand_origins,
    groupUniqArrayIf(name, name != '') AS name_aliases,
    norm = 'unknown' AS is_unknown,
    min(seen_at) AS first_seen_at
FROM (
    SELECT
        if(brand_normalized = '', 'unknown', brand_normalized) AS norm,
        name,
        brand_origin AS origin,
        valid_from AS seen_at
    FROM silver_brand

    UNION ALL

    -- luôn có member 'unknown' để fact không bị mất liên kết
    SELECT 'unknown', '', '', now()
)
GROUP BY norm;
