-- silver_seller sinh 1 seller_sk mới mỗi khi seller_rate / seller_rate_cnt đổi.
-- Gold gộp theo tên seller ; rating được đưa sang gold_fact_product_snapshot / gold_fact_seller_daily.
INSERT INTO gold_dim_seller (
    seller_key,
    seller_name,
    is_official,
    is_unknown,
    first_seen_at
)
SELECT
    lower(hex(SHA256(norm))) AS seller_key,
    if(norm = 'unknown', 'Không rõ người bán', norm) AS seller_name,
    match(norm, 'official|chính hãng|^tiki trading$') AS is_official,
    norm = 'unknown' AS is_unknown,
    min(seen_at) AS first_seen_at
FROM (
    SELECT
        if(name = '', 'unknown', name) AS norm,
        valid_from AS seen_at
    FROM silver_seller

    UNION ALL

    SELECT 'unknown', now()
)
GROUP BY norm;
