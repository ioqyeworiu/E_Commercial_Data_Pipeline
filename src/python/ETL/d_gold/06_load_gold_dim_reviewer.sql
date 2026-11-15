-- Đọc từ bronze_user_review vì silver_user / silver_review chưa được triển khai.
-- Khi có silver_user thì đổi nguồn sang silver_user.
INSERT INTO gold_dim_reviewer (
    user_key,
    user_name,
    joined_at_est,
    tenure_band,
    reviewed_cnt,
    thank_received,
    first_seen_at,
    last_seen_at
)
SELECT
    lower(hex(SHA256(norm))) AS user_key,
    norm AS user_name,
    argMax(joined_at, observed_at) AS joined_at_est,
    multiIf(
        argMax(joined_duration, observed_at) <= 0, 'không rõ',
        argMax(joined_duration, observed_at) < 8760, 'dưới 1 năm',
        argMax(joined_duration, observed_at) < 3 * 8760, '1-3 năm',
        argMax(joined_duration, observed_at) < 5 * 8760, '3-5 năm',
        'trên 5 năm'
    ) AS tenure_band,
    toUInt32(greatest(argMax(reviewed, observed_at), 0)) AS reviewed_cnt,
    toUInt32(greatest(argMax(thank_received, observed_at), 0)) AS thank_received,
    min(observed_at) AS first_seen_at,
    max(observed_at) AS last_seen_at
FROM (
    SELECT
        if(user_name = '', 'unknown', user_name) AS norm,
        joined_duration,                    -- đơn vị giờ ; 0 = không parse được
        if(joined_duration > 0, observed_at - toIntervalHour(joined_duration), NULL) AS joined_at,
        reviewed,
        thank_received,
        observed_at
    FROM bronze_user_review
)
GROUP BY norm;
