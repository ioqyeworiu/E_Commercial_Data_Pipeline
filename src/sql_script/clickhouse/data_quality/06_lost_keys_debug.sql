-- =====================================================================================
-- X02 (debug) - liệt kê tối đa 20 lần crawl bị rơi / bị thừa ở mỗi bước raw -> bronze -> silver -> gold.
-- Không ghi vào dq_check_result. run_dq.sh chỉ chạy file này khi X01 / B01 / S04 / G04 của lần chạy có FAIL
-- (mỗi bước phải GROUP BY ~20 triệu khóa nên không chạy khi không cần).
-- Chạy tay: docker exec -i clickhouse_server clickhouse-client ... --multiquery < 06_lost_keys_debug.sql
-- =====================================================================================

SELECT 'KHÓA BỊ RƠI / THỪA GIỮA CÁC TẦNG (tối đa 20 dòng mỗi bước)';

SELECT buoc, loai, url_id, observed_at
FROM (
    SELECT '1. raw -> bronze' AS buoc, if(in_a, 'thiếu ở tầng sau', 'thừa ở tầng sau (orphan)') AS loai, uid AS url_id, ts AS observed_at
    FROM (
        SELECT kh, max(src = 0) AS in_a, max(src = 1) AS in_b, any(uid) AS uid, any(ts) AS ts
        FROM (
            SELECT cityHash64(lower(idempotency_key)) AS kh, 0 AS src, toInt64OrZero(url_id) AS uid,
                   toDateTime(dq_ts(observed_at), 'Asia/Ho_Chi_Minh') AS ts
            FROM raw_product_snapshot
            UNION ALL
            SELECT cityHash64(lower(idempotency_key)), 1, toInt64(url_id), observed_at FROM bronze_product_snapshot
        )
        GROUP BY kh
    )
    WHERE in_a != in_b
    LIMIT 20

    UNION ALL

    SELECT '2. bronze -> silver', if(in_a, 'thiếu ở tầng sau', 'thừa ở tầng sau (orphan)'), uid, ts
    FROM (
        SELECT kh, max(src = 0) AS in_a, max(src = 1) AS in_b, any(uid) AS uid, any(ts) AS ts
        FROM (
            SELECT cityHash64(toInt64(url_id), observed_at) AS kh, 0 AS src, toInt64(url_id) AS uid,
                   toNullable(observed_at) AS ts
            FROM bronze_product_snapshot
            UNION ALL
            SELECT cityHash64(url_id, observed_at), 1, url_id, observed_at FROM silver_snapshot WHERE url_id > 0
        )
        GROUP BY kh
    )
    WHERE in_a != in_b
    LIMIT 20

    UNION ALL

    SELECT '3. silver -> gold F1', if(in_a, 'thiếu ở tầng sau', 'thừa ở tầng sau (orphan)'), uid, ts
    FROM (
        SELECT kh, max(src = 0) AS in_a, max(src = 1) AS in_b, any(uid) AS uid, any(ts) AS ts
        FROM (
            SELECT cityHash64(url_id, observed_at) AS kh, 0 AS src, url_id AS uid, toNullable(observed_at) AS ts
            FROM silver_snapshot WHERE url_id > 0
            UNION ALL
            SELECT cityHash64(product_id, observed_at), 1, product_id, observed_at FROM gold_fact_product_snapshot FINAL
        )
        GROUP BY kh
    )
    WHERE in_a != in_b
    LIMIT 20
)
ORDER BY buoc, url_id, observed_at
FORMAT PrettyCompactMonoBlock;
