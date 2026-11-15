-- =====================================================================================
-- DATA QUALITY - ĐỐI SOÁT XUYÊN TẦNG (raw -> bronze -> silver -> gold)
-- B01 / S04 / G04 so từng cặp tầng trên TOÀN BỘ dữ liệu. File này chia theo NGÀY quan sát để chỉ ra
-- dữ liệu rơi ở ngày nào, bước nào (thường do 1 lần chạy job lỗi giữa chừng), và đo độ trễ giữa các tầng.
--
--   X01 reconciliation  Phễu theo ngày observed_at: số dòng mỗi bước phải bằng số khóa của bước trước
--                       (raw dòng -> raw khóa khác nhau -> bronze -> silver_snapshot -> gold F1)
--   X02 reconciliation  (file 06_lost_keys_debug.sql) liệt kê khóa bị rơi ; run_dq.sh chỉ chạy khi X01 FAIL
--   X03 timeliness      Tầng sau chậm hơn raw bao nhiêu giờ (max observed_at) -> phát hiện tầng chưa dựng lại
--
-- Cuối file có 1 SELECT in bảng phễu đầy đủ theo ngày (không ghi vào dq_check_result) để làm evidence.
-- Chạy: bash run_dq.sh cross
-- =====================================================================================



-- ---------------------------------------------------------------- X01 phễu theo ngày
-- raw được phép có dòng lặp (R07) -> mốc so sánh của bronze là SỐ KHÓA raw, không phải số dòng raw.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'cross', c.1, c.2, c.3, 'reconciliation', 'observed_at (ngày)', scope,
    count() AS days, countIf(c.4 != c.5) AS bad_days, dq_pct(bad_days, days), '%', 0, 0,
    arrayStringConcat(arraySlice(arraySort(groupArrayIf(concat(toString(d), ': ', toString(c.4), ' -> ', toString(c.5)), c.4 != c.5)), 1, 5), ' ; '),
    concat('tổng ', toString(sum(c.4)), ' -> ', toString(sum(c.5)), ' ; lệch ', toString(toInt64(sum(c.5)) - toInt64(sum(c.4))))
FROM (
    SELECT
        d, scope,
        sumIf(n, step = 'raw_key') AS raw_keys,
        sumIf(n, step = 'bronze') AS bronze_rows,
        sumIf(n, step = 'silver') AS silver_rows,
        sumIf(n, step = 'gold') AS gold_rows,
        sumIf(n, step = 'rv_raw_key') AS rv_raw_keys,
        sumIf(n, step = 'rv_bronze') AS rv_bronze_rows
    FROM (
        SELECT 'raw_key' AS step, assumeNotNull(toDate(dq_ts(observed_at))) AS d, dq_scope(toInt64OrZero(url_id)) AS scope,
               uniqExact(cityHash64(idempotency_key)) AS n
        FROM raw_product_snapshot WHERE dq_ts(observed_at) IS NOT NULL GROUP BY d, scope
        UNION ALL
        SELECT 'bronze', toDate(observed_at), dq_scope(url_id), count() FROM bronze_product_snapshot GROUP BY 2, 3
        UNION ALL
        SELECT 'silver', toDate(observed_at), dq_scope(url_id), count() FROM silver_snapshot WHERE url_id > 0 GROUP BY 2, 3
        UNION ALL
        SELECT 'gold', toDate(observed_at), dq_scope(product_id), count() FROM gold_fact_product_snapshot FINAL GROUP BY 2, 3
        UNION ALL
        SELECT 'rv_raw_key', assumeNotNull(toDate(dq_ts(observed_at))), dq_scope(toInt64OrZero(url_id)),
               uniqExact(cityHash64(idempotency_key))
        FROM raw_user_review WHERE dq_ts(observed_at) IS NOT NULL GROUP BY 2, 3
        UNION ALL
        SELECT 'rv_bronze', toDate(observed_at), dq_scope(url_id), count() FROM bronze_user_review GROUP BY 2, 3
    )
    GROUP BY d, scope
)
ARRAY JOIN [
    ('bronze_product_snapshot', 'X01a', 'Ngày có số dòng bronze khác số khóa raw', raw_keys, bronze_rows),
    ('silver_snapshot', 'X01b', 'Ngày có số dòng silver_snapshot khác số dòng bronze', bronze_rows, silver_rows),
    ('gold_fact_product_snapshot', 'X01c', 'Ngày có số dòng gold F1 khác số dòng silver_snapshot', silver_rows, gold_rows),
    ('bronze_user_review', 'X01d', 'Ngày có số review bronze khác số khóa review raw', rv_raw_keys, rv_bronze_rows)
] AS c
GROUP BY c.1, c.2, c.3, scope;



-- ---------------------------------------------------------------- X03 độ trễ giữa các tầng
-- So lần quan sát mới nhất của mỗi tầng với raw. Bronze / silver / gold là batch chạy tay, nên chậm vài giờ
-- là bình thường ; chậm > 1 ngày nghĩa là quên dựng lại tầng đó sau khi crawl thêm.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'cross', tbl, 'X03', 'Tầng chậm hơn raw (max observed_at)', 'timeliness', 'observed_at', 'all',
    0, 0, greatest(dateDiff('second', layer_max, raw_max), 0) / 3600., 'giờ', 6, 24,
    toString(layer_max), concat('raw mới nhất: ', toString(raw_max))
FROM (
    SELECT 'bronze_product_snapshot' AS tbl,
           (SELECT toDateTime(max(dq_ts(observed_at)), 'Asia/Ho_Chi_Minh') FROM raw_product_snapshot) AS raw_max,
           (SELECT max(observed_at) FROM bronze_product_snapshot) AS layer_max
    UNION ALL
    SELECT 'silver_snapshot',
           (SELECT toDateTime(max(dq_ts(observed_at)), 'Asia/Ho_Chi_Minh') FROM raw_product_snapshot),
           (SELECT max(observed_at) FROM silver_snapshot)
    UNION ALL
    SELECT 'gold_fact_product_snapshot',
           (SELECT toDateTime(max(dq_ts(observed_at)), 'Asia/Ho_Chi_Minh') FROM raw_product_snapshot),
           (SELECT max(observed_at) FROM gold_fact_product_snapshot)
    UNION ALL
    SELECT 'bronze_user_review',
           (SELECT toDateTime(max(dq_ts(observed_at)), 'Asia/Ho_Chi_Minh') FROM raw_user_review),
           (SELECT max(observed_at) FROM bronze_user_review)
    UNION ALL
    SELECT 'gold_fact_review',
           (SELECT toDateTime(max(dq_ts(observed_at)), 'Asia/Ho_Chi_Minh') FROM raw_user_review),
           (SELECT max(observed_at) FROM gold_fact_review)
);



-- ---------------------------------------------------------------- Bảng phễu theo ngày (chỉ in, không ghi)
SELECT 'PHỄU THEO NGÀY observed_at: raw (dòng / khóa) -> bronze -> silver -> gold F1 ; cột lech_* = bước sau - bước trước';
SELECT
    d AS ngay, scope,
    raw_rows, raw_keys, bronze_rows, silver_rows, gold_rows,
    toInt64(bronze_rows) - toInt64(raw_keys) AS lech_bronze,
    toInt64(silver_rows) - toInt64(bronze_rows) AS lech_silver,
    toInt64(gold_rows) - toInt64(silver_rows) AS lech_gold
FROM (
    SELECT
        d, scope,
        sumIf(n, step = 'raw_row') AS raw_rows,
        sumIf(n, step = 'raw_key') AS raw_keys,
        sumIf(n, step = 'bronze') AS bronze_rows,
        sumIf(n, step = 'silver') AS silver_rows,
        sumIf(n, step = 'gold') AS gold_rows
    FROM (
        SELECT 'raw_row' AS step, assumeNotNull(toDate(dq_ts(observed_at))) AS d, dq_scope(toInt64OrZero(url_id)) AS scope,
               count() AS n
        FROM raw_product_snapshot WHERE dq_ts(observed_at) IS NOT NULL GROUP BY d, scope
        UNION ALL
        SELECT 'raw_key', assumeNotNull(toDate(dq_ts(observed_at))), dq_scope(toInt64OrZero(url_id)), uniqExact(cityHash64(idempotency_key))
        FROM raw_product_snapshot WHERE dq_ts(observed_at) IS NOT NULL GROUP BY 2, 3
        UNION ALL
        SELECT 'bronze', toDate(observed_at), dq_scope(url_id), count() FROM bronze_product_snapshot GROUP BY 2, 3
        UNION ALL
        SELECT 'silver', toDate(observed_at), dq_scope(url_id), count() FROM silver_snapshot WHERE url_id > 0 GROUP BY 2, 3
        UNION ALL
        SELECT 'gold', toDate(observed_at), dq_scope(product_id), count() FROM gold_fact_product_snapshot FINAL GROUP BY 2, 3
    )
    GROUP BY d, scope
)
ORDER BY scope, ngay
FORMAT PrettyCompactMonoBlock;
