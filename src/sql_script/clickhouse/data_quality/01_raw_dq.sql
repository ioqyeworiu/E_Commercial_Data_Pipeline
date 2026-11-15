-- =====================================================================================
-- DATA QUALITY - TẦNG RAW (raw_product_snapshot, raw_user_review)
-- Raw giữ nguyên chuỗi từ crawler, nên đây là nơi đo lỗi của NGUỒN (trang web, selector, crawler).
--
--   R01 volume        Khối lượng theo ngày: ngày không có dòng nào hoặc tụt < 50% trung vị
--   R02 timeliness    Độ tươi: số giờ từ lần crawl mới nhất đến hiện tại
--   R03 completeness  Thiếu giá trị theo từng cột, ngưỡng theo mức quan trọng của cột
--   R04 completeness  Selector failure: nhiều trường lõi cùng thiếu trên 1 dòng (trang render lỗi)
--   R05 validity      Sai định dạng (giá, %, bộ đếm, điểm, chuỗi thời lượng của review)
--   R06 validity /    observed_at: không đọc được, không đúng ISO-8601, ngoài khoảng hợp lý ;
--       consistency / ingested_at < observed_at (lệch đồng hồ) ; độ trễ crawl -> ClickHouse p95
--       timeliness
--   R07 uniqueness    Dòng trùng idempotency_key, nội dung giống hệt (Kafka gửi lại, bronze phải loại)
--   R08 uniqueness    Va chạm khóa: cùng idempotency_key nhưng NỘI DUNG KHÁC -> dropDuplicates làm mất dữ liệu
--   R08b completeness Review: tỉ lệ lần crawl chỉ có đúng 1 review (chỉ xét dữ liệu thật, xem ghi chú ở check)
--   R09 validity /    Review: status ngoài miền, status lệch star_score, JSON mảng lỗi, ảnh chưa phải URL
--       consistency
--   R10 integrity     idempotency_key tính lại từ (url_id, observed_at) phải khớp khóa đã lưu: khóa tất định
--                     thì Spark đọc lại Kafka (restart, mất checkpoint) vẫn sinh đúng khóa cũ và bị loại trùng
--
-- Chạy: bash run_dq.sh raw   (hoặc clickhouse-client --param_run_id=manual --multiquery < 01_raw_dq.sql)
-- Ước tính trên 10 triệu dòng: vài chục giây đến ~2 phút. R07/R08 nặng nhất (GROUP BY 10 triệu khóa).
-- =====================================================================================



-- ---------------------------------------------------------------- R01 khối lượng theo ngày
-- Ngày đầu và cuối kỳ crawl thường thiếu giờ nên không xét "tụt". Ngày ở giữa mà không có dòng nào
-- (crawler chết) được tính bằng cách so số ngày lịch với số ngày có dữ liệu.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', tbl, 'R01', 'Khối lượng theo ngày: ngày trống hoặc tụt dưới 50% trung vị', 'volume',
    'observed_at', scope,
    toUInt64(dateDiff('day', d_min, d_max) + 1) AS calendar_days,
    toUInt64(calendar_days - count()) + countIf(n < 0.5 * med AND d NOT IN (d_min, d_max)) AS bad_days,
    dq_pct(bad_days, calendar_days), '%', 0, 10,
    arrayStringConcat(arraySlice(arraySort(
        groupArrayIf(concat(toString(d), ': ', toString(n)), n < 0.5 * med AND d NOT IN (d_min, d_max))), 1, 5), ' ; '),
    concat(toString(d_min), ' -> ', toString(d_max), ' ; trung vị ', toString(round(any(med))), ' dòng/ngày ; ',
           toString(calendar_days - count()), ' ngày không có dòng nào')
FROM (
    SELECT tbl, scope, d, n,
           median(n) OVER (PARTITION BY tbl, scope) AS med,
           min(d) OVER (PARTITION BY tbl, scope) AS d_min,
           max(d) OVER (PARTITION BY tbl, scope) AS d_max
    FROM (
        SELECT 'raw_product_snapshot' AS tbl, dq_scope(toInt64OrZero(url_id)) AS scope,
               assumeNotNull(toDate(dq_ts(observed_at))) AS d, count() AS n
        FROM raw_product_snapshot
        WHERE dq_ts(observed_at) IS NOT NULL
        GROUP BY tbl, scope, d
        UNION ALL
        SELECT 'raw_user_review', dq_scope(toInt64OrZero(url_id)),
               assumeNotNull(toDate(dq_ts(observed_at))), count()
        FROM raw_user_review
        WHERE dq_ts(observed_at) IS NOT NULL
        GROUP BY 1, 2, 3
    )
)
GROUP BY tbl, scope, d_min, d_max;



-- ---------------------------------------------------------------- R02 độ tươi
-- Crawler chạy liên tục thì lần crawl mới nhất phải trong vòng vài giờ.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', tbl, 'R02', 'Độ tươi: số giờ từ lần crawl mới nhất đến hiện tại', 'timeliness',
    'observed_at', scope, total, 0,
    dateDiff('second', last_obs, now()) / 3600., 'giờ', 24, 72,
    toString(last_obs), concat('ingested_at mới nhất: ', toString(last_ing))
FROM (
    SELECT 'raw_product_snapshot' AS tbl, dq_scope(toInt64OrZero(url_id)) AS scope, count() AS total,
           max(dq_ts(observed_at)) AS last_obs, max(ingested_at) AS last_ing
    FROM raw_product_snapshot GROUP BY scope
    UNION ALL
    SELECT 'raw_user_review', dq_scope(toInt64OrZero(url_id)), count(), max(dq_ts(observed_at)), max(ingested_at)
    FROM raw_user_review GROUP BY 2
);



-- ---------------------------------------------------------------- R03 thiếu giá trị theo cột
-- Ngưỡng (warn / fail %) theo mức quan trọng:
--   cột lõi      0,5 / 2   : không có thì dòng vô dụng (khóa, thời điểm, giá bán, tên, danh mục)
--   cột quan trọng 5 / 20  : thiếu làm sai phân tích (seller, brand, đã bán ...)
--   cột tùy chọn  INFO     : nguồn được phép để trống (giá gốc khi không giảm, xuất xứ, bảo hành, rating khi chưa có đánh giá)
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', 'raw_product_snapshot', 'R03', 'Thiếu giá trị (rỗng / NULL / undefined)', 'completeness',
    c.1, scope, total, c.2, dq_pct(c.2, total), '%', c.3, c.4, '', c.5
FROM (
    SELECT
        dq_scope(toInt64OrZero(url_id)) AS scope,
        count() AS total,
        countIf(dq_missing(url_id)) AS m_url_id,
        countIf(dq_missing(observed_at)) AS m_observed_at,
        countIf(dq_missing(title)) AS m_title,
        countIf(dq_missing(sell_price)) AS m_sell_price,
        countIf(dq_missing(category_tree)) AS m_category_tree,
        countIf(dq_missing(seller)) AS m_seller,
        countIf(dq_missing(brand)) AS m_brand,
        countIf(dq_missing(sold)) AS m_sold,
        countIf(dq_missing(seller_rate)) AS m_seller_rate,
        countIf(dq_missing(seller_rate_cnt)) AS m_seller_rate_cnt,
        countIf(dq_missing(price)) AS m_price,
        countIf(dq_missing(discount)) AS m_discount,
        countIf(dq_missing(product_rate)) AS m_product_rate,
        countIf(dq_missing(product_rate_cnt)) AS m_product_rate_cnt,
        countIf(dq_missing(brand_origin)) AS m_brand_origin,
        countIf(dq_missing(made_in)) AS m_made_in,
        countIf(dq_missing(warranty)) AS m_warranty,
        countIf(dq_missing(description)) AS m_description,
        countIf(dq_missing(additional_info)) AS m_additional_info
    FROM raw_product_snapshot
    GROUP BY scope
)
ARRAY JOIN [
    ('url_id',           m_url_id,           0.5, 2,  'cột lõi'),
    ('observed_at',      m_observed_at,      0.5, 2,  'cột lõi'),
    ('title',            m_title,            0.5, 2,  'cột lõi'),
    ('sell_price',       m_sell_price,       0.5, 2,  'cột lõi ; "Liên hệ" không tính là thiếu'),
    ('category_tree',    m_category_tree,    0.5, 2,  'cột lõi'),
    ('seller',           m_seller,           5,   20, 'cột quan trọng'),
    ('brand',            m_brand,            5,   20, 'cột quan trọng'),
    ('sold',             m_sold,             5,   20, 'cột quan trọng ; bronze đổi ô trống thành 0'),
    ('seller_rate',      m_seller_rate,      5,   20, 'cột quan trọng ; "Chưa có đánh giá" không tính là thiếu'),
    ('seller_rate_cnt',  m_seller_rate_cnt,  5,   20, 'cột quan trọng'),
    ('price',            m_price,            0,   -1, 'tùy chọn: trống khi sản phẩm không giảm giá'),
    ('discount',         m_discount,         0,   -1, 'tùy chọn: trống khi sản phẩm không giảm giá'),
    ('product_rate',     m_product_rate,     0,   -1, 'tùy chọn: trống khi chưa có đánh giá'),
    ('product_rate_cnt', m_product_rate_cnt, 0,   -1, 'tùy chọn: trống khi chưa có đánh giá'),
    ('brand_origin',     m_brand_origin,     0,   -1, 'tùy chọn'),
    ('made_in',          m_made_in,          0,   -1, 'tùy chọn'),
    ('warranty',         m_warranty,         0,   -1, 'tùy chọn'),
    ('description',      m_description,      0,   -1, 'tùy chọn'),
    ('additional_info',  m_additional_info,  0,   -1, 'tùy chọn')
] AS c;

-- comment: Tiki cho phép review chỉ có sao, nên comment trống một phần là bình thường.
-- Nhưng trống gần như 100% là dấu hiệu selector của comment_crawler không lấy được nội dung -> FAIL ở 95%.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', 'raw_user_review', 'R03', 'Thiếu giá trị (rỗng / NULL / undefined)', 'completeness',
    c.1, scope, total, c.2, dq_pct(c.2, total), '%', c.3, c.4, '', c.5
FROM (
    SELECT
        dq_scope(toInt64OrZero(url_id)) AS scope,
        count() AS total,
        countIf(dq_missing(url_id)) AS m_url_id,
        countIf(dq_missing(observed_at)) AS m_observed_at,
        countIf(dq_missing(user_name)) AS m_user_name,
        countIf(dq_missing(star_score)) AS m_star_score,
        countIf(dq_missing(status)) AS m_status,
        countIf(dq_missing(rated_at)) AS m_rated_at,
        countIf(dq_missing(joined_duration)) AS m_joined_duration,
        countIf(dq_missing(reviewed)) AS m_reviewed,
        countIf(dq_missing(thank_received)) AS m_thank_received,
        countIf(dq_missing(comment)) AS m_comment,
        countIf(dq_missing(use_time)) AS m_use_time,
        countIf(dq_missing(images_urls)) AS m_images_urls,
        countIf(dq_missing(prod_properties)) AS m_prod_properties
    FROM raw_user_review
    GROUP BY scope
)
ARRAY JOIN [
    ('url_id',          m_url_id,          0.5, 2,  'cột lõi'),
    ('observed_at',     m_observed_at,     0.5, 2,  'cột lõi'),
    ('user_name',       m_user_name,       0.5, 2,  'cột lõi'),
    ('star_score',      m_star_score,      0.5, 2,  'cột lõi'),
    ('status',          m_status,          5,   20, 'cột quan trọng'),
    ('rated_at',        m_rated_at,        5,   20, 'cột quan trọng: thiếu thì không biết ngày review'),
    ('joined_duration', m_joined_duration, 5,   20, 'cột quan trọng'),
    ('reviewed',        m_reviewed,        5,   20, 'cột quan trọng'),
    ('thank_received',  m_thank_received,  5,   20, 'cột quan trọng'),
    ('comment',         m_comment,         60,  95, 'review chỉ có sao là hợp lệ ; ~100% trống = selector comment hỏng'),
    ('use_time',        m_use_time,        0,   -1, 'tùy chọn'),
    ('images_urls',     m_images_urls,     0,   -1, 'tùy chọn: "[]" = review không có ảnh'),
    ('prod_properties', m_prod_properties, 0,   -1, 'tùy chọn: "[]" = sản phẩm không có biến thể')
] AS c;



-- ---------------------------------------------------------------- R04 selector failure
-- 1 trường trống là chuyện thường của dữ liệu web ; >= 3 trong 5 trường lõi cùng trống trên 1 dòng
-- gần như chắc chắn là trang chưa render xong / bị chặn / selector đổi -> dòng rác.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', 'raw_product_snapshot', 'R04',
    'Selector failure: >= 3/5 trường lõi cùng thiếu (title, sell_price, category_tree, seller, sold)', 'completeness',
    '', scope, count() AS total, countIf(n_miss >= 3) AS failed, dq_pct(failed, total), '%', 0.5, 2,
    anyIf(concat(url_id, ' @ ', observed_at), n_miss >= 3),
    concat('thiếu đúng 1 trường: ', toString(countIf(n_miss = 1)), ' dòng ; thiếu 2: ', toString(countIf(n_miss = 2)),
           ' ; thiếu cả 5: ', toString(countIf(n_miss = 5)))
FROM (
    SELECT dq_scope(toInt64OrZero(url_id)) AS scope, url_id, observed_at,
           toUInt8(dq_missing(title)) + toUInt8(dq_missing(sell_price)) + toUInt8(dq_missing(category_tree))
         + toUInt8(dq_missing(seller)) + toUInt8(dq_missing(sold)) AS n_miss
    FROM raw_product_snapshot
)
GROUP BY scope;

INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', 'raw_user_review', 'R04',
    'Selector failure: >= 3/4 trường lõi cùng thiếu (user_name, star_score, status, rated_at)', 'completeness',
    '', scope, count() AS total, countIf(n_miss >= 3) AS failed, dq_pct(failed, total), '%', 0.5, 2,
    anyIf(concat(url_id, ' @ ', observed_at), n_miss >= 3),
    concat('thiếu đúng 1 trường: ', toString(countIf(n_miss = 1)), ' dòng ; thiếu 2: ', toString(countIf(n_miss = 2)))
FROM (
    SELECT dq_scope(toInt64OrZero(url_id)) AS scope, url_id, observed_at,
           toUInt8(dq_missing(user_name)) + toUInt8(dq_missing(star_score))
         + toUInt8(dq_missing(status)) + toUInt8(dq_missing(rated_at)) AS n_miss
    FROM raw_user_review
)
GROUP BY scope;



-- ---------------------------------------------------------------- R05 sai định dạng
-- Chỉ xét ô KHÔNG thiếu (ô thiếu đã đo ở R03). Regex mô tả mọi dạng hợp lệ đã thấy trên Tiki:
--   giá '173.700₫' / '430000' ; giảm giá '-20%' ; đã bán '759' / '1k' / 'Đã bán 1,2k+' ;
--   bộ đếm '(819 đánh giá)' / '(37.5k+ đánh giá)' / '(5.6tr+ đánh giá)'.
-- 'Liên hệ' và 'Chưa có đánh giá' là giá trị giữ chỗ HỢP LỆ của nguồn -> không tính lỗi, chỉ đếm ở note.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', 'raw_product_snapshot', 'R05', 'Sai định dạng (bỏ qua ô thiếu)', 'validity',
    col, scope, countIf(NOT is_missing) AS checked, countIf(is_bad) AS failed, dq_pct(failed, checked), '%', 0, 0.5,
    anyIf(val, is_bad),
    concat('giá trị giữ chỗ hợp lệ (Liên hệ / Chưa có đánh giá): ', toString(countIf(is_sentinel)))
FROM (
    SELECT
        dq_scope(toInt64OrZero(url_id)) AS scope,
        kv.1 AS col,
        kv.2 AS val,
        lowerUTF8(trimBoth(kv.2)) AS v,
        dq_missing(kv.2) AS is_missing,
        v IN ('liên hệ', 'chưa có đánh giá') AS is_sentinel,
        NOT is_missing AND NOT is_sentinel AND NOT multiIf(
            col = 'url_id',                      match(v, '^[0-9]+$'),
            col IN ('price', 'sell_price'),      match(v, '^[0-9][0-9.,]*\\s*(₫|đ|vnđ|vnd)?$'),
            col = 'discount',                    match(v, '^[-+]?[0-9]+([.,][0-9]+)?\\s*%$'),
            col = 'sold',                        match(v, '^(đã bán\\s*)?[0-9][0-9.,]*\\s*(k|tr|nghìn|triệu)?\\+?$'),
            col IN ('product_rate', 'seller_rate'), match(v, '^[0-9]([.,][0-9]+)?(\\s*/\\s*5)?$'),
            col IN ('product_rate_cnt', 'seller_rate_cnt'),
                match(v, '^\\(?[0-9][0-9.,]*\\s*(k|tr|nghìn|triệu)?\\+?(\\s*đánh giá)?\\)?$'),
            false) AS is_bad
    FROM raw_product_snapshot
    ARRAY JOIN [
        ('url_id', url_id), ('price', price), ('sell_price', sell_price), ('discount', discount), ('sold', sold),
        ('product_rate', product_rate), ('product_rate_cnt', product_rate_cnt),
        ('seller_rate', seller_rate), ('seller_rate_cnt', seller_rate_cnt)
    ] AS kv
)
GROUP BY col, scope;

INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', 'raw_user_review', 'R05', 'Sai định dạng (bỏ qua ô thiếu)', 'validity',
    col, scope, countIf(NOT is_missing) AS checked, countIf(is_bad) AS failed, dq_pct(failed, checked), '%', 0, 0.5,
    anyIf(val, is_bad), ''
FROM (
    SELECT
        dq_scope(toInt64OrZero(url_id)) AS scope,
        kv.1 AS col,
        kv.2 AS val,
        lowerUTF8(trimBoth(kv.2)) AS v,
        dq_missing(kv.2) AS is_missing,
        NOT is_missing AND NOT multiIf(
            col = 'url_id',          match(v, '^[0-9]+$'),
            col = 'star_score',      match(v, '^[1-5]$'),
            col = 'reviewed',        match(v, '^[0-9][0-9.,]*\\s*(k|tr)?\\+?\\s*đánh giá$'),
            col = 'thank_received',  match(v, '^[0-9][0-9.,]*\\s*(k|tr)?\\+?\\s*lượt cảm ơn$'),
            col = 'joined_duration', match(v, '^đã tham gia (hơn )?[0-9]+ (năm|tháng|tuần|ngày|giờ|phút)( [0-9]+ (tháng|tuần|ngày))?$'),
            col = 'rated_at',        match(v, '^đánh giá vào (hơn )?[0-9]+ (năm|tháng|tuần|ngày|giờ|phút) trước$'),
            col = 'use_time',        match(v, '^đã dùng (hơn )?[0-9]+ (năm|tháng|tuần|ngày|giờ|phút)$'),
            false) AS is_bad
    FROM raw_user_review
    ARRAY JOIN [
        ('url_id', url_id), ('star_score', star_score), ('reviewed', reviewed), ('thank_received', thank_received),
        ('joined_duration', joined_duration), ('rated_at', rated_at), ('use_time', use_time)
    ] AS kv
)
GROUP BY col, scope;



-- ---------------------------------------------------------------- R06 thời gian (observed_at, ingested_at)
-- observed_at là mốc thời gian NGHIỆP VỤ của toàn pipeline (CLAUDE.md), nên phải đọc được, có múi giờ,
-- và không sau thời điểm ClickHouse nhận dòng. Độ trễ = ingested_at - observed_at (crawler -> Kafka -> Spark -> CH).
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', tbl, c.1, c.2, c.3, 'observed_at', scope, total, c.4, c.5, c.6, c.7, c.8, c.9, c.10
FROM (
    SELECT
        tbl, scope,
        count() AS total,
        countIf(ts IS NULL AND NOT dq_missing(observed_at)) AS bad_parse,
        countIf(NOT dq_missing(observed_at) AND NOT match(observed_at,
            '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?(Z|[+-][0-9]{2}:?[0-9]{2})$')) AS not_iso,
        countIf(ts > now() + INTERVAL 10 MINUTE OR ts < toDateTime('2020-01-01 00:00:00', 'Asia/Ho_Chi_Minh')) AS out_range,
        countIf(lat_s < -60) AS skew,
        countIf(lat_s > 3600) AS slow,
        quantileIf(0.5)(lat_s, lat_s IS NOT NULL) AS p50,
        quantileIf(0.95)(lat_s, lat_s IS NOT NULL) AS p95,
        max(lat_s) AS lat_max,
        anyIf(observed_at, ts IS NULL AND NOT dq_missing(observed_at)) AS s_parse,
        anyIf(observed_at, NOT dq_missing(observed_at) AND NOT match(observed_at,
            '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?(Z|[+-][0-9]{2}:?[0-9]{2})$')) AS s_iso,
        anyIf(observed_at, ts > now() + INTERVAL 10 MINUTE OR ts < toDateTime('2020-01-01 00:00:00', 'Asia/Ho_Chi_Minh')) AS s_range,
        anyIf(concat(observed_at, ' -> ingested ', toString(ingested_at)), lat_s < -60) AS s_skew
    FROM (
        SELECT 'raw_product_snapshot' AS tbl, dq_scope(toInt64OrZero(url_id)) AS scope, observed_at, ingested_at,
               dq_ts(observed_at) AS ts, dateDiff('second', ts, ingested_at) AS lat_s
        FROM raw_product_snapshot
        UNION ALL
        SELECT 'raw_user_review', dq_scope(toInt64OrZero(url_id)), observed_at, ingested_at,
               dq_ts(observed_at), dateDiff('second', dq_ts(observed_at), ingested_at)
        FROM raw_user_review
    )
    GROUP BY tbl, scope
)
ARRAY JOIN [
    ('R06a', 'observed_at không đọc được thành thời gian', 'validity',
        bad_parse, dq_pct(bad_parse, total), '%', 0, 0, s_parse, ''),
    ('R06b', 'observed_at không đúng ISO-8601 có múi giờ', 'validity',
        not_iso, dq_pct(not_iso, total), '%', 0, 0, s_iso, 'thiếu múi giờ thì không biết chính xác lúc crawler nhìn thấy trang'),
    ('R06c', 'observed_at ở tương lai hoặc trước 2020', 'validity',
        out_range, dq_pct(out_range, total), '%', 0, 0, s_range, ''),
    ('R06d', 'ingested_at sớm hơn observed_at quá 1 phút (lệch đồng hồ crawler / Spark)', 'consistency',
        skew, dq_pct(skew, total), '%', 0, 0.1, s_skew, ''),
    ('R06e', 'Độ trễ crawl -> ClickHouse, phân vị 95', 'timeliness',
        slow, ifNull(p95, 0) / 60., 'phút', 10, 60, '',
        concat('p50 = ', toString(round(ifNull(p50, 0) / 60., 2)), ' phút ; max = ', toString(round(ifNull(lat_max, 0) / 60., 1)),
               ' phút ; failed_rows = số dòng trễ > 60 phút'))
] AS c;



-- ---------------------------------------------------------------- R07 + R08 trùng / va chạm idempotency_key
-- Gom theo idempotency_key (đúng khóa mà Spark dùng để loại trùng). Trong mỗi nhóm so hash nội dung:
--   min(hash) = max(hash) -> các bản giống hệt (Kafka gửi lại / Spark đọc lại sau restart) -> R07, bronze loại được.
--   min(hash) != max(hash) -> CÙNG khóa nhưng KHÁC nội dung -> R08: khóa không đủ phân biệt bản ghi,
--                            dropDuplicates giữ ngẫu nhiên 1 bản và làm MẤT các bản còn lại.
-- Chỉ giữ số (url_id, observed_at dạng DateTime64) trong mỗi nhóm để 10 triệu nhóm vẫn vừa RAM.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', tbl, c.1, c.2, 'uniqueness', 'idempotency_key', scope, total, c.3, dq_pct(c.3, total), '%',
    c.4, c.5, c.6, c.7
FROM (
    SELECT
        tbl, dq_scope(uid) AS scope,
        sum(cnt) AS total,
        toUInt64(sumIf(cnt - 1, cnt > 1 AND h_min = h_max)) AS exact_extra,
        countIf(cnt > 1 AND h_min = h_max) AS exact_keys,
        sumIf(cnt, h_min != h_max) AS conflict_rows,
        countIf(h_min != h_max) AS conflict_keys,
        max(cnt) AS max_copies,
        anyIf(concat(toString(uid), ' @ ', toString(ts)), cnt > 1 AND h_min = h_max) AS s_exact,
        anyIf(concat(toString(uid), ' @ ', toString(ts)), h_min != h_max) AS s_conflict
    FROM (
        SELECT 'raw_product_snapshot' AS tbl, cityHash64(idempotency_key) AS kh,
               any(toInt64OrZero(url_id)) AS uid, any(dq_ts(observed_at)) AS ts, count() AS cnt,
               min(content_hash) AS h_min, max(content_hash) AS h_max
        FROM (
            SELECT idempotency_key, url_id, observed_at,
                   cityHash64(title, brand, price, discount, sell_price, sold, product_rate, product_rate_cnt,
                              category_tree, brand_origin, made_in, warranty, description, seller, seller_rate,
                              seller_rate_cnt, additional_info) AS content_hash
            FROM raw_product_snapshot
        )
        GROUP BY kh
        UNION ALL
        SELECT 'raw_user_review', cityHash64(idempotency_key),
               any(toInt64OrZero(url_id)), any(dq_ts(observed_at)), count(),
               min(content_hash), max(content_hash)
        FROM (
            SELECT idempotency_key, url_id, observed_at,
                   cityHash64(user_name, joined_duration, reviewed, thank_received, star_score, comment, status,
                              images_urls, rated_at, use_time, prod_properties) AS content_hash
            FROM raw_user_review
        )
        GROUP BY 2
    )
    GROUP BY tbl, scope
)
ARRAY JOIN [
    ('R07', 'Dòng trùng idempotency_key, nội dung giống hệt (bản lặp thừa)', exact_extra, 0, 1, s_exact,
        concat(toString(exact_keys), ' khóa bị lặp ; nhiều nhất ', toString(max_copies),
               ' bản / khóa ; raw là append-only nên được phép có, nhưng bronze phải loại hết (B01)')),
    ('R08', 'Va chạm khóa: cùng idempotency_key nhưng nội dung khác', conflict_rows, 0, 0, s_conflict,
        concat(toString(conflict_keys), ' khóa ; khóa = sha2(url_id, observed_at) không đủ phân biệt bản ghi'))
] AS c;

-- R08b. comment_crawler gán CÙNG observed_at cho mọi review của 1 lần crawl trang, trong khi
-- idempotency_key của review = sha2(url_id, observed_at). Spark loại trùng theo khóa này TRƯỚC khi ghi raw,
-- nên các review bị gộp nhầm không còn để R08 thấy. Dấu vết còn lại: mọi lần crawl chỉ còn đúng 1 review.
-- Chỉ xét dữ liệu thật: bộ sinh test cho mỗi review 1 observed_at riêng, nên không phản ánh hành vi crawler.
-- WARN (không FAIL) vì sản phẩm ít review cũng có thể chỉ có 1 review ; cần >= 5 lần crawl mới kết luận.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', 'raw_user_review', 'R08b', 'Tỉ lệ lần crawl chỉ còn đúng 1 review (dấu hiệu gộp nhầm review)',
    'completeness', 'idempotency_key', 'real', count() AS crawls, countIf(n = 1) AS one_review,
    dq_pct(one_review, crawls), '%', 90, 101, '',
    concat('nhiều nhất ', toString(max(n)), ' review / lần crawl ; trung bình ', toString(round(avg(n), 2)))
FROM (
    SELECT url_id, observed_at, count() AS n
    FROM raw_user_review
    WHERE toInt64OrZero(url_id) < 1000000
    GROUP BY url_id, observed_at
)
HAVING crawls >= 5;



-- ---------------------------------------------------------------- R09 review: miền giá trị và nhất quán
-- status <-> star_score: Tiki hiển thị nhãn theo số sao (5 cực kì hài lòng ... 1 rất không hài lòng).
-- images_urls hiện lưu nguyên thuộc tính CSS 'background-image: url("...")' chứ chưa phải URL ->
-- R09e báo WARN để nhắc bóc URL trước khi dùng (gold chỉ đếm số ảnh nên chưa bị ảnh hưởng).
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', 'raw_user_review', c.1, c.2, c.3, c.4, scope, c.5, c.6, dq_pct(c.6, c.5), '%', c.7, c.8, c.9, ''
FROM (
    SELECT
        scope,
        count() AS total,
        countIf(NOT dq_missing(status) AND st_star = 0) AS bad_status,
        countIf(st_star > 0 AND star BETWEEN 1 AND 5) AS both_present,
        countIf(st_star > 0 AND star BETWEEN 1 AND 5 AND st_star != star) AS mismatch,
        countIf(NOT (isValidJSON(images_urls) AND JSONType(images_urls) = 'Array')) AS bad_img_json,
        countIf(NOT (isValidJSON(prod_properties) AND JSONType(prod_properties) = 'Array')) AS bad_prop_json,
        sum(length(imgs)) AS img_total,
        sum(arrayCount(x -> NOT match(x, '^https?://[^\\s"\'()]+$'), imgs)) AS img_not_url,
        anyIf(status, NOT dq_missing(status) AND st_star = 0) AS s_status,
        anyIf(concat(status, ' / ', star_score, ' sao'), st_star > 0 AND star BETWEEN 1 AND 5 AND st_star != star) AS s_mismatch,
        anyIf(images_urls, NOT (isValidJSON(images_urls) AND JSONType(images_urls) = 'Array')) AS s_img_json,
        anyIf(prod_properties, NOT (isValidJSON(prod_properties) AND JSONType(prod_properties) = 'Array')) AS s_prop_json,
        anyIf(substring(arrayFirst(x -> NOT match(x, '^https?://[^\\s"\'()]+$'), imgs), 1, 120),
              arrayExists(x -> NOT match(x, '^https?://[^\\s"\'()]+$'), imgs)) AS s_img
    FROM (
        SELECT
            dq_scope(toInt64OrZero(url_id)) AS scope,
            status, star_score, images_urls, prod_properties,
            transform(lowerUTF8(trimBoth(status)),
                      ['cực kì hài lòng', 'cực kỳ hài lòng', 'hài lòng', 'bình thường', 'không hài lòng', 'rất không hài lòng'],
                      [5, 5, 4, 3, 2, 1], 0) AS st_star,
            toUInt8OrZero(trimBoth(star_score)) AS star,
            if(isValidJSON(images_urls), JSONExtract(images_urls, 'Array(String)'), []) AS imgs
        FROM raw_user_review
    )
    GROUP BY scope
)
ARRAY JOIN [
    ('R09a', 'status ngoài miền 5 nhãn của Tiki', 'validity', 'status', total, bad_status, 0, 0.5, s_status),
    ('R09b', 'status không khớp star_score', 'consistency', 'status', both_present, mismatch, 0, 1, s_mismatch),
    ('R09c', 'images_urls không phải mảng JSON', 'validity', 'images_urls', total, bad_img_json, 0, 0, s_img_json),
    ('R09d', 'prod_properties không phải mảng JSON', 'validity', 'prod_properties', total, bad_prop_json, 0, 0, s_prop_json),
    ('R09e', 'Phần tử ảnh chưa phải URL thuần (tính trên số ảnh)', 'validity', 'images_urls', img_total, img_not_url, 0, 101, s_img)
] AS c;



-- ---------------------------------------------------------------- R10 idempotency_key tất định
-- Spark: idempotency_key = sha2(concat_ws('\u0001', url_id, observed_at), 256) trên giá trị đã trim.
-- Tính lại ở đây và so với khóa đã lưu. Lệch nghĩa là cách sinh khóa đã đổi giữa các phiên bản job
-- (hoặc khóa sinh từ dữ liệu khác dữ liệu được lưu) -> lần đọc lại Kafka sẽ ra khóa mới và lọt bản trùng.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'raw', tbl, 'R10', 'idempotency_key tính lại từ (url_id, observed_at) không khớp khóa đã lưu',
    'integrity', 'idempotency_key', scope, count() AS total, countIf(recomputed != stored) AS failed,
    dq_pct(failed, total), '%', 0, 0,
    anyIf(concat(url_id, ' @ ', observed_at), recomputed != stored),
    'khóa tất định là điều kiện để chống trùng khi Spark đọc lại Kafka'
FROM (
    SELECT 'raw_product_snapshot' AS tbl, dq_scope(toInt64OrZero(url_id)) AS scope, url_id, observed_at,
           lower(idempotency_key) AS stored,
           lower(hex(SHA256(concat(url_id, '\x01', observed_at)))) AS recomputed
    FROM raw_product_snapshot
    UNION ALL
    SELECT 'raw_user_review', dq_scope(toInt64OrZero(url_id)), url_id, observed_at,
           lower(idempotency_key), lower(hex(SHA256(concat(url_id, '\x01', observed_at))))
    FROM raw_user_review
)
GROUP BY tbl, scope;
