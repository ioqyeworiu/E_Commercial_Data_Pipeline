-- =====================================================================================
-- DATA QUALITY - TẦNG BRONZE (bronze_product_snapshot, bronze_user_review)
-- Bronze = raw đã ép kiểu + chuẩn hóa bằng Spark. Ở đây đo: (1) job có làm mất / nhân đôi dòng không,
-- (2) job parse có đúng không (so với giá trị kỳ vọng tính độc lập bằng SQL), (3) dữ liệu sau chuẩn hóa
-- có hợp lệ và nhất quán về nghiệp vụ / theo thời gian không, (4) entity resolution brand / seller.
--
--   B01 reconciliation / uniqueness  raw -> bronze theo idempotency_key: thiếu, thừa (orphan), trùng
--   B02 accuracy      Giá trị bronze lệch giá trị kỳ vọng tính lại từ raw (lỗi hàm normalize_*, lệch múi giờ)
--                     Sản phẩm: mẫu 5% theo url_id (url_id % 20 = 0) + toàn bộ dữ liệu thật ; review: toàn bộ
--   B03 validity      Miền giá trị: số âm, % giảm ngoài [0,100], rating ngoài [0,5], giá vô lý, thời gian lạ
--   B04 consistency   Nghiệp vụ: giá bán > giá gốc, % giảm không khớp giá, rating và số lượt rating mâu thuẫn ;
--       completeness  sell_price = 0 (nguồn thiếu giá)
--   B05 completeness  Thiếu brand / seller / category sau chuẩn hóa ; brand có tên nhưng brand_normalized NULL
--   B06 consistency   Entity resolution: brand gộp nhiều cách viết, khóa quá ngắn, cặp khóa gần giống nhau
--                     (ứng viên chưa gộp), tên seller khác nhau nhưng cùng khóa chuẩn hóa (seller chưa gộp)
--   B07 validity      Cây danh mục: độ sâu ngoài [2,6], có cấp rỗng ; leaf thuộc nhiều nhánh cha
--   B08 consistency   Theo thời gian của từng url_id: bộ đếm lũy kế giảm (tách "tụt về 0 do ô trống"
--                     và "giảm thật"), seller chập chờn / đổi, category / brand đổi
--   B09 validity      Ngoại lai: giá nhảy > ±80% giữa 2 lần crawl ; giá ngoài IQR x3 (thang log) trong danh mục cấp 1
--   B10 validity /    Review: sao ngoài 1..5, thời lượng không parse được, review cũ hơn tài khoản, user rỗng
--       consistency
--
-- Chạy: bash run_dq.sh bronze
-- Nặng nhất: B01 (GROUP BY ~20 triệu khóa) và B08 (window theo url_id trên 10 triệu dòng) ; cần spill ra đĩa.
-- =====================================================================================



-- ---------------------------------------------------------------- B01 đối soát raw -> bronze
-- Gom khóa của cả 2 tầng vào 1 GROUP BY (không JOIN 10 triệu x 10 triệu).
-- Kỳ vọng: mỗi khóa raw có đúng 1 dòng bronze ; bronze không có khóa lạ.
-- Job bronze append toàn bộ raw mỗi lần chạy -> chạy lại mà không TRUNCATE sẽ làm B01c > 0.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', tbl, c.1, c.2, c.3, 'idempotency_key', scope, c.4, c.5, dq_pct(c.5, c.4), '%', 0, 0,
    dq_sample(c.5, c.6), c.7
FROM (
    SELECT
        tbl, dq_scope(uid) AS scope,
        countIf(in_raw) AS raw_keys,
        countIf(in_bronze) AS bronze_keys,
        sum(b_copies) AS bronze_rows,
        countIf(in_raw AND NOT in_bronze) AS missing,
        countIf(in_bronze AND NOT in_raw) AS orphan,
        toUInt64(sumIf(b_copies - 1, b_copies > 1)) AS dup,
        toString(anyIf(uid, in_raw AND NOT in_bronze)) AS s_missing,
        toString(anyIf(uid, in_bronze AND NOT in_raw)) AS s_orphan,
        toString(anyIf(uid, b_copies > 1)) AS s_dup
    FROM (
        SELECT tbl, kh, max(src = 0) AS in_raw, max(src = 1) AS in_bronze, countIf(src = 1) AS b_copies, any(uid) AS uid
        FROM (
            SELECT 'bronze_product_snapshot' AS tbl, cityHash64(lower(idempotency_key)) AS kh, 0 AS src,
                   toInt64OrZero(url_id) AS uid
            FROM raw_product_snapshot
            UNION ALL
            SELECT 'bronze_product_snapshot', cityHash64(lower(idempotency_key)), 1, toInt64(url_id) FROM bronze_product_snapshot
            UNION ALL
            SELECT 'bronze_user_review', cityHash64(lower(idempotency_key)), 0, toInt64OrZero(url_id) FROM raw_user_review
            UNION ALL
            SELECT 'bronze_user_review', cityHash64(lower(idempotency_key)), 1, toInt64(url_id) FROM bronze_user_review
        )
        GROUP BY tbl, kh
    )
    GROUP BY tbl, scope
)
ARRAY JOIN [
    ('B01a', 'Khóa có ở raw nhưng không có ở bronze (job làm mất dòng)', 'reconciliation', raw_keys, missing, s_missing,
        concat('raw ', toString(raw_keys), ' khóa -> bronze ', toString(bronze_keys), ' khóa')),
    ('B01b', 'Khóa ở bronze không có ở raw (orphan)', 'reconciliation', bronze_keys, orphan, s_orphan,
        'raw đã bị xóa mà bronze chưa dựng lại, hoặc khóa bị biến đổi khi ép kiểu'),
    ('B01c', 'Dòng trùng idempotency_key ở bronze', 'uniqueness', bronze_rows, dup, s_dup,
        'job bronze append toàn bộ raw: chạy lại mà không TRUNCATE sẽ nhân đôi')
] AS c;



-- ---------------------------------------------------------------- B02 độ chính xác của job parse (sản phẩm)
-- Giá trị kỳ vọng tính ĐỘC LẬP bằng SQL từ chuỗi raw rồi so với bronze:
--   giá: ghép mọi chữ số ('173.700₫' -> 173700), sai >= 1 đồng là lỗi (bronze lưu Float32: giá > 16,7 triệu
--        không còn chính xác tới từng đồng nếu không chia hết cho 2, 4, 8 ...)
--   bộ đếm (sold, *_rate_cnt): hiểu hậu tố k / tr ('1k' -> 1000, '(5.6tr+ đánh giá)' -> 5.600.000)
--   observed_at: lệch > 1 giây là lỗi ép kiểu / múi giờ (Spark cast timestamp -> JDBC -> DateTime)
-- Dùng mẫu 5% sản phẩm (đủ lớn: ~500 nghìn dòng) để JOIN theo khóa không tốn RAM ; dữ liệu thật lấy toàn bộ.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', 'B02', 'Giá trị bronze lệch giá trị kỳ vọng tính lại từ raw',
    'accuracy', c.1, scope, total, c.2, dq_pct(c.2, total), '%', c.3, c.4, c.5,
    concat(c.6, if(scope = 'test', ' ; mẫu 5% theo url_id', ''))
FROM (
    SELECT
        dq_scope(b.url_id) AS scope,
        count() AS total,
        countIf(toInt64OrZero(r.url_id) != b.url_id) AS f_url_id,
        countIf(abs(b.price - e_price) >= 1) AS f_price,
        countIf(abs(b.sell_price - e_sell_price) >= 1) AS f_sell_price,
        countIf(abs(b.discount - e_discount) > 0.01) AS f_discount,
        countIf(abs(b.sold - e_sold) >= 1) AS f_sold,
        countIf(abs(b.product_rate - e_product_rate) > 0.001) AS f_product_rate,
        countIf(abs(b.product_rate_cnt - e_product_rate_cnt) >= 1) AS f_product_rate_cnt,
        countIf(abs(b.seller_rate - e_seller_rate) > 0.001) AS f_seller_rate,
        countIf(abs(b.seller_rate_cnt - e_seller_rate_cnt) >= 1) AS f_seller_rate_cnt,
        countIf(e_observed_at IS NULL OR abs(dateDiff('second', b.observed_at, assumeNotNull(e_observed_at))) > 1) AS f_observed_at,
        countIf(NOT dq_missing(r.title) AND match(r.title, '[\\p{L}\\p{N}]') AND b.title = '') AS f_title,
        countIf(match(r.brand, '[\\p{L}\\p{N}]') AND b.brand_normalized IS NULL) AS f_brand_normalized,
        anyIf(concat(r.url_id, ' -> ', toString(b.url_id)), toInt64OrZero(r.url_id) != b.url_id) AS s_url_id,
        anyIf(concat(r.price, ' -> ', toString(b.price)), abs(b.price - e_price) >= 1) AS s_price,
        anyIf(concat(r.sell_price, ' -> ', toString(b.sell_price)), abs(b.sell_price - e_sell_price) >= 1) AS s_sell_price,
        anyIf(concat(r.discount, ' -> ', toString(b.discount)), abs(b.discount - e_discount) > 0.01) AS s_discount,
        anyIf(concat(r.sold, ' -> ', toString(b.sold), ' (kỳ vọng ', toString(e_sold), ')'), abs(b.sold - e_sold) >= 1) AS s_sold,
        anyIf(concat(r.product_rate, ' -> ', toString(b.product_rate)), abs(b.product_rate - e_product_rate) > 0.001) AS s_product_rate,
        anyIf(concat(r.product_rate_cnt, ' -> ', toString(b.product_rate_cnt), ' (kỳ vọng ', toString(e_product_rate_cnt), ')'),
              abs(b.product_rate_cnt - e_product_rate_cnt) >= 1) AS s_product_rate_cnt,
        anyIf(concat(r.seller_rate, ' -> ', toString(b.seller_rate)), abs(b.seller_rate - e_seller_rate) > 0.001) AS s_seller_rate,
        anyIf(concat(r.seller_rate_cnt, ' -> ', toString(b.seller_rate_cnt), ' (kỳ vọng ', toString(e_seller_rate_cnt), ')'),
              abs(b.seller_rate_cnt - e_seller_rate_cnt) >= 1) AS s_seller_rate_cnt,
        anyIf(concat(r.observed_at, ' -> ', toString(b.observed_at)),
              e_observed_at IS NULL OR abs(dateDiff('second', b.observed_at, assumeNotNull(e_observed_at))) > 1) AS s_observed_at,
        anyIf(r.title, NOT dq_missing(r.title) AND match(r.title, '[\\p{L}\\p{N}]') AND b.title = '') AS s_title,
        anyIf(r.brand, match(r.brand, '[\\p{L}\\p{N}]') AND b.brand_normalized IS NULL) AS s_brand_normalized
    FROM (
        SELECT
            lower(idempotency_key) AS k, url_id, title, brand, price, sell_price, discount, sold,
            product_rate, product_rate_cnt, seller_rate, seller_rate_cnt, observed_at,
            toFloat64OrZero(replaceRegexpAll(price, '[^0-9]', '')) AS e_price,
            toFloat64OrZero(replaceRegexpAll(sell_price, '[^0-9]', '')) AS e_sell_price,
            if(match(replaceRegexpAll(discount, '\\s+', ''), '^[-+]?[0-9]+([.,][0-9]+)?%?$'),
               abs(toFloat64OrZero(replaceOne(extract(discount, '([0-9]+(?:[.,][0-9]+)?)'), ',', '.'))), 0) AS e_discount,
            dq_expected_count(sold) AS e_sold,
            toFloat64OrZero(replaceOne(extract(replaceRegexpAll(product_rate, '\\s+', ''), '^([0-9]+(?:[.,][0-9]+)?)'), ',', '.')) AS pr,
            if(pr BETWEEN 0 AND 5, pr, 0) AS e_product_rate,
            dq_expected_count(product_rate_cnt) AS e_product_rate_cnt,
            toFloat64OrZero(replaceOne(extract(replaceRegexpAll(seller_rate, '\\s+', ''), '^([0-9]+(?:[.,][0-9]+)?)'), ',', '.')) AS sr,
            if(sr BETWEEN 0 AND 5, sr, 0) AS e_seller_rate,
            dq_expected_count(seller_rate_cnt) AS e_seller_rate_cnt,
            toDateTime(dq_ts(observed_at), 'Asia/Ho_Chi_Minh') AS e_observed_at
        FROM raw_product_snapshot
        WHERE toInt64OrZero(url_id) % 20 = 0 OR toInt64OrZero(url_id) < 1000000
        LIMIT 1 BY k
    ) AS r
    INNER JOIN (
        SELECT lower(idempotency_key) AS k, url_id, title, brand_normalized, price, sell_price, discount, sold,
               product_rate, product_rate_cnt, seller_rate, seller_rate_cnt, observed_at
        FROM bronze_product_snapshot
        WHERE url_id % 20 = 0 OR url_id < 1000000
    ) AS b ON b.k = r.k
    GROUP BY scope
)
ARRAY JOIN [
    ('url_id',           f_url_id,           0, 0,   s_url_id,           'Spark ép url_id sang int 32-bit'),
    ('price',            f_price,            0, 0.1, s_price,            'normalize_price'),
    ('sell_price',       f_sell_price,       0, 0.1, s_sell_price,       'normalize_price ; "Liên hệ" / trống -> 0'),
    ('discount',         f_discount,         0, 0.1, s_discount,         'normalize_discount'),
    ('sold',             f_sold,             0, 0.1, s_sold,             'normalize_sold chỉ ghép chữ số: "1k" thành 1'),
    ('product_rate',     f_product_rate,     0, 0.1, s_product_rate,     'normalize_rating'),
    ('product_rate_cnt', f_product_rate_cnt, 0, 0.1, s_product_rate_cnt, 'normalize_rating_count'),
    ('seller_rate',      f_seller_rate,      0, 0.1, s_seller_rate,      'normalize_rating'),
    ('seller_rate_cnt',  f_seller_rate_cnt,  0, 0.1, s_seller_rate_cnt,  'normalize_rating_count'),
    ('observed_at',      f_observed_at,      0, 0,   s_observed_at,      'lệch > 1 giây: lỗi ép kiểu / múi giờ'),
    ('title',            f_title,            0, 0.1, s_title,            'normalize_title xóa hết ký tự'),
    ('brand_normalized', f_brand_normalized, 0, 0,   s_brand_normalized, 'normalize_brand trả NULL dù brand có chữ')
] AS c;

-- B02 cho review: đủ nhỏ (~500 nghìn dòng) để so toàn bộ.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_user_review', 'B02', 'Giá trị bronze lệch giá trị kỳ vọng tính lại từ raw',
    'accuracy', c.1, scope, total, c.2, dq_pct(c.2, total), '%', c.3, c.4, c.5, c.6
FROM (
    SELECT
        dq_scope(b.url_id) AS scope,
        count() AS total,
        countIf(toInt64OrZero(r.url_id) != b.url_id) AS f_url_id,
        countIf(abs(b.joined_duration - e_joined) > 1) AS f_joined,
        countIf(abs(b.rated_at - e_rated) > 0.01 + 1e-6 * e_rated) AS f_rated,
        countIf(abs(b.use_time - e_use) > 0.01 + 1e-6 * e_use) AS f_use,
        countIf(abs(b.reviewed - dq_expected_count(r.reviewed)) >= 1) AS f_reviewed,
        countIf(abs(b.thank_received - dq_expected_count(r.thank_received)) >= 1) AS f_thank,
        countIf(abs(b.star_score - toFloat64OrZero(extract(r.star_score, '([0-9]+)'))) > 0.001) AS f_star,
        countIf(e_observed_at IS NULL OR abs(dateDiff('second', b.observed_at, assumeNotNull(e_observed_at))) > 1) AS f_observed_at,
        countIf(match(r.comment, '[\\p{L}\\p{N}]') AND b.comment = '') AS f_comment,
        anyIf(concat(r.joined_duration, ' -> ', toString(b.joined_duration)), abs(b.joined_duration - e_joined) > 1) AS s_joined,
        anyIf(concat(r.rated_at, ' -> ', toString(b.rated_at)), abs(b.rated_at - e_rated) > 0.01 + 1e-6 * e_rated) AS s_rated,
        anyIf(concat(r.use_time, ' -> ', toString(b.use_time)), abs(b.use_time - e_use) > 0.01 + 1e-6 * e_use) AS s_use,
        anyIf(concat(r.reviewed, ' -> ', toString(b.reviewed)), abs(b.reviewed - dq_expected_count(r.reviewed)) >= 1) AS s_reviewed,
        anyIf(concat(r.thank_received, ' -> ', toString(b.thank_received)),
              abs(b.thank_received - dq_expected_count(r.thank_received)) >= 1) AS s_thank,
        anyIf(concat(r.star_score, ' -> ', toString(b.star_score)),
              abs(b.star_score - toFloat64OrZero(extract(r.star_score, '([0-9]+)'))) > 0.001) AS s_star,
        anyIf(concat(r.observed_at, ' -> ', toString(b.observed_at)),
              e_observed_at IS NULL OR abs(dateDiff('second', b.observed_at, assumeNotNull(e_observed_at))) > 1) AS s_observed_at,
        anyIf(substring(r.comment, 1, 60), match(r.comment, '[\\p{L}\\p{N}]') AND b.comment = '') AS s_comment
    FROM (
        SELECT lower(idempotency_key) AS k, url_id, joined_duration, rated_at, use_time, reviewed, thank_received,
               star_score, comment, observed_at,
               ifNull(dq_hours(joined_duration), 0) AS e_joined,
               ifNull(dq_hours(rated_at), 0) AS e_rated,
               ifNull(dq_hours(use_time), 0) AS e_use,
               toDateTime(dq_ts(observed_at), 'Asia/Ho_Chi_Minh') AS e_observed_at
        FROM raw_user_review
        LIMIT 1 BY k
    ) AS r
    INNER JOIN (
        SELECT lower(idempotency_key) AS k, url_id, joined_duration, rated_at, use_time, reviewed, thank_received,
               star_score, comment, observed_at
        FROM bronze_user_review
    ) AS b ON b.k = r.k
    GROUP BY scope
)
ARRAY JOIN [
    ('url_id',          f_url_id,      0, 0,   '',            ''),
    ('joined_duration', f_joined,      0, 0.1, s_joined,      'to_hours, lưu Int32 nên cho lệch 1 giờ'),
    ('rated_at',        f_rated,       0, 0.1, s_rated,       'to_hours'),
    ('use_time',        f_use,         0, 0.1, s_use,         'to_hours ; trống -> 0'),
    ('reviewed',        f_reviewed,    0, 0.1, s_reviewed,    'parse_count'),
    ('thank_received',  f_thank,       0, 0.1, s_thank,       'parse_count'),
    ('star_score',      f_star,        0, 0,   s_star,        'parse_star_score'),
    ('observed_at',     f_observed_at, 0, 0,   s_observed_at, 'lệch > 1 giây: lỗi ép kiểu / múi giờ'),
    ('comment',         f_comment,     0, 1,   s_comment,     'clean_comment xóa hết nội dung (comment chỉ có emoji / ký tự đặc biệt)')
] AS c;



-- ---------------------------------------------------------------- B03 miền giá trị (sản phẩm)
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', 'B03', c.1, 'validity', c.2, scope, total, c.3,
    dq_pct(c.3, total), '%', c.4, c.5, dq_sample(c.3, c.6), ''
FROM (
    SELECT
        dq_scope(url_id) AS scope,
        count() AS total,
        countIf(url_id <= 0) AS f_url,
        countIf(price < 0 OR sell_price < 0) AS f_neg_price,
        countIf(sold < 0 OR product_rate_cnt < 0 OR seller_rate_cnt < 0) AS f_neg_cnt,
        countIf(discount < 0 OR discount > 100) AS f_discount,
        countIf(product_rate < 0 OR product_rate > 5) AS f_prate,
        countIf(seller_rate < 0 OR seller_rate > 5) AS f_srate,
        countIf(sell_price > 1e10 OR price > 1e10) AS f_huge,
        countIf(observed_at < toDateTime('2020-01-01 00:00:00', 'Asia/Ho_Chi_Minh') OR observed_at > now() + INTERVAL 1 HOUR) AS f_time,
        countIf(ingested_at < observed_at) AS f_ingest,
        toString(anyIf(url_id, url_id <= 0)) AS s_url,
        toString(anyIf(tuple(price, sell_price), price < 0 OR sell_price < 0)) AS s_neg_price,
        toString(anyIf(tuple(sold, product_rate_cnt, seller_rate_cnt), sold < 0 OR product_rate_cnt < 0 OR seller_rate_cnt < 0)) AS s_neg_cnt,
        toString(anyIf(discount, discount < 0 OR discount > 100)) AS s_discount,
        toString(anyIf(product_rate, product_rate < 0 OR product_rate > 5)) AS s_prate,
        toString(anyIf(seller_rate, seller_rate < 0 OR seller_rate > 5)) AS s_srate,
        toString(anyIf(tuple(url_id, sell_price), sell_price > 1e10 OR price > 1e10)) AS s_huge,
        toString(anyIf(observed_at, observed_at < toDateTime('2020-01-01 00:00:00', 'Asia/Ho_Chi_Minh') OR observed_at > now() + INTERVAL 1 HOUR)) AS s_time,
        toString(anyIf(tuple(observed_at, ingested_at), ingested_at < observed_at)) AS s_ingest
    FROM bronze_product_snapshot
    GROUP BY scope
)
ARRAY JOIN [
    ('url_id <= 0',                                   'url_id',       f_url,       0, 0,    s_url),
    ('Giá âm',                                        'price',        f_neg_price, 0, 0,    s_neg_price),
    ('Bộ đếm âm (sold, *_rate_cnt)',                  'sold',         f_neg_cnt,   0, 0,    s_neg_cnt),
    ('% giảm giá ngoài [0, 100]',                     'discount',     f_discount,  0, 0,    s_discount),
    ('product_rate ngoài [0, 5]',                     'product_rate', f_prate,     0, 0,    s_prate),
    ('seller_rate ngoài [0, 5]',                      'seller_rate',  f_srate,     0, 0,    s_srate),
    ('Giá > 10 tỷ đồng (vô lý)',                      'sell_price',   f_huge,      0, 0.01, s_huge),
    ('observed_at trước 2020 hoặc ở tương lai',       'observed_at',  f_time,      0, 0,    s_time),
    ('ingested_at (lúc nạp bronze) sớm hơn observed_at', 'ingested_at', f_ingest,  0, 0,    s_ingest)
] AS c;



-- ---------------------------------------------------------------- B04 nhất quán nghiệp vụ (sản phẩm)
-- % giảm Tiki hiển thị đã làm tròn tới số nguyên -> cho lệch 1 điểm %.
-- price = 0 nghĩa là nguồn không hiện giá gốc (không giảm giá), khi đó discount phải = 0.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', c.1, c.2, c.3, c.4, scope, c.5, c.6,
    dq_pct(c.6, c.5), '%', c.7, c.8, dq_sample(c.6, c.9), c.10
FROM (
    SELECT
        dq_scope(url_id) AS scope,
        count() AS total,
        countIf(price > 0) AS n_has_price,
        countIf(price > 0 AND sell_price > price) AS f_sell_gt_list,
        countIf(price > 0 OR discount > 0) AS n_discount,
        countIf((price > 0 AND sell_price > 0 AND abs((price - sell_price) / price * 100 - discount) > 1)
                OR (discount > 0 AND price = 0)) AS f_discount,
        countIf((product_rate > 0) != (product_rate_cnt > 0)) AS f_prate,
        countIf((seller_rate > 0) != (seller_rate_cnt > 0)) AS f_srate,
        countIf(sell_price = 0) AS f_zero_price,
        toString(anyIf(tuple(url_id, price, sell_price), price > 0 AND sell_price > price)) AS s_sell_gt_list,
        toString(anyIf(tuple(url_id, price, sell_price, discount),
            (price > 0 AND sell_price > 0 AND abs((price - sell_price) / price * 100 - discount) > 1)
            OR (discount > 0 AND price = 0))) AS s_discount,
        toString(anyIf(tuple(url_id, product_rate, product_rate_cnt), (product_rate > 0) != (product_rate_cnt > 0))) AS s_prate,
        toString(anyIf(tuple(url_id, seller, seller_rate, seller_rate_cnt), (seller_rate > 0) != (seller_rate_cnt > 0))) AS s_srate,
        toString(anyIf(url_id, sell_price = 0)) AS s_zero_price
    FROM bronze_product_snapshot
    GROUP BY scope
)
ARRAY JOIN [
    ('B04a', 'Giá bán lớn hơn giá gốc', 'consistency', 'sell_price', n_has_price, f_sell_gt_list, 0, 0.1, s_sell_gt_list,
        'tính trên các dòng có giá gốc'),
    ('B04b', '% giảm không khớp (giá gốc - giá bán) / giá gốc (lệch > 1 điểm %)', 'consistency', 'discount',
        n_discount, f_discount, 0.1, 1, s_discount, 'tính trên các dòng có giá gốc hoặc có % giảm'),
    ('B04c', 'product_rate và product_rate_cnt mâu thuẫn (một bên 0, một bên > 0)', 'consistency', 'product_rate',
        total, f_prate, 0.1, 1, s_prate, ''),
    ('B04d', 'seller_rate và seller_rate_cnt mâu thuẫn (một bên 0, một bên > 0)', 'consistency', 'seller_rate',
        total, f_srate, 0.1, 1, s_srate, ''),
    ('B04e', 'sell_price = 0 sau chuẩn hóa (nguồn trống / "Liên hệ" / undefined)', 'completeness', 'sell_price',
        total, f_zero_price, 0.5, 2, s_zero_price, 'gold coi sell_price = 0 là thiếu giá ; tạo "đổi giá" giả ở F3')
] AS c;



-- ---------------------------------------------------------------- B05 thiếu brand / seller / category
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', 'B05', c.1, c.2, c.3, scope, total, c.4,
    dq_pct(c.4, total), '%', c.5, c.6, dq_sample(c.4, c.7), c.8
FROM (
    SELECT
        dq_scope(url_id) AS scope,
        count() AS total,
        countIf(brand = '') AS f_no_brand,
        countIf(match(brand, '[\\p{L}\\p{N}]') AND brand_normalized IS NULL) AS f_brand_lost,
        countIf(brand_normalized IN ('oem', 'nobrand', 'noname', 'khongthuonghieu', 'generic', 'unbranded', 'khac', 'other', 'none')) AS f_unbranded,
        countIf(seller = '') AS f_no_seller,
        countIf(category_tree = '') AS f_no_cat,
        anyIf(brand, match(brand, '[\\p{L}\\p{N}]') AND brand_normalized IS NULL) AS s_brand_lost,
        toString(topKIf(3)(brand_normalized, brand_normalized IN ('oem', 'nobrand', 'noname', 'khongthuonghieu', 'generic', 'unbranded', 'khac', 'other', 'none'))) AS s_unbranded
    FROM bronze_product_snapshot
    GROUP BY scope
)
ARRAY JOIN [
    ('Thiếu brand',                                         'completeness', 'brand',            f_no_brand,   5, 20,  '',           'gold gán brand "unknown"'),
    ('brand có chữ nhưng brand_normalized NULL',            'validity',     'brand_normalized', f_brand_lost, 0, 0,   s_brand_lost, ''),
    ('Hàng không thương hiệu (OEM / No Brand ...)',         'completeness', 'brand_normalized', f_unbranded,  0, -1,  s_unbranded,  'không phải lỗi, nhưng không nên coi là 1 thương hiệu khi phân tích thị phần'),
    ('Thiếu seller',                                        'completeness', 'seller',           f_no_seller,  5, 20,  '',           'gold gán seller "unknown"'),
    ('Thiếu category_tree',                                 'completeness', 'category_tree',    f_no_cat,     0.5, 2, '',           '')
] AS c;



-- ---------------------------------------------------------------- B06 entity resolution (brand, seller)
-- Đo ở mức GIÁ TRỊ KHÁC NHAU (vài nghìn brand / seller), không phải mức dòng.
--   B06a brand_normalized gộp được >= 2 cách viết: bằng chứng chuẩn hóa có tác dụng (INFO).
--   B06b khóa chuẩn hóa quá ngắn / toàn số: dấu hiệu chuẩn hóa xóa quá tay (2 brand khác nhau dễ bị gộp nhầm).
--   B06c cặp khóa nghi là cùng 1 brand nhưng chưa gộp -> INFO kèm mẫu để người xem lại:
--        typo  : Damerau-Levenshtein = 1 (khóa >= 5 ký tự) VÀ khóa hiếm có <= 5% số dòng của khóa phổ biến
--                (lỗi chính tả thường hiếm so với cách viết đúng ; chỉ dùng khoảng cách sửa thì tên ngắn
--                 khác nhau 1 ký tự như 'hosion' / 'rosion' bị bắt nhầm hàng loạt)
--        hậu tố: khóa này = khóa kia + 'store' / 'shop' / 'vietnam' / 'mall' ...
--   B06d tên seller khác nhau nhưng trùng khóa sau khi bỏ dấu / hậu tố official: gold gộp seller chỉ theo tên
--        (đã lower) nên những tên này vẫn bị tách thành nhiều seller.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', 'B06a', 'Brand gộp được nhiều cách viết về 1 brand_normalized',
    'consistency', 'brand_normalized', 'all', count() AS keys, countIf(spellings > 1) AS merged, dq_pct(merged, keys), '%', 0, -1,
    arrayStringConcat(arrayMap(x -> concat(x.1, ' <- ', arrayStringConcat(x.2, ' | ')),
        arraySlice(arrayReverseSort(x -> length(x.2), groupArrayIf((k, names), spellings > 1)), 1, 3)), ' ; '),
    concat(toString(sum(spellings)), ' tên gốc -> ', toString(keys), ' brand')
FROM (
    SELECT brand_normalized AS k, uniqExact(brand) AS spellings, groupUniqArray(5)(brand) AS names
    FROM bronze_product_snapshot
    WHERE brand_normalized IS NOT NULL
    GROUP BY k
);

INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', 'B06b', 'brand_normalized quá ngắn (<= 2 ký tự) hoặc toàn chữ số',
    'consistency', 'brand_normalized', 'all', count() AS keys,
    countIf(lengthUTF8(k) <= 2 OR match(k, '^[0-9]+$')) AS bad, dq_pct(bad, keys), '%', 0, 101,
    arrayStringConcat(arraySlice(groupArrayIf(concat(k, ' <- ', name), lengthUTF8(k) <= 2 OR match(k, '^[0-9]+$')), 1, 5), ' ; '),
    'không chắc là lỗi (vd "lg", "3m") nhưng là nơi dễ gộp nhầm 2 brand'
FROM (
    SELECT brand_normalized AS k, any(brand) AS name
    FROM bronze_product_snapshot
    WHERE brand_normalized IS NOT NULL
    GROUP BY k
);

INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
WITH keys AS (
    SELECT brand_normalized AS k, count() AS n
    FROM bronze_product_snapshot
    WHERE brand_normalized IS NOT NULL
    GROUP BY k
)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', 'B06c',
    'Cặp brand_normalized nghi cùng 1 brand nhưng chưa gộp (lỗi chính tả hiếm / thêm hậu tố)', 'consistency',
    'brand_normalized', 'all',
    (SELECT count() FROM keys) AS n_keys, count() AS pairs, dq_pct(pairs, n_keys), '%', 0, -1,
    arrayStringConcat(arraySlice(groupArray(concat(a, ' (', toString(na), ' dòng) ~ ', b, ' (', toString(nb), ' dòng)')), 1, 5), ' ; '),
    concat('typo: ', toString(countIf(rule = 'typo')), ' cặp ; hậu tố: ', toString(countIf(rule = 'suffix')),
           ' cặp ; failed_rows = số cặp ứng viên, cần người xem lại rồi thêm vào bảng alias')
FROM (
    SELECT a.k AS a, b.k AS b, a.n AS na, b.n AS nb,
           multiIf(
               lengthUTF8(a.k) >= 5 AND damerauLevenshteinDistance(a.k, b.k) = 1
                   AND least(a.n, b.n) <= 0.05 * greatest(a.n, b.n), 'typo',
               startsWith(b.k, a.k) AND lengthUTF8(a.k) >= 3
                   AND substring(b.k, length(a.k) + 1) IN ('store', 'shop', 'vietnam', 'vn', 'mall', 'group', 'brand',
                       'global', 'home', 'house', 'japan', 'korea', 'usa', 'beauty', 'cosmetics', 'fashion', 'kids',
                       'baby', 'sport', 'sports', 'plus', 'pro'), 'suffix',
               '') AS rule
    FROM keys AS a
    CROSS JOIN keys AS b
    WHERE a.k < b.k
)
WHERE rule != '';

INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', 'B06d',
    'Tên seller khác nhau nhưng trùng khóa chuẩn hóa (seller chưa được gộp)', 'consistency', 'seller', 'all',
    sum(length(names)) AS seller_names, sumIf(length(names), length(names) > 1) AS unmerged,
    dq_pct(unmerged, seller_names), '%', 0, 5,
    arrayStringConcat(arrayMap(x -> arrayStringConcat(x, ' | '),
        arraySlice(arrayReverseSort(x -> length(x), groupArrayIf(names, length(names) > 1)), 1, 3)), ' ; '),
    concat(toString(countIf(length(names) > 1)), ' nhóm ; seller chỉ được lower ở bronze, gold gộp theo tên nên các tên này vẫn tách rời')
FROM (
    SELECT dq_name_key(seller) AS k, groupUniqArray(seller) AS names
    FROM (SELECT DISTINCT seller FROM bronze_product_snapshot WHERE seller != '')
    WHERE k != ''
    GROUP BY k
);



-- ---------------------------------------------------------------- B07 cây danh mục
-- Chỉ có vài trăm cây khác nhau -> phân tích trên giá trị khác nhau, cân theo số dòng.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', c.1, c.2, 'validity', 'category_tree', 'all', c.3, c.4,
    dq_pct(c.4, c.3), '%', c.5, c.6, c.7, c.8
FROM (
    SELECT
        sum(n) AS total_rows,
        sumIf(n, depth < 2 OR depth > 6) AS f_depth,
        sumIf(n, has(levels, '')) AS f_empty_level,
        count() AS trees,
        countIf(depth < 2 OR depth > 6) AS trees_depth,
        anyIf(category_tree, depth < 2 OR depth > 6) AS s_depth,
        anyIf(category_tree, has(levels, '')) AS s_empty_level,
        toString(sumMap([toUInt64(depth)], [n])) AS depth_hist
    FROM (
        SELECT category_tree, count() AS n,
               arrayMap(x -> trimBoth(x), splitByChar('>', category_tree)) AS levels,
               length(levels) AS depth
        FROM bronze_product_snapshot
        WHERE category_tree != ''
        GROUP BY category_tree
    )
)
ARRAY JOIN [
    ('B07a', 'Độ sâu cây danh mục ngoài [2, 6]', total_rows, f_depth, 0.1, 1, s_depth,
        concat(toString(trees_depth), '/', toString(trees), ' cây ; silver chỉ có 6 cột cấp ; (độ sâu, số dòng) = ', depth_hist)),
    ('B07b', 'Cây danh mục có cấp rỗng ("a >  > b", ">" thừa ở đầu / cuối)', total_rows, f_empty_level, 0, 0.1, s_empty_level, '')
] AS c;

INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', 'B07c', 'Cùng tên leaf nhưng thuộc nhiều nhánh cha khác nhau',
    'consistency', 'category_tree', 'all', count() AS leaves, countIf(parents > 1) AS f, dq_pct(f, leaves), '%', 0, -1,
    arrayStringConcat(arraySlice(groupArrayIf(concat(leaf, ': ', arrayStringConcat(paths, ' | ')), parents > 1), 1, 2), ' ; '),
    'không hẳn là lỗi (vd leaf "khác"), nhưng phân tích theo leaf phải kèm nhánh cha'
FROM (
    SELECT levels[-1] AS leaf, uniqExact(arrayStringConcat(arraySlice(levels, 1, -1), ' > ')) AS parents,
           groupUniqArray(3)(arrayStringConcat(arraySlice(levels, 1, -1), ' > ')) AS paths
    FROM (
        SELECT DISTINCT arrayMap(x -> trimBoth(x), splitByChar('>', category_tree)) AS levels
        FROM bronze_product_snapshot
        WHERE category_tree != ''
    )
    GROUP BY leaf
);



-- ---------------------------------------------------------------- B08 + B09a nhất quán theo thời gian (từng url_id)
-- So mỗi lần crawl với lần trước của CÙNG sản phẩm (window theo url_id, thứ tự observed_at).
-- Trong window chỉ mang số và hash (không mang chuỗi seller / category) để sort 10 triệu dòng vừa RAM.
--   sold, product_rate_cnt là bộ đếm LŨY KẾ -> không được giảm. Tách 2 loại:
--     tụt về 0  : ô "đã bán" trống bị bronze đổi thành 0 (lỗi nguồn / selector, gold 08 đã chặn bằng max lũy kế)
--     giảm thật : Tiki reset / làm tròn số, hoặc crawl nhầm trang
--   seller chập chờn: 1 trong 2 lần liền nhau seller trống -> phải điền theo giá trị gần nhất trước khi gán khóa
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', c.1, c.2, c.3, c.4, scope, transitions, c.5,
    dq_pct(c.5, transitions), '%', c.6, c.7, c.8, concat(c.9, ' ; mẫu số = số cặp lần crawl liền nhau')
FROM (
    SELECT
        scope,
        count() AS transitions,
        countIf(prev_sold > sold AND sold = 0) AS f_sold_zero,
        countIf(prev_sold > sold AND sold > 0) AS f_sold_down,
        countIf(prev_rc > product_rate_cnt AND product_rate_cnt = 0) AS f_rc_zero,
        countIf(prev_rc > product_rate_cnt AND product_rate_cnt > 0) AS f_rc_down,
        countIf(seller_h != prev_seller_h AND (seller_empty OR prev_seller_empty)) AS f_seller_blank,
        countIf(seller_h != prev_seller_h AND NOT seller_empty AND NOT prev_seller_empty) AS f_seller_change,
        countIf(cat_h != prev_cat_h AND NOT cat_empty AND NOT prev_cat_empty) AS f_cat_change,
        countIf(brand_h != prev_brand_h AND NOT brand_null AND NOT prev_brand_null) AS f_brand_change,
        countIf(prev_price > 0 AND sell_price > 0 AND (sell_price > 1.8 * prev_price OR sell_price < 0.2 * prev_price)) AS f_jump,
        uniqExactIf(url_id, prev_sold > sold AND sold > 0) AS p_sold_down,
        uniqExactIf(url_id, seller_h != prev_seller_h AND (seller_empty OR prev_seller_empty)) AS p_seller_blank,
        uniqExactIf(url_id, seller_h != prev_seller_h AND NOT seller_empty AND NOT prev_seller_empty) AS p_seller_change,
        anyIf(concat(toString(url_id), ': ', toString(prev_sold), ' -> 0'), prev_sold > sold AND sold = 0) AS s_sold_zero,
        anyIf(concat(toString(url_id), ': ', toString(prev_sold), ' -> ', toString(sold)), prev_sold > sold AND sold > 0) AS s_sold_down,
        anyIf(concat(toString(url_id), ': ', toString(prev_rc), ' -> 0'), prev_rc > product_rate_cnt AND product_rate_cnt = 0) AS s_rc_zero,
        anyIf(concat(toString(url_id), ': ', toString(prev_rc), ' -> ', toString(product_rate_cnt)),
              prev_rc > product_rate_cnt AND product_rate_cnt > 0) AS s_rc_down,
        anyIf(concat(toString(url_id), ' @ ', toString(observed_at)), seller_h != prev_seller_h AND (seller_empty OR prev_seller_empty)) AS s_seller_blank,
        anyIf(concat(toString(url_id), ' @ ', toString(observed_at)), seller_h != prev_seller_h AND NOT seller_empty AND NOT prev_seller_empty) AS s_seller_change,
        anyIf(concat(toString(url_id), ' @ ', toString(observed_at)), cat_h != prev_cat_h AND NOT cat_empty AND NOT prev_cat_empty) AS s_cat_change,
        anyIf(concat(toString(url_id), ' @ ', toString(observed_at)), brand_h != prev_brand_h AND NOT brand_null AND NOT prev_brand_null) AS s_brand_change,
        anyIf(concat(toString(url_id), ': ', toString(prev_price), ' -> ', toString(sell_price)),
              prev_price > 0 AND sell_price > 0 AND (sell_price > 1.8 * prev_price OR sell_price < 0.2 * prev_price)) AS s_jump
    FROM (
        SELECT
            dq_scope(url_id) AS scope, url_id, observed_at, sold, product_rate_cnt, sell_price,
            seller_h, seller_empty, cat_h, cat_empty, brand_h, brand_null,
            lagInFrame(toNullable(sold)) OVER w AS prev_sold,
            lagInFrame(toNullable(product_rate_cnt)) OVER w AS prev_rc,
            lagInFrame(toNullable(sell_price)) OVER w AS prev_price,
            lagInFrame(toNullable(seller_h)) OVER w AS prev_seller_h,
            lagInFrame(toNullable(seller_empty)) OVER w AS prev_seller_empty,
            lagInFrame(toNullable(cat_h)) OVER w AS prev_cat_h,
            lagInFrame(toNullable(cat_empty)) OVER w AS prev_cat_empty,
            lagInFrame(toNullable(brand_h)) OVER w AS prev_brand_h,
            lagInFrame(toNullable(brand_null)) OVER w AS prev_brand_null
        FROM (
            SELECT url_id, observed_at, sold, product_rate_cnt, sell_price,
                   cityHash64(seller) AS seller_h, seller = '' AS seller_empty,
                   cityHash64(category_tree) AS cat_h, category_tree = '' AS cat_empty,
                   cityHash64(ifNull(brand_normalized, '')) AS brand_h, brand_normalized IS NULL AS brand_null
            FROM bronze_product_snapshot
        )
        WINDOW w AS (PARTITION BY url_id ORDER BY observed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    )
    WHERE prev_sold IS NOT NULL
    GROUP BY scope
)
ARRAY JOIN [
    ('B08a', 'sold tụt về 0 giữa 2 lần crawl (ô "đã bán" trống)', 'consistency', 'sold', f_sold_zero, 0.1, 2, s_sold_zero,
        'gold 08 dùng max lũy kế nên không bị ảnh hưởng ; F1 vẫn giữ giá trị thô'),
    ('B08b', 'sold giảm thật (bộ đếm lũy kế không được giảm)', 'consistency', 'sold', f_sold_down, 0.05, 1, s_sold_down,
        concat(toString(p_sold_down), ' sản phẩm')),
    ('B08c', 'product_rate_cnt tụt về 0 giữa 2 lần crawl', 'consistency', 'product_rate_cnt', f_rc_zero, 0.1, 2, s_rc_zero, ''),
    ('B08d', 'product_rate_cnt giảm thật', 'consistency', 'product_rate_cnt', f_rc_down, 0.05, 1, s_rc_down, ''),
    ('B08e', 'seller chập chờn: 1 trong 2 lần crawl liền nhau bị trống', 'consistency', 'seller', f_seller_blank, 0.5, 2,
        s_seller_blank, concat(toString(p_seller_blank), ' sản phẩm')),
    ('B08f', 'seller đổi giữa 2 lần crawl (cả 2 lần đều có tên)', 'consistency', 'seller', f_seller_change, 0, -1,
        s_seller_change, concat(toString(p_seller_change), ' sản phẩm ; có thể hợp lệ (Tiki đổi người bán hiển thị)')),
    ('B08g', 'category_tree đổi giữa 2 lần crawl', 'consistency', 'category_tree', f_cat_change, 0.01, 1, s_cat_change, ''),
    ('B08h', 'brand_normalized đổi giữa 2 lần crawl', 'consistency', 'brand_normalized', f_brand_change, 0.01, 1, s_brand_change, ''),
    ('B09a', 'sell_price nhảy quá +80% / -80% giữa 2 lần crawl', 'validity', 'sell_price', f_jump, 0.1, 1, s_jump,
        'không tính lần giá = 0 (thiếu giá, đã đo ở B04e)')
] AS c;



-- ---------------------------------------------------------------- B09b ngoại lai giá trong danh mục cấp 1
-- Giá phân phối lệch phải rất mạnh (vài nghìn -> vài chục triệu) nên IQR tính trên log10(giá).
-- Rào x3 IQR (Tukey "far out"): ngoài rào là giá rất bất thường so với cùng danh mục cấp 1.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', 'B09b', 'sell_price ngoài rào IQR x3 (thang log) của danh mục cấp 1',
    'validity', 'sell_price', dq_scope(s.url_id) AS scope, count() AS total,
    countIf(s.lp < q.lo OR s.lp > q.hi) AS f, dq_pct(f, total), '%', 1, 5,
    dq_sample(f, anyIf(tuple(s.url_id, s.l1, s.sell_price), s.lp < q.lo OR s.lp > q.hi)),
    'chỉ xét sell_price > 0'
FROM (
    SELECT url_id, trimBoth(splitByChar('>', category_tree)[1]) AS l1, sell_price, log10(sell_price) AS lp
    FROM bronze_product_snapshot
    WHERE sell_price > 0 AND category_tree != ''
) AS s
INNER JOIN (
    SELECT l1, qs[1] - 3 * (qs[2] - qs[1]) AS lo, qs[2] + 3 * (qs[2] - qs[1]) AS hi
    FROM (
        SELECT trimBoth(splitByChar('>', category_tree)[1]) AS l1, quantilesExact(0.25, 0.75)(log10(sell_price)) AS qs
        FROM bronze_product_snapshot
        WHERE sell_price > 0 AND category_tree != ''
        GROUP BY l1
    )
) AS q ON q.l1 = s.l1
GROUP BY scope;



-- ---------------------------------------------------------------- B10 review
-- rated_at, joined_duration đều là "số giờ tính đến lúc crawl" và chỉ chính xác tới đơn vị lớn nhất
-- ('4 năm'), nên review cũ hơn tài khoản chỉ bị tính khi lệch quá 1 năm (8760 giờ).
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_user_review', c.1, c.2, c.3, c.4, scope, total, c.5, dq_pct(c.5, total), '%',
    c.6, c.7, dq_sample(c.5, c.8), c.9
FROM (
    SELECT
        dq_scope(url_id) AS scope,
        count() AS total,
        countIf(star_score < 1 OR star_score > 5) AS f_star,
        countIf(joined_duration <= 0) AS f_joined,
        countIf(rated_at <= 0) AS f_rated,
        countIf(rated_at > joined_duration + 8760 AND joined_duration > 0) AS f_older,
        countIf(user_name = '') AS f_user,
        countIf(reviewed < 0 OR thank_received < 0) AS f_neg,
        toString(anyIf(tuple(url_id, star_score), star_score < 1 OR star_score > 5)) AS s_star,
        toString(anyIf(tuple(url_id, user_name), joined_duration <= 0)) AS s_joined,
        toString(anyIf(tuple(url_id, user_name), rated_at <= 0)) AS s_rated,
        toString(anyIf(tuple(url_id, user_name, rated_at, joined_duration), rated_at > joined_duration + 8760 AND joined_duration > 0)) AS s_older,
        toString(anyIf(url_id, user_name = '')) AS s_user
    FROM bronze_user_review
    GROUP BY scope
)
ARRAY JOIN [
    ('B10a', 'star_score ngoài 1..5 (0 = không có sao / parse lỗi)', 'validity', 'star_score', f_star, 0, 0.5, s_star, ''),
    ('B10b', 'joined_duration <= 0 (không parse được ngày tham gia)', 'completeness', 'joined_duration', f_joined, 0.5, 2, s_joined, ''),
    ('B10c', 'rated_at <= 0 (không biết review viết lúc nào)', 'completeness', 'rated_at', f_rated, 1, 5, s_rated,
        'gold đặt review_date_key = 0'),
    ('B10d', 'Review cũ hơn tài khoản quá 1 năm (rated_at > joined_duration)', 'consistency', 'rated_at', f_older, 0.5, 2, s_older, ''),
    ('B10e', 'user_name rỗng', 'completeness', 'user_name', f_user, 0.5, 2, s_user, 'gold gán user "unknown"'),
    ('B10f', 'reviewed / thank_received âm', 'validity', 'reviewed', f_neg, 0, 0, '', '')
] AS c;
