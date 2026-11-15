-- =====================================================================================
-- DATA QUALITY - TẦNG SILVER (silver_snapshot, silver_product, silver_brand, silver_seller, silver_category)
-- Silver là nơi dữ liệu được tách thành dimension có surrogate key (hash thuộc tính) + bảng snapshot.
-- Đo: khóa có duy nhất không, SCD2 có đúng không, snapshot có trỏ đúng vào dimension không, và silver có
-- giữ đủ / đúng dữ liệu của bronze không. Gold đọc silver nên mọi lỗi ở đây lan thẳng lên gold.
--
--   S01 uniqueness      SK duy nhất trong từng dimension ; (url_id, observed_at) duy nhất trong silver_snapshot
--   S02 consistency     SCD2 silver_product: mỗi url_id đúng 1 version is_current ; is_current <-> valid_to IS NULL
--   S03 integrity       SK trong silver_snapshot / silver_product không có trong dimension tương ứng (orphan)
--   S04 reconciliation  bronze -> silver_snapshot: khóa thiếu / thừa, bảo toàn tổng (giá, đã bán, lượt đánh giá) ;
--                       số dòng dimension = số tổ hợp thuộc tính khác nhau ở bronze
--   S05 consistency     Phân mảnh entity: số dòng silver trên 1 thực thể (seller theo tên, brand theo
--                       brand_normalized) -> bằng chứng cho việc gold phải gộp lại
--   S06 validity        silver_category: cấp rỗng xen giữa, danh mục rỗng ; silver_snapshot.url_id = 0
--
-- Chạy: bash run_dq.sh silver
-- =====================================================================================



-- ---------------------------------------------------------------- S01 khóa duy nhất
-- Loader silver chỉ chèn SK chưa có, nhưng dùng rank() = 1: 2 dòng bronze cùng SK và cùng observed_at
-- (hòa hạng) sẽ chèn 2 lần. silver_snapshot append toàn bộ bronze -> chạy lại không TRUNCATE sẽ nhân đôi.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'silver', tbl, 'S01', 'Khóa trùng (dòng thừa = số dòng - số khóa khác nhau)', 'uniqueness',
    col, 'all', total, dup, dq_pct(dup, total), '%', 0, 0, dq_sample(dup, sample), note
FROM (
    SELECT 'silver_product' AS tbl, 'product_sk' AS col, total, toUInt64(total - keys) AS dup, sample,
           'rank() = 1 bị hòa khi 2 dòng bronze cùng SK và cùng observed_at (giây) -> nên dùng row_number()' AS note
    FROM (SELECT sum(c) AS total, count() AS keys, toString(anyIf((k, c, vf), c > 1)) AS sample
          FROM (SELECT product_sk AS k, count() AS c, any(valid_from) AS vf FROM silver_product GROUP BY k))
    UNION ALL
    SELECT 'silver_category', 'category_sk', total, toUInt64(total - keys), sample,
           'rank() = 1 bị hòa khi 2 dòng bronze cùng SK và cùng observed_at (giây) -> nên dùng row_number()'
    FROM (SELECT sum(c) AS total, count() AS keys, toString(anyIf((k, c, vf), c > 1)) AS sample
          FROM (SELECT category_sk AS k, count() AS c, any(valid_from) AS vf FROM silver_category GROUP BY k))
    UNION ALL
    SELECT 'silver_brand', 'brand_sk', total, toUInt64(total - keys), sample,
           'rank() = 1 bị hòa khi 2 dòng bronze cùng SK và cùng observed_at (giây) -> nên dùng row_number()'
    FROM (SELECT sum(c) AS total, count() AS keys, toString(anyIf((k, c, vf), c > 1)) AS sample
          FROM (SELECT brand_sk, any(name) AS k, count() AS c, any(valid_from) AS vf FROM silver_brand GROUP BY brand_sk))
    UNION ALL
    SELECT 'silver_seller', 'seller_sk', total, toUInt64(total - keys), sample,
           'rank() = 1 bị hòa khi 2 dòng bronze cùng SK và cùng observed_at (giây) -> nên dùng row_number()'
    FROM (SELECT sum(c) AS total, count() AS keys, toString(anyIf((k, c, vf), c > 1)) AS sample
          FROM (SELECT seller_sk, any(name) AS k, count() AS c, any(valid_from) AS vf FROM silver_seller GROUP BY seller_sk))
    UNION ALL
    SELECT 'silver_snapshot', 'url_id, observed_at', count(), toUInt64(count() - uniqExact(cityHash64(url_id, observed_at))), '',
           'grain của snapshot: 1 sản phẩm x 1 lần crawl ; job append toàn bộ bronze -> chạy lại không TRUNCATE sẽ nhân đôi'
    FROM silver_snapshot
);



-- ---------------------------------------------------------------- S02 SCD2 silver_product
-- url_id là khóa bền ; mỗi product_sk (hash url_id + title + made_in + warranty) là 1 version.
-- Đúng SCD2: version cũ phải được đóng (is_current = false, valid_to = valid_from của version kế tiếp).
-- Loader hiện chưa có bước đóng version (khối ALTER ... UPDATE đang bị comment), nên S02a sẽ báo FAIL
-- ở mọi sản phẩm có > 1 version. Gold (gold_dim_product) tự tính lại valid_to / is_current nên không bị ảnh hưởng.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'silver', 'silver_product', c.1, c.2, 'consistency', c.3, scope, c.4, c.5, dq_pct(c.5, c.4), '%', 0, 0,
    dq_sample(c.5, c.6), c.7
FROM (
    SELECT
        dq_scope(url_id) AS scope,
        count() AS products,
        countIf(n_current != 1) AS f_current,
        countIf(n_versions > 1) AS multi_version,
        sum(n_rows) AS total_rows_,
        sum(f_valid_to) AS f_valid_to_,
        anyIf(concat(toString(url_id), ': ', toString(n_versions), ' version, ', toString(n_current), ' current'), n_current != 1) AS s_current,
        anyIf(toString(url_id), f_valid_to > 0) AS s_valid_to
    FROM (
        SELECT url_id,
               count() AS n_rows,
               uniqExact(product_sk) AS n_versions,
               countIf(is_current) AS n_current,
               countIf(is_current != (valid_to IS NULL)) AS f_valid_to
        FROM silver_product
        GROUP BY url_id
    )
    GROUP BY scope
)
ARRAY JOIN [
    ('S02a', 'Sản phẩm (url_id) không có đúng 1 version is_current', 'is_current', products, f_current, s_current,
        concat(toString(multi_version), ' sản phẩm có > 1 version ; loader chưa đóng version cũ')),
    ('S02b', 'is_current không khớp valid_to IS NULL', 'valid_to', total_rows_, f_valid_to_, s_valid_to,
        'version đang hiệu lực phải có valid_to = NULL, version đã đóng phải có valid_to')
] AS c;



-- ---------------------------------------------------------------- S03 toàn vẹn tham chiếu
-- Mỗi SK trong snapshot phải tìm được dòng ở dimension. Gold (07) LEFT JOIN rồi rơi về 'unknown' khi không thấy,
-- nên orphan ở đây không làm mất dòng nhưng làm sai brand / seller ở gold.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'silver', 'silver_snapshot', 'S03', 'SK không có trong dimension (orphan)', 'integrity',
    c.1, scope, total, c.2, dq_pct(c.2, total), '%', 0, 0, dq_sample(c.2, c.3), c.4
FROM (
    SELECT
        dq_scope(url_id) AS scope,
        count() AS total,
        countIf(product_sk NOT IN (SELECT product_sk FROM silver_product)) AS f_product,
        countIf(brand_sk NOT IN (SELECT brand_sk FROM silver_brand)) AS f_brand,
        countIf(seller_sk NOT IN (SELECT seller_sk FROM silver_seller)) AS f_seller,
        countIf(category_sk NOT IN (SELECT category_sk FROM silver_category)) AS f_category,
        anyIf(product_sk, product_sk NOT IN (SELECT product_sk FROM silver_product)) AS s_product,
        anyIf(brand_sk, brand_sk NOT IN (SELECT brand_sk FROM silver_brand)) AS s_brand,
        anyIf(seller_sk, seller_sk NOT IN (SELECT seller_sk FROM silver_seller)) AS s_seller,
        anyIf(category_sk, category_sk NOT IN (SELECT category_sk FROM silver_category)) AS s_category
    FROM silver_snapshot
    GROUP BY scope
)
ARRAY JOIN [
    ('product_sk -> silver_product',   f_product,  s_product,  ''),
    ('brand_sk -> silver_brand',       f_brand,    s_brand,    'gold sẽ gán brand unknown'),
    ('seller_sk -> silver_seller',     f_seller,   s_seller,   'gold sẽ gán seller unknown'),
    ('category_sk -> silver_category', f_category, s_category, '')
] AS c;

INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'silver', 'silver_product', 'S03', 'SK không có trong dimension (orphan)', 'integrity',
    'category_sk -> silver_category', dq_scope(url_id) AS scope, count() AS total,
    countIf(category_sk NOT IN (SELECT category_sk FROM silver_category)) AS f, dq_pct(f, total), '%', 0, 0,
    dq_sample(f, anyIf(category_sk, category_sk NOT IN (SELECT category_sk FROM silver_category))), ''
FROM silver_product
GROUP BY scope;



-- ---------------------------------------------------------------- S04 đối soát bronze -> silver
-- S04a/b: so tập khóa (url_id, observed_at) bằng 1 GROUP BY trên hash (không JOIN 10 triệu x 10 triệu).
-- S04c: các tổng phải bằng nhau tuyệt đối. Giá bronze là Float32 nhưng luôn là số nguyên nên tổng Float64
--       của 10 triệu dòng vẫn chính xác (< 2^53) ; lệch nghĩa là có dòng bị mất / nhân đôi / biến đổi.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'silver', 'silver_snapshot', c.1, c.2, 'reconciliation', 'url_id, observed_at', scope, c.3, c.4,
    dq_pct(c.4, c.3), '%', 0, 0, dq_sample(c.4, c.5), c.6
FROM (
    SELECT
        dq_scope(uid) AS scope,
        countIf(in_bronze) AS bronze_keys,
        countIf(in_silver) AS silver_keys,
        countIf(in_bronze AND NOT in_silver) AS missing,
        countIf(in_silver AND NOT in_bronze) AS orphan,
        anyIf(uid, in_bronze AND NOT in_silver) AS s_missing,
        anyIf(uid, in_silver AND NOT in_bronze) AS s_orphan
    FROM (
        SELECT kh, max(src = 0) AS in_bronze, max(src = 1) AS in_silver, any(uid) AS uid
        FROM (
            SELECT cityHash64(toInt64(url_id), observed_at) AS kh, 0 AS src, toInt64(url_id) AS uid FROM bronze_product_snapshot
            UNION ALL
            SELECT cityHash64(url_id, observed_at), 1, url_id FROM silver_snapshot
        )
        GROUP BY kh
    )
    GROUP BY scope
)
ARRAY JOIN [
    ('S04a', 'Lần crawl có ở bronze nhưng không có ở silver_snapshot', bronze_keys, missing, s_missing,
        concat('bronze ', toString(bronze_keys), ' -> silver ', toString(silver_keys))),
    ('S04b', 'Lần crawl ở silver_snapshot không có ở bronze (orphan)', silver_keys, orphan, s_orphan,
        'bronze đã dựng lại mà silver chưa dựng lại')
] AS c;

INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'silver', 'silver_snapshot', 'S04c', 'Bảo toàn tổng bronze -> silver_snapshot (lệch tương đối)',
    'reconciliation', c.1, scope, toUInt64(abs(c.2)), toUInt64(abs(c.2 - c.3)),
    if(c.2 = 0, if(c.3 = 0, 0., 100.), abs(c.2 - c.3) * 100 / abs(c.2)), '%', 0, 0.000001,
    '', concat('bronze = ', toString(c.2), ' ; silver = ', toString(c.3))
FROM (
    SELECT if(b.scope != '', b.scope, s.scope) AS scope, b.n AS b_n, s.n AS s_n, b.s_price AS b_price, s.s_price AS s_price,
           b.s_sold AS b_sold, s.s_sold AS s_sold, b.s_rcnt AS b_rcnt, s.s_rcnt AS s_rcnt
    FROM (
        SELECT dq_scope(url_id) AS scope, toFloat64(count()) AS n, sum(toFloat64(sell_price)) AS s_price,
               sum(toFloat64(sold)) AS s_sold, sum(toFloat64(product_rate_cnt)) AS s_rcnt
        FROM bronze_product_snapshot GROUP BY scope
    ) AS b
    FULL OUTER JOIN (
        SELECT dq_scope(url_id) AS scope, toFloat64(count()) AS n, sum(sell_price) AS s_price,
               sum(toFloat64(sold)) AS s_sold, sum(toFloat64(product_rate_cnt)) AS s_rcnt
        FROM silver_snapshot GROUP BY scope
    ) AS s ON s.scope = b.scope
    SETTINGS join_use_nulls = 0
)
ARRAY JOIN [
    ('count()',               b_n,     s_n),
    ('sum(sell_price)',       b_price, s_price),
    ('sum(sold)',             b_sold,  s_sold),
    ('sum(product_rate_cnt)', b_rcnt,  s_rcnt)
] AS c;

-- S04d: dimension dựng từ đầu thì số SK = số tổ hợp thuộc tính khác nhau ở bronze (đúng công thức hash của loader).
-- Thiếu = có thực thể ở bronze mà silver không có ; thừa = SK cũ không còn ở bronze hoặc bị chèn 2 lần.
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'silver', tbl, 'S04d', 'Số SK của dimension khác số tổ hợp thuộc tính ở bronze', 'reconciliation',
    col, 'all', bronze_n, toUInt64(abs(toInt64(silver_n) - toInt64(bronze_n))),
    dq_pct(abs(toInt64(silver_n) - toInt64(bronze_n)), bronze_n), '%', 0, 0, '',
    concat('bronze ', toString(bronze_n), ' tổ hợp -> silver ', toString(silver_n), ' dòng')
FROM (
    SELECT 'silver_product' AS tbl, 'url_id, title, made_in, warranty' AS col,
           (SELECT uniqExact(url_id, title, made_in, warranty) FROM bronze_product_snapshot) AS bronze_n,
           (SELECT count() FROM silver_product) AS silver_n
    UNION ALL
    SELECT 'silver_category', 'category_tree',
           (SELECT uniqExact(category_tree) FROM bronze_product_snapshot), (SELECT count() FROM silver_category)
    UNION ALL
    SELECT 'silver_brand', 'brand_normalized, brand_origin',
           (SELECT uniqExact(lower(hex(SHA256(ifnull(concat_ws('||', brand_normalized, brand_origin), ''))))) FROM bronze_product_snapshot),
           (SELECT count() FROM silver_brand)
    UNION ALL
    SELECT 'silver_seller', 'seller, seller_rate, seller_rate_cnt',
           (SELECT uniqExact(seller, seller_rate, seller_rate_cnt) FROM bronze_product_snapshot), (SELECT count() FROM silver_seller)
);



-- ---------------------------------------------------------------- S05 phân mảnh entity
-- Silver đưa thuộc tính hay đổi (seller_rate, brand_origin) vào khóa -> 1 thực thể thành nhiều SK.
-- Hệ số = số dòng / số thực thể. Đây là lý do gold gộp lại (brand theo brand_normalized, seller theo tên).
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'silver', tbl, 'S05', 'Phân mảnh entity: số SK trên 1 thực thể', 'consistency', col, 'all',
    rows_, rows_ - entities, if(entities = 0, 0., rows_ / entities), 'x', 0, -1, sample,
    concat(toString(rows_), ' SK / ', toString(entities), ' thực thể ; failed_rows = số SK thừa')
FROM (
    SELECT 'silver_seller' AS tbl, 'name' AS col, count() AS rows_, uniqExact(name) AS entities,
           toString(arraySlice(topK(3)(name), 1, 3)) AS sample
    FROM silver_seller
    UNION ALL
    SELECT 'silver_brand', 'brand_normalized', count(), uniqExact(brand_normalized), toString(topK(3)(brand_normalized))
    FROM silver_brand
    UNION ALL
    SELECT 'silver_product', 'url_id', count(), uniqExact(url_id), '' FROM silver_product
);



-- ---------------------------------------------------------------- S06 cấu trúc danh mục, url_id
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'silver', 'silver_category', c.1, c.2, 'validity', 'category_level_*', 'all', total, c.3,
    dq_pct(c.3, total), '%', c.4, c.5, dq_sample(c.3, c.6), c.7
FROM (
    SELECT
        count() AS total,
        countIf(arrayExists(i -> levels[i] = '' AND levels[i + 1] != '', range(1, 6))) AS f_gap,
        countIf(category_level_1 = '') AS f_empty,
        anyIf(arrayStringConcat(levels, ' > '), arrayExists(i -> levels[i] = '' AND levels[i + 1] != '', range(1, 6))) AS s_gap,
        anyIf(category_sk, category_level_1 = '') AS s_empty
    FROM (
        SELECT category_sk, category_level_1,
               [category_level_1, category_level_2, category_level_3, category_level_4, category_level_5, category_level_6] AS levels
        FROM silver_category
    )
)
ARRAY JOIN [
    ('S06a', 'Cấp danh mục rỗng nằm giữa 2 cấp có giá trị', f_gap, 0, 0, s_gap, ''),
    ('S06b', 'Danh mục rỗng (category_level_1 = \'\', sinh từ category_tree trống)', f_empty, 0, 101, s_empty,
        'là "unknown member" của danh mục ; WARN để nhớ lọc khi phân tích')
] AS c;

INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'silver', tbl, 'S06c', 'url_id = 0 (dữ liệu nạp trước migration thêm cột url_id)', 'validity',
    'url_id', 'all', total, f, dq_pct(f, total), '%', 0, 0, '', 'TRUNCATE rồi nạp lại silver'
FROM (
    SELECT 'silver_snapshot' AS tbl, count() AS total, countIf(url_id = 0) AS f FROM silver_snapshot
    UNION ALL
    SELECT 'silver_product', count(), countIf(url_id = 0) FROM silver_product
);
