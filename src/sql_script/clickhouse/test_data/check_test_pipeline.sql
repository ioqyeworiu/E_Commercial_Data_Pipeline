-- =====================================================================================
-- Kiểm tra kết quả pipeline sau khi nạp bộ dữ liệu test (chạy qua run_test_pipeline.sh check).
-- "real" = dữ liệu crawl thật (url_id < 1.000.000), "test" = dữ liệu sinh (url_id >= 1.000.000).
-- =====================================================================================

SELECT '1. SỐ DÒNG THEO TẦNG (fact có url_id)';
SELECT layer, tbl, real_rows, test_rows, total FROM (
    SELECT 1 o, 'raw' layer, 'raw_product_snapshot' tbl, countIf(toInt64OrZero(url_id) < 1000000) real_rows, countIf(toInt64OrZero(url_id) >= 1000000) test_rows, count() total FROM raw_product_snapshot
    UNION ALL SELECT 2, 'raw', 'raw_user_review', countIf(toInt64OrZero(url_id) < 1000000), countIf(toInt64OrZero(url_id) >= 1000000), count() FROM raw_user_review
    UNION ALL SELECT 3, 'bronze', 'bronze_product_snapshot', countIf(url_id < 1000000), countIf(url_id >= 1000000), count() FROM bronze_product_snapshot
    UNION ALL SELECT 4, 'bronze', 'bronze_user_review', countIf(url_id < 1000000), countIf(url_id >= 1000000), count() FROM bronze_user_review
    UNION ALL SELECT 5, 'silver', 'silver_snapshot', countIf(url_id < 1000000), countIf(url_id >= 1000000), count() FROM silver_snapshot
    UNION ALL SELECT 6, 'silver', 'silver_product', countIf(url_id < 1000000), countIf(url_id >= 1000000), count() FROM silver_product
    UNION ALL SELECT 7, 'gold', 'gold_dim_product', countIf(product_id < 1000000), countIf(product_id >= 1000000), count() FROM gold_dim_product FINAL
    UNION ALL SELECT 8, 'gold', 'gold_fact_product_snapshot', countIf(product_id < 1000000), countIf(product_id >= 1000000), count() FROM gold_fact_product_snapshot FINAL
    UNION ALL SELECT 9, 'gold', 'gold_fact_product_daily', countIf(product_id < 1000000), countIf(product_id >= 1000000), count() FROM gold_fact_product_daily FINAL
    UNION ALL SELECT 10, 'gold', 'gold_fact_price_change', countIf(product_id < 1000000), countIf(product_id >= 1000000), count() FROM gold_fact_price_change FINAL
    UNION ALL SELECT 11, 'gold', 'gold_fact_review', countIf(product_id < 1000000), countIf(product_id >= 1000000), count() FROM gold_fact_review FINAL
) ORDER BY o
FORMAT PrettyCompactMonoBlock;

SELECT '2. SỐ DÒNG DIMENSION (silver -> gold)';
SELECT * FROM (
    SELECT 1 o, 'silver_category' tbl, count() n FROM silver_category
    UNION ALL SELECT 2, 'gold_dim_category', count() FROM gold_dim_category FINAL
    UNION ALL SELECT 3, 'silver_brand', count() FROM silver_brand
    UNION ALL SELECT 4, 'gold_dim_brand', count() FROM gold_dim_brand FINAL
    UNION ALL SELECT 5, 'silver_seller', count() FROM silver_seller
    UNION ALL SELECT 6, 'gold_dim_seller', count() FROM gold_dim_seller FINAL
    UNION ALL SELECT 7, 'gold_dim_reviewer', count() FROM gold_dim_reviewer FINAL
    UNION ALL SELECT 8, 'gold_fact_seller_daily', count() FROM gold_fact_seller_daily FINAL
    UNION ALL SELECT 9, 'gold_dim_date', count() FROM gold_dim_date FINAL
) ORDER BY o
FORMAT PrettyCompactMonoBlock;

SELECT '3. DUNG LƯỢNG & NÉN';
SELECT
    table,
    sum(rows) AS rows,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    round(sum(data_uncompressed_bytes) / greatest(sum(data_compressed_bytes), 1), 1) AS ratio,
    count() AS parts
FROM system.parts
WHERE database = currentDatabase() AND active
GROUP BY table
ORDER BY sum(data_compressed_bytes) DESC
FORMAT PrettyCompactMonoBlock;

SELECT '4. CHẤT LƯỢNG BRONZE (dữ liệu test): giá trị lỗi / thiếu sau chuẩn hóa';
SELECT
    countIf(sell_price = 0) AS sell_price_0,           -- từ '' và 'Liên hệ'
    countIf(sold = 0) AS sold_0,
    countIf(brand_normalized IS NULL) AS brand_null,
    countIf(category_tree = '') AS category_empty,
    countIf(made_in = '') AS made_in_empty,
    countIf(seller = '') AS seller_empty,
    countIf(seller_rate = 0) AS seller_rate_0
FROM bronze_product_snapshot WHERE url_id >= 1000000
FORMAT PrettyCompactMonoBlock;

SELECT '4b. normalize_rating_count đọc dạng "tr+" (triệu): "(5.6tr+ đánh giá)" phải thành 5600000 ở bronze (ok = 1)';
SELECT r.seller_rate_cnt AS raw_value, b.seller_rate_cnt AS bronze_value, b.seller_rate_cnt >= 1000000 AS ok, count() AS rows
FROM (SELECT idempotency_key, seller_rate_cnt FROM raw_product_snapshot WHERE seller_rate_cnt LIKE '%tr+%' LIMIT 1 BY seller_rate_cnt) AS r
INNER JOIN bronze_product_snapshot AS b ON b.idempotency_key = r.idempotency_key
GROUP BY raw_value, bronze_value, ok
FORMAT PrettyCompactMonoBlock;

SELECT '5. ENTITY RESOLUTION BRAND: silver tách theo origin, gold gộp theo brand_normalized';
SELECT
    (SELECT count() FROM silver_brand) AS silver_brand_rows,
    (SELECT uniqExact(brand_normalized) FROM silver_brand) AS distinct_brand_normalized,
    (SELECT count() FROM gold_dim_brand FINAL) AS gold_dim_brand_rows,
    (SELECT countIf(length(name_aliases) > 1) FROM gold_dim_brand FINAL) AS gold_brands_with_aliases
FORMAT PrettyCompactMonoBlock;
SELECT brand_name, brand_origin, name_aliases, brand_origins
FROM gold_dim_brand FINAL WHERE length(name_aliases) >= 4 ORDER BY length(name_aliases) DESC LIMIT 3
FORMAT PrettyCompactMonoBlock;

SELECT '6. SILVER SELLER PHÌNH RA (khóa có rating) so với gold_dim_seller';
SELECT
    (SELECT count() FROM silver_seller) AS silver_seller_rows,
    (SELECT uniqExact(name) FROM silver_seller) AS distinct_seller_names,
    (SELECT count() FROM gold_dim_seller FINAL) AS gold_dim_seller_rows
FORMAT PrettyCompactMonoBlock;

SELECT '7. SCD2: số sản phẩm có > 1 version (dữ liệu sinh: ~3% = ~1.500)';
SELECT countIf(versions > 1) AS products_with_new_version, max(versions) AS max_versions
FROM (SELECT product_id, count() AS versions FROM gold_dim_product FINAL WHERE product_id >= 1000000 GROUP BY product_id)
FORMAT PrettyCompactMonoBlock;

SELECT '8. HIỆU ỨNG NGÀY SALE: units_sold_est trung bình / sản phẩm / ngày';
SELECT
    d.full_date AS date,
    d.sale_event AS sale_event,
    round(avg(f.units_sold_est), 2) AS avg_units_per_product,
    formatReadableQuantity(sum(f.revenue_est)) AS revenue_est,
    round(avg(f.avg_discount_pct), 1) AS avg_discount_pct
FROM gold_fact_product_daily AS f FINAL
INNER JOIN gold_dim_date AS d FINAL ON d.date_key = f.date_key
WHERE f.product_id >= 1000000 AND d.full_date BETWEEN '2026-10-08' AND '2026-10-13'
   OR f.product_id >= 1000000 AND d.full_date BETWEEN '2026-11-08' AND '2026-11-12'
GROUP BY date, sale_event
ORDER BY date
FORMAT PrettyCompactMonoBlock;

SELECT '8b. BỘ ĐẾM LŨY KẾ: F1 còn lần crawl "đã bán" trống (= 0), F2 phải không bao giờ giảm (ok = 1)';
SELECT
    (SELECT countIf(sold_cum = 0 AND prev_sold > 0)
     FROM (SELECT sold_cum, lagInFrame(sold_cum) OVER (PARTITION BY product_id ORDER BY observed_at
                                                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev_sold
           FROM gold_fact_product_snapshot FINAL)) AS f1_sold_blank_obs,          -- lỗi đầu vào, F2 phải bỏ qua được
    countIf(sold_eod < prev_sold_eod) AS f2_sold_eod_decrease,
    countIf(rating_cnt_eod < prev_rating_cnt_eod) AS f2_rating_cnt_eod_decrease,
    f2_sold_eod_decrease = 0 AND f2_rating_cnt_eod_decrease = 0 AS ok
FROM (
    SELECT
        sold_eod,
        rating_cnt_eod,
        lagInFrame(sold_eod) OVER w AS prev_sold_eod,
        lagInFrame(rating_cnt_eod) OVER w AS prev_rating_cnt_eod,
        row_number() OVER w AS rn
    FROM gold_fact_product_daily FINAL
    WINDOW w AS (PARTITION BY product_id ORDER BY date_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
WHERE rn > 1
FORMAT PrettyCompactMonoBlock;

SELECT '9. PHÁT HIỆN GIẢM GIÁ ẢO: so is_list_price_inflated với "đáp án" của bộ sinh (cityHash64(p, ''fake'') % 100 < 10)';
SELECT
    countIf(is_truth) AS truly_fake,
    countIf(is_detected) AS detected_products,
    countIf(is_truth AND is_detected) AS true_positive,
    round(countIf(is_truth AND is_detected) / greatest(countIf(is_detected), 1), 3) AS precision,
    round(countIf(is_truth AND is_detected) / greatest(countIf(is_truth), 1), 3) AS recall
FROM (
    SELECT
        1000000 + number AS pid,
        cityHash64(number, 'fake') % 100 < 10 AS is_truth,
        pid IN (SELECT product_id FROM gold_fact_price_change FINAL WHERE is_list_price_inflated) AS is_detected
    FROM numbers(50000)
)
FORMAT PrettyCompactMonoBlock;
SELECT direction, count() AS changes, round(avg(change_pct), 1) AS avg_change_pct, countIf(is_list_price_inflated) AS inflated
FROM gold_fact_price_change FINAL WHERE product_id >= 1000000 GROUP BY direction
FORMAT PrettyCompactMonoBlock;

SELECT '10. REVIEW: raw -> bronze -> gold (gộp lần crawl lại)';
SELECT
    (SELECT count() FROM raw_user_review WHERE toInt64OrZero(url_id) >= 1000000) AS raw_rows,
    (SELECT count() FROM bronze_user_review WHERE url_id >= 1000000) AS bronze_rows,
    (SELECT count() FROM gold_fact_review FINAL WHERE product_id >= 1000000) AS gold_reviews,
    (SELECT countIf(product_key = '') FROM gold_fact_review FINAL WHERE product_id >= 1000000) AS reviews_without_product,
    (SELECT countIf(review_date_key = 0) FROM gold_fact_review FINAL WHERE product_id >= 1000000) AS reviews_without_date
FORMAT PrettyCompactMonoBlock;
SELECT star_score, count() AS reviews, round(avg(has_image), 3) AS share_with_image, round(avg(comment_length), 1) AS avg_comment_len
FROM gold_fact_review FINAL WHERE product_id >= 1000000 GROUP BY star_score ORDER BY star_score
FORMAT PrettyCompactMonoBlock;

SELECT '11. TOÀN VẸN KHÓA: khóa trong fact không có trong dim (phải = 0)';
SELECT
    countIf(product_key NOT IN (SELECT product_key FROM gold_dim_product)) AS miss_product,
    countIf(brand_key NOT IN (SELECT brand_key FROM gold_dim_brand)) AS miss_brand,
    countIf(seller_key NOT IN (SELECT seller_key FROM gold_dim_seller)) AS miss_seller,
    countIf(category_key NOT IN (SELECT category_key FROM gold_dim_category)) AS miss_category,
    countIf(date_key NOT IN (SELECT date_key FROM gold_dim_date)) AS miss_date
FROM gold_fact_product_snapshot FINAL
FORMAT PrettyCompactMonoBlock;
