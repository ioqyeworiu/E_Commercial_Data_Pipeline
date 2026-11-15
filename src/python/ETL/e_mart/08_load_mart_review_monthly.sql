-- M8. 1 dòng / tháng viết review (ước tính) / category / nhóm thâm niên reviewer. Không phụ thuộc mart khác.
-- Nguồn: F4, gold_dim_reviewer (tenure_band), gold_dim_category.
-- reviewed_at_est chỉ chính xác theo năm/tháng (nguồn ghi "4 năm trước") nên grain thời gian là THÁNG.
-- Review chưa gắn được sản phẩm (category_key = '') có category_level_1 = 'unknown'.
DROP TABLE IF EXISTS mart_review_monthly__stage;
CREATE TABLE mart_review_monthly__stage AS mart_review_monthly;

INSERT INTO mart_review_monthly__stage (
    review_month,
    category_key,
    category_level_1,
    category_level_2,
    tenure_band,
    n_reviews,
    n_rated_reviews,
    star_sum,
    star_1,
    star_2,
    star_3,
    star_4,
    star_5,
    n_positive,
    n_negative,
    n_with_image,
    n_with_comment,
    image_sum,
    word_sum
)
WITH
    rv AS (
        SELECT
            if(r.reviewed_at_est IS NULL, toDate('1970-01-01'), toStartOfMonth(assumeNotNull(r.reviewed_at_est))) AS k_month,
            r.category_key AS k_category_key,
            if(u.tenure_band = '', 'không rõ', u.tenure_band) AS k_tenure_band,
            toUInt32(count()) AS k_reviews,
            toUInt32(countIf(r.star_score > 0)) AS k_rated,
            toUInt64(sum(r.star_score)) AS k_star_sum,
            toUInt32(countIf(r.star_score = 1)) AS k_star_1,
            toUInt32(countIf(r.star_score = 2)) AS k_star_2,
            toUInt32(countIf(r.star_score = 3)) AS k_star_3,
            toUInt32(countIf(r.star_score = 4)) AS k_star_4,
            toUInt32(countIf(r.star_score = 5)) AS k_star_5,
            toUInt32(countIf(r.is_positive)) AS k_positive,
            toUInt32(countIf(r.is_negative)) AS k_negative,
            toUInt32(countIf(r.has_image)) AS k_with_image,
            toUInt32(countIf(r.comment_length > 0)) AS k_with_comment,
            toUInt64(sum(r.image_count)) AS k_image_sum,
            toUInt64(sum(r.word_count)) AS k_word_sum
        FROM gold_fact_review AS r FINAL
        LEFT JOIN gold_dim_reviewer AS u FINAL ON u.user_key = r.user_key
        GROUP BY k_month, k_category_key, k_tenure_band
    )
SELECT
    rv.k_month,
    rv.k_category_key,
    if(c.category_level_1 = '', 'unknown', c.category_level_1),
    c.category_level_2,
    rv.k_tenure_band,
    rv.k_reviews,
    rv.k_rated,
    rv.k_star_sum,
    rv.k_star_1,
    rv.k_star_2,
    rv.k_star_3,
    rv.k_star_4,
    rv.k_star_5,
    rv.k_positive,
    rv.k_negative,
    rv.k_with_image,
    rv.k_with_comment,
    rv.k_image_sum,
    rv.k_word_sum
FROM rv
LEFT JOIN gold_dim_category AS c FINAL ON c.category_key = rv.k_category_key;

ALTER TABLE mart_review_monthly REPLACE PARTITION tuple() FROM mart_review_monthly__stage;
DROP TABLE mart_review_monthly__stage;
