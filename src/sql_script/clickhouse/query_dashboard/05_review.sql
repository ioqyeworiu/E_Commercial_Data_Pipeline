-- =====================================================================================
-- DASHBOARD 5: ĐÁNH GIÁ KHÁCH HÀNG (VOICE OF CUSTOMER)
-- Câu hỏi: khách hài lòng tới đâu, danh mục / brand nào bị chê nhiều, người dùng lâu năm có khó tính
-- hơn không, rating cao có đi kèm bán chạy không.
--
-- Tham số: {category_l1:String} ('' = tất cả).
-- KHÔNG lọc theo date_from/date_to: ngày viết review chỉ là ước tính ("4 năm trước") và review trải
-- từ 2019 tới nay, trong khi dữ liệu bán chỉ có từ 10/2026. Trục thời gian của dashboard này là THÁNG.
-- Tỉ lệ (sao TB, % tiêu cực) luôn tính trên review CÓ SAO (n_rated_reviews), không tính review 0 sao.
-- =====================================================================================


-- [5.1] Thẻ KPI review | Biểu đồ: KPI cards
SELECT
    sum(n_reviews) AS reviews,
    round(sum(star_sum) / nullIf(sum(n_rated_reviews), 0), 2) AS avg_star,
    round(sum(n_positive) / nullIf(sum(n_rated_reviews), 0) * 100, 1) AS positive_pct,      -- >= 4 sao
    round(sum(n_negative) / nullIf(sum(n_rated_reviews), 0) * 100, 1) AS negative_pct,      -- 1-2 sao
    -- Net Promoter-style: % 5 sao - % 1-3 sao (thang -100 .. 100)
    round((sum(star_5) - sum(star_1) - sum(star_2) - sum(star_3)) / nullIf(sum(n_rated_reviews), 0) * 100, 1) AS net_sentiment,
    round(sum(n_with_image) / nullIf(sum(n_reviews), 0) * 100, 1) AS with_image_pct,
    round(sum(n_with_comment) / nullIf(sum(n_reviews), 0) * 100, 1) AS with_comment_pct,
    round(sum(word_sum) / nullIf(sum(n_with_comment), 0), 1) AS avg_words_per_comment
FROM mart_review_monthly
WHERE {category_l1:String} = '' OR category_level_1 = {category_l1:String};


-- [5.2] Phân bố số sao | Biểu đồ: bar ngang 5 -> 1 sao (màu xanh -> đỏ)
SELECT
    stars,
    reviews,
    round(reviews / sum(reviews) OVER () * 100, 1) AS pct
FROM (
    SELECT
        sum(star_1) AS s1, sum(star_2) AS s2, sum(star_3) AS s3, sum(star_4) AS s4, sum(star_5) AS s5
    FROM mart_review_monthly
    WHERE {category_l1:String} = '' OR category_level_1 = {category_l1:String}
)
ARRAY JOIN [5, 4, 3, 2, 1] AS stars, [s5, s4, s3, s2, s1] AS reviews
ORDER BY stars DESC;


-- [5.3] Số review và sao TB theo tháng | Biểu đồ: combo (cột = số review, đường = sao TB, trục phụ)
-- Bỏ tháng 1970-01 (không xác định được ngày viết).
SELECT
    review_month,
    sum(n_reviews) AS reviews,
    round(sum(star_sum) / nullIf(sum(n_rated_reviews), 0), 2) AS avg_star,
    round(sum(n_negative) / nullIf(sum(n_rated_reviews), 0) * 100, 1) AS negative_pct,
    round(sum(n_with_image) / nullIf(sum(n_reviews), 0) * 100, 1) AS with_image_pct
FROM mart_review_monthly
WHERE review_month > '1970-01-01'
  AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
GROUP BY review_month
ORDER BY review_month;


-- [5.4] Chất lượng review theo danh mục (drill-down cấp 1 -> cấp 2) | Biểu đồ: bar ngang sắp theo % tiêu cực
-- (cột chính) + điểm sao TB ; chỉ hiện nhóm >= 100 review có sao để tỉ lệ không dao động vì mẫu nhỏ.
SELECT
    if({category_l1:String} = '', category_level_1,
       if(category_level_2 = '', '(không phân cấp)', category_level_2)) AS category,
    sum(n_reviews) AS reviews,
    sum(n_rated_reviews) AS rated_reviews,
    round(sum(star_sum) / nullIf(rated_reviews, 0), 2) AS avg_star,
    round(sum(n_negative) / nullIf(rated_reviews, 0) * 100, 1) AS negative_pct,
    round(sum(n_positive) / nullIf(rated_reviews, 0) * 100, 1) AS positive_pct,
    round(sum(n_with_image) / nullIf(sum(n_reviews), 0) * 100, 1) AS with_image_pct,
    round(sum(word_sum) / nullIf(sum(n_with_comment), 0), 1) AS avg_words_per_comment
FROM mart_review_monthly
WHERE {category_l1:String} = '' OR category_level_1 = {category_l1:String}
GROUP BY category
HAVING rated_reviews >= 100
ORDER BY negative_pct DESC;


-- [5.5] Thâm niên người review và mức độ khó tính | Biểu đồ: column (sao TB, % tiêu cực theo tenure_band)
SELECT
    tenure_band,
    sum(n_reviews) AS reviews,
    round(sum(n_reviews) / sum(sum(n_reviews)) OVER () * 100, 1) AS reviews_pct,
    round(sum(star_sum) / nullIf(sum(n_rated_reviews), 0), 2) AS avg_star,
    round(sum(n_negative) / nullIf(sum(n_rated_reviews), 0) * 100, 1) AS negative_pct,
    round(sum(n_with_image) / nullIf(sum(n_reviews), 0) * 100, 1) AS with_image_pct,
    round(sum(word_sum) / nullIf(sum(n_with_comment), 0), 1) AS avg_words_per_comment
FROM mart_review_monthly
WHERE {category_l1:String} = '' OR category_level_1 = {category_l1:String}
GROUP BY tenure_band
ORDER BY multiIf(tenure_band = 'dưới 1 năm', 1, tenure_band = '1-3 năm', 2, tenure_band = '3-5 năm', 3,
                 tenure_band = 'trên 5 năm', 4, 5);


-- [5.6] Thương hiệu bị đánh giá tiêu cực nhiều nhất | Biểu đồ: bar ngang top 15
-- Gộp từ mart_product_scorecard theo brand ; tối thiểu 100 review có sao.
-- Xếp theo cận dưới khoảng tin cậy Wilson 95% của % tiêu cực, để brand ít review mà tỉ lệ cao do
-- ngẫu nhiên không lọt lên đầu (cách xếp hạng của Evan Miller, "How Not To Sort By Average Rating").
WITH brands AS (
    SELECT
        any(brand_name) AS brand_name,
        sum(n_rated_reviews) AS n,
        sum(n_negative) AS neg,
        sum(star_sum) AS stars,
        sum(revenue_total) AS revenue
    FROM mart_product_scorecard
    WHERE NOT brand_is_unknown
      AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
    GROUP BY brand_key
    HAVING n >= 100
)
SELECT
    brand_name,
    n AS rated_reviews,
    round(stars / n, 2) AS avg_star,
    round(neg / n * 100, 1) AS negative_pct,
    round((neg / n + 1.96 * 1.96 / (2 * n)
           - 1.96 * sqrt((neg / n) * (1 - neg / n) / n + 1.96 * 1.96 / (4 * n * n)))
          / (1 + 1.96 * 1.96 / n) * 100, 1) AS negative_pct_wilson_low,
    revenue
FROM brands
ORDER BY negative_pct_wilson_low DESC
LIMIT 15;


-- [5.7] Rating có đi kèm bán chạy không? | Biểu đồ: column (bán TB / ngày theo nhóm sao TB của sản phẩm)
-- Mỗi sản phẩm xếp vào 1 nhóm theo avg_star từ review ; sản phẩm chưa có review riêng một nhóm.
SELECT
    multiIf(n_rated_reviews = 0, '0. chưa có review',
            avg_star < 3, '1. < 3 sao',
            avg_star < 3.5, '2. 3 - 3,5 sao',
            avg_star < 4, '3. 3,5 - 4 sao',
            avg_star < 4.5, '4. 4 - 4,5 sao',
            '5. >= 4,5 sao') AS star_band,
    count() AS products,
    round(avg(avg_daily_units), 2) AS avg_daily_units,
    round(avg(revenue_total)) AS avg_revenue_per_product,
    round(countIf(abc_class = 'A') / count() * 100, 1) AS class_a_pct,
    round(avg(current_sell_price)) AS avg_price
FROM mart_product_scorecard
WHERE {category_l1:String} = '' OR category_level_1 = {category_l1:String}
GROUP BY star_band
ORDER BY star_band;
