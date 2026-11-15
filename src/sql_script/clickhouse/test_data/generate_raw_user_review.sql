-- =====================================================================================
-- Sinh dữ liệu TEST cho raw_user_review
--   500.000 review trên các sản phẩm test (url_id 1.000.000 -> 1.049.999, sản phẩm nhỏ có nhiều review hơn)
--   + 25.000 lần crawl lại (cùng review, observed_at sau 3 ngày) -> gold_fact_review phải gộp lại
-- Định dạng giống crawler thật: 'Đã tham gia 7 năm', '27 Đánh giá', '0 Lượt cảm ơn',
-- 'Đánh giá vào 4 năm trước', 'Đã dùng 2 tháng', '["Màu: Xanh đen"]', images_urls dạng JSON.
-- Chạy sau generate_raw_product_snapshot.sql.
-- =====================================================================================
INSERT INTO raw_user_review (
    url_id, user_name, joined_duration, reviewed, thank_received, star_score, comment, status,
    images_urls, rated_at, use_time, prod_properties, observed_at, ingested_at, idempotency_key
)
SELECT
    url_id_str, user_name_raw, joined_raw, reviewed_raw, thank_raw, star_raw, comment_raw, status_raw,
    images_raw, rated_at_raw, use_time_raw, props_raw, observed_at_raw, ingested_at_raw, idempotency_key_raw
FROM (
    WITH
        ['Nguyễn','Trần','Lê','Phạm','Hoàng','Huỳnh','Phan','Vũ','Võ','Đặng','Bùi','Đỗ','Hồ','Ngô','Dương'] AS surnames,
        ['Văn','Thị','Minh','Hoàng','Ngọc','Thanh','Quốc','Đức','Thu','Gia','Bảo','Kim'] AS middles,
        ['An','Bình','Châu','Dũng','Giang','Hà','Hải','Hạnh','Hiếu','Hoa','Hùng','Hương','Khang','Khánh',
         'Lan','Linh','Long','Mai','Minh','My','Nam','Nga','Ngân','Nhung','Phong','Phúc','Phương','Quân',
         'Quang','Sơn','Tâm','Thảo','Thắng','Trang','Trung','Tú','Tuấn','Uyên','Vy','Yến'] AS givens,
        ['Rất không hài lòng','Không hài lòng','Bình thường','Hài lòng','Cực kì hài lòng'] AS statuses,
        ['Sản phẩm rất tốt, đúng như mô tả.', 'Giao hàng nhanh, đóng gói cẩn thận!',
         'Chất lượng tuyệt vời so với giá tiền 👍', 'Mặc rất vừa, vải mát, sẽ ủng hộ shop dài dài.',
         'Hàng chính hãng, dùng rất thích ❤️'] AS good_comments,
        ['Tạm ổn, không quá xuất sắc.', 'Hàng giống hình nhưng giao hơi chậm.', 'Dùng được, giá hơi cao.'] AS mid_comments,
        ['Hàng lỗi, không giống mô tả!!!', 'Chất lượng kém, rất thất vọng.', 'Giao sai màu, shop phản hồi chậm.'] AS bad_comments,
        ['Đen','Trắng','Be','Xám','Xanh navy','Đỏ đô','Hồng pastel'] AS colors,
        ['S','M','L','XL','XXL','Free size'] AS sizes,
        toDateTime('2026-10-15 00:00:00', 'Asia/Ho_Chi_Minh') AS crawl_start
    SELECT
        r, recrawl,
        toUInt32(floor(50000 * pow((cityHash64(r, 'product') % 1000000) / 1e6, 1.5))) AS p,
        toString(1000000 + p) AS url_id_str,

        -- người dùng: ~7.200 tên khác nhau cho 200.000 "người" -> có trùng tên như dữ liệu thật
        toUInt32(floor(200000 * pow((cityHash64(r, 'user') % 1000000) / 1e6, 1.3))) AS uid,
        concat(surnames[1 + uid % 15], ' ', middles[1 + intDiv(uid, 15) % 12], ' ', givens[1 + intDiv(uid, 180) % 40]) AS user_name_raw,
        cityHash64(uid, 'joined') % 100 AS jv,
        if(jv < 10, concat('Đã tham gia ', toString(1 + jv), ' tháng'),
                    concat('Đã tham gia ', toString(1 + jv % 9), ' năm')) AS joined_raw,
        concat(toString(1 + cityHash64(uid, 'reviewed') % 80), ' Đánh giá') AS reviewed_raw,
        concat(toString(cityHash64(uid, 'thanks') % 30), ' Lượt cảm ơn') AS thank_raw,

        cityHash64(r, 'star') % 100 AS sv,
        multiIf(sv < 5, 1, sv < 10, 2, sv < 20, 3, sv < 40, 4, 5) AS star,
        toString(star) AS star_raw,
        statuses[star] AS status_raw,
        cityHash64(r, 'comment') % 100 AS cv,
        multiIf(
            cv < 30, '',
            star >= 4, concat(good_comments[1 + cityHash64(r, 'c1') % 5],
                              if(cv < 60, concat(' ', good_comments[1 + cityHash64(r, 'c2') % 5]), '')),
            star = 3, mid_comments[1 + cityHash64(r, 'c1') % 3],
            bad_comments[1 + cityHash64(r, 'c1') % 3]
        ) AS comment_raw,
        cityHash64(r, 'img') % 100 AS iv,
        if(iv >= 15, '[]',
           concat('[', arrayStringConcat(arrayMap(i -> concat(
               '"background-image: url(\\"https://salt.tikicdn.com/cache/w280/ts/review/',
               substring(lower(hex(cityHash64(r, i))), 1, 2), '/', substring(lower(hex(cityHash64(r, i))), 3, 2), '/',
               lower(hex(cityHash64(r, i, 'img'))), '.jpg\\");"'), range(1 + iv % 3)), ','), ']')) AS images_raw,

        -- thời điểm crawler đọc trang review của sản phẩm p ; lần crawl lại cách 3 ngày
        crawl_start + toIntervalSecond(cityHash64(p, 'review_crawl') % (25 * 86400)
                                       + cityHash64(r, 'pos') % 600 + recrawl * 3 * 86400) AS t,
        -- tuổi review (giờ) tại lúc crawl: đa số mới, đuôi dài tới ~5 năm
        pow((cityHash64(r, 'age') % 1000000) / 1e6, 2) * 5 * 8760 + recrawl * 72 AS age_h,
        multiIf(
            age_h >= 8760, concat('Đánh giá vào ', toString(toUInt32(floor(age_h / 8760))), ' năm trước'),
            age_h >= 730,  concat('Đánh giá vào ', toString(toUInt32(floor(age_h / 730))), ' tháng trước'),
            age_h >= 168,  concat('Đánh giá vào ', toString(toUInt32(floor(age_h / 168))), ' tuần trước'),
            age_h >= 24,   concat('Đánh giá vào ', toString(toUInt32(floor(age_h / 24))), ' ngày trước'),
            concat('Đánh giá vào ', toString(toUInt32(greatest(floor(age_h), 1))), ' giờ trước')
        ) AS rated_at_raw,
        cityHash64(r, 'use') % 100 AS uv,
        multiIf(uv < 20, '',
                uv < 50, concat('Đã dùng ', toString(1 + uv % 6), ' ngày'),
                uv < 75, concat('Đã dùng ', toString(1 + uv % 4), ' tuần'),
                concat('Đã dùng ', toString(1 + uv % 11), ' tháng')) AS use_time_raw,
        cityHash64(r, 'props') % 100 AS pv,
        multiIf(pv < 40, '[]',
                pv < 70, concat('["Màu: ', colors[1 + cityHash64(r, 'pc') % 7], '"]'),
                concat('["Màu: ', colors[1 + cityHash64(r, 'pc') % 7], '","Size: ', sizes[1 + cityHash64(r, 'ps') % 6], '"]')) AS props_raw,
        -- micro giây = r -> (url_id, observed_at) không bao giờ trùng giữa 2 review khác nhau
        concat(formatDateTime(t, '%Y-%m-%dT%H:%i:%S'), '.', leftPad(toString(r % 1000000), 6, '0'), '+07:00') AS observed_at_raw,
        t + toIntervalSecond(5 + cityHash64(r, recrawl, 'lag') % 115) AS ingested_at_raw,
        lower(hex(SHA256(concat(url_id_str, '\x01', observed_at_raw)))) AS idempotency_key_raw
    FROM (
        SELECT number AS r, 0 AS recrawl FROM numbers(500000)
        UNION ALL
        SELECT cityHash64(number, 'recrawl') % 500000 AS r, 1 AS recrawl FROM numbers(25000)
    )
);
