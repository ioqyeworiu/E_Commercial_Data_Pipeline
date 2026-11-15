-- =====================================================================================
-- DASHBOARD 4: GIÁ & KHUYẾN MÃI
-- Câu hỏi: giá biến động thế nào, giảm giá sâu hơn có bán nhiều hơn không, ngày sale có thật sự rẻ
-- hơn ngày thường không, ai đang "giảm giá ảo" (nâng giá gốc rồi ghi % giảm lớn).
--
-- Tham số: {date_from:Date}, {date_to:Date}, {category_l1:String} ('' = tất cả),
--          {sale_event:String} ví dụ '11.11' hoặc '10.10' (cho [4.5] - [4.7]).
-- "Giảm thật / giảm giá ảo" so giá ngày sale với giá 30 ngày trước đó (mart_sale_event_product,
-- xem định nghĩa discount_verdict ở src/python/ETL/e_mart/07_load_mart_sale_event_product.sql).
-- =====================================================================================


-- [4.1] Số lần đổi giá theo ngày | Biểu đồ: stacked column (tăng / giảm) + đường (nghi giảm giá ảo, trục phụ)
-- Đã bỏ "đổi giá giả" do nguồn thiếu giá (giá -> 0 -> giá).
SELECT
    full_date,
    sale_event,
    price_up,
    price_down,
    price_changes - price_up - price_down AS list_price_only,   -- chỉ giá gốc đổi, giá bán giữ nguyên
    inflated_events,
    round(price_changes / products_observed * 100, 2) AS changes_per_100_products
FROM mart_kpi_daily
WHERE is_reliable_day AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
ORDER BY full_date;


-- [4.2] Giảm giá sâu hơn có bán nhiều hơn không? | Biểu đồ: column (bán TB / sản phẩm / ngày theo mức giảm)
-- + đường (% doanh thu). Chỉ lấy NGÀY THƯỜNG để tách hiệu ứng ngày sale ra khỏi hiệu ứng % giảm.
-- Đây là tương quan, không phải nhân quả: sản phẩm bán chạy có thể được giảm giá nhiều hơn.
SELECT
    discount_band,
    sum(product_days) AS product_days,
    round(sum(units_sold_est) / nullIf(sum(product_days_sales_known), 0), 2) AS units_per_product_day,
    round(sum(products_selling) / nullIf(sum(product_days_sales_known), 0) * 100, 1) AS selling_pct,
    sum(revenue_est) AS revenue,
    round(sum(revenue_est) / sum(sum(revenue_est)) OVER () * 100, 1) AS revenue_share_pct,
    round(units_per_product_day / nullIf(anyIf(units_per_product_day, discount_band = '0. không giảm') OVER (), 0), 2) AS index_vs_no_discount
FROM mart_price_discount_daily
WHERE is_reliable_day
  AND sale_event = ''
  AND full_date BETWEEN {date_from:Date} AND {date_to:Date}
  AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
GROUP BY discount_band
ORDER BY discount_band;


-- [4.3] Bảng điểm các ngày sale | Biểu đồ: table (mỗi dòng 1 sự kiện)
-- participants = sản phẩm ghi % giảm >= 10 trong ngày sale.
-- real_deal_pct = % sản phẩm tham gia có giá thật thấp hơn giá bình thường >= 5%.
-- lowest_30d_pct = % sản phẩm tham gia có giá <= giá thấp nhất 30 ngày trước (chuẩn Omnibus).
-- median_uplift = trung vị (bán ngày sale / bán TB ngày thường) ; dùng trung vị vì tỉ lệ bị lệch phải.
-- Lưu ý: 10.10 chỉ có 2 ngày baseline (dữ liệu bắt đầu 07/10) nên giá tham chiếu kém tin cậy hơn 11.11.
SELECT
    sale_event,
    event_date,
    count() AS products_on_event_day,
    countIf(shown_discount_pct >= 10) AS participants,
    round(participants / products_on_event_day * 100, 1) AS participation_pct,
    round(avgIf(shown_discount_pct, shown_discount_pct >= 10), 1) AS avg_shown_discount_pct,
    round(avgIf(real_discount_pct, shown_discount_pct >= 10), 1) AS avg_real_discount_pct,
    round(countIf(discount_verdict = 'giảm thật' AND shown_discount_pct >= 10) / nullIf(participants, 0) * 100, 1) AS real_deal_pct,
    round(countIf(is_lowest_30d AND shown_discount_pct >= 10) / nullIf(participants, 0) * 100, 1) AS lowest_30d_pct,
    countIf(discount_verdict = 'giảm giá ảo') AS fake_discount_products,
    round(fake_discount_products / nullIf(participants, 0) * 100, 1) AS fake_discount_pct,
    sum(units_event_day) AS units_event_day,
    sum(units_next_day) AS units_next_day,
    sum(revenue_event_day) AS revenue_event_day,
    round(quantileExactIf(0.5)(uplift_ratio, uplift_ratio IS NOT NULL), 2) AS median_uplift,
    round(avg(baseline_days), 1) AS avg_baseline_days
FROM mart_sale_event_product
WHERE {category_l1:String} = '' OR category_level_1 = {category_l1:String}
GROUP BY sale_event, event_date
ORDER BY event_date;


-- [4.4] Phân loại khuyến mãi trong ngày sale | Biểu đồ: 100% stacked bar (mỗi cột 1 sự kiện, màu = verdict)
-- kèm uplift trung vị của từng nhóm: giảm giá ảo có bán tốt hơn "không giảm" không?
SELECT
    sale_event,
    discount_verdict,
    count() AS products,
    round(count() / sum(count()) OVER (PARTITION BY sale_event) * 100, 1) AS products_pct,
    round(avg(shown_discount_pct), 1) AS avg_shown_discount_pct,
    round(avg(real_discount_pct), 1) AS avg_real_discount_pct,
    round(quantileExactIf(0.5)(uplift_ratio, uplift_ratio IS NOT NULL), 2) AS median_uplift,
    sum(revenue_event_day) AS revenue_event_day
FROM mart_sale_event_product
WHERE {category_l1:String} = '' OR category_level_1 = {category_l1:String}
GROUP BY sale_event, discount_verdict
ORDER BY sale_event, products DESC;


-- [4.5] % giảm hiển thị so với % giảm thật theo danh mục (1 sự kiện) | Biểu đồ: dumbbell / bar nhóm
-- Khoảng cách giữa 2 cột = phần "giảm trên giấy". Chỉ tính sản phẩm ghi giảm >= 10%.
SELECT
    category_level_1,
    countIf(shown_discount_pct >= 10) AS participants,
    round(avgIf(shown_discount_pct, shown_discount_pct >= 10), 1) AS avg_shown_discount_pct,
    round(avgIf(real_discount_pct, shown_discount_pct >= 10 AND real_discount_pct IS NOT NULL), 1) AS avg_real_discount_pct,
    round(avg_shown_discount_pct - avg_real_discount_pct, 1) AS gap_pct_points,
    countIf(discount_verdict = 'giảm giá ảo') AS fake_discount_products,
    round(fake_discount_products / nullIf(participants, 0) * 100, 1) AS fake_discount_pct
FROM mart_sale_event_product
WHERE sale_event = {sale_event:String}
GROUP BY category_level_1
HAVING participants >= 20
ORDER BY gap_pct_points DESC;


-- [4.6] Seller có nhiều sản phẩm giảm giá ảo nhất (1 sự kiện) | Biểu đồ: bar ngang top 15 + bảng
SELECT
    seller_name,
    if(any(seller_is_official), 'official', '') AS badge,
    count() AS products_on_event_day,
    countIf(discount_verdict = 'giảm giá ảo') AS fake_discount_products,
    round(fake_discount_products / count() * 100, 1) AS fake_discount_pct,
    round(avgIf(list_inflation_pct, discount_verdict = 'giảm giá ảo'), 1) AS avg_list_inflation_pct,
    round(avgIf(real_discount_pct, discount_verdict = 'giảm giá ảo'), 1) AS avg_real_discount_pct,
    sumIf(revenue_event_day, discount_verdict = 'giảm giá ảo') AS fake_discount_revenue,
    seller_key
FROM mart_sale_event_product
WHERE sale_event = {sale_event:String}
  AND seller_key != lower(hex(SHA256('unknown')))            -- bỏ member "Không rõ người bán"
  AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
GROUP BY seller_key, seller_name
HAVING fake_discount_products > 0
ORDER BY fake_discount_products DESC, fake_discount_revenue DESC
LIMIT 15;


-- [4.7] Danh sách sản phẩm giảm giá ảo (1 sự kiện) | Biểu đồ: table để drill-through sang Dashboard 6
SELECT
    e.product_id,
    p.title,
    e.category_level_1,
    e.brand_name,
    e.seller_name,
    e.ref_list_median_30d,
    e.event_list_price,
    round(e.list_inflation_pct, 1) AS list_inflation_pct,
    e.shown_discount_pct,
    e.ref_sell_median_30d,
    e.event_sell_price,
    round(e.real_discount_pct, 1) AS real_discount_pct,
    e.units_event_day,
    e.revenue_event_day
FROM mart_sale_event_product AS e
LEFT JOIN mart_product_scorecard AS p ON p.product_id = e.product_id
WHERE e.sale_event = {sale_event:String}
  AND e.discount_verdict = 'giảm giá ảo'
  AND ({category_l1:String} = '' OR e.category_level_1 = {category_l1:String})
ORDER BY e.revenue_event_day DESC
LIMIT 100;


-- [4.8] Phân bố mức đổi giá | Biểu đồ: histogram (bucket 5%, cắt ở -50% / +50%)
-- Đọc thẳng gold F3 (435k dòng, lọc theo ngày nên vẫn nhanh) vì mart không giữ từng lần đổi giá.
SELECT
    greatest(least(floor(change_pct / 5) * 5, 50), -50) AS change_pct_bucket,   -- -50 nghĩa là <= -50%
    countIf(direction = 'down') AS price_down,
    countIf(direction = 'up') AS price_up,
    countIf(is_list_price_inflated) AS inflated
FROM gold_fact_price_change FINAL
WHERE old_sell_price > 0 AND new_sell_price > 0
  AND direction != 'unchanged'
  AND toDate(changed_at) BETWEEN {date_from:Date} AND {date_to:Date}
GROUP BY change_pct_bucket
ORDER BY change_pct_bucket;
