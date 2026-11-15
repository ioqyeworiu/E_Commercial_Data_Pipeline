-- =====================================================================================
-- DASHBOARD 6: SẢN PHẨM (danh sách + trang chi tiết drill-through)
-- Câu hỏi: sản phẩm nào bán chạy / đang lên / đáng lo ; một sản phẩm cụ thể có lịch sử giá, bán,
-- review, thay đổi thuộc tính thế nào.
--
-- Tham số: {category_l1:String} ('' = tất cả), {product_id:Int64} cho [6.4] - [6.8],
--          {date_from:Date}, {date_to:Date} cho [6.5].
-- [6.1] - [6.4] đọc mart_product_scorecard (toàn kỳ ; "7 ngày" tính tới as_of_date).
-- [6.5] - [6.7] đọc thẳng gold vì cần từng ngày / từng lần đổi giá của MỘT sản phẩm: lọc theo product_id
-- là cột đầu của ORDER BY nên ClickHouse chỉ đọc vài granule, chạy dưới 0,1 giây.
-- =====================================================================================


-- [6.1] Top 20 sản phẩm theo doanh thu | Biểu đồ: table (title là link sang trang chi tiết)
SELECT
    revenue_rank,
    product_id,
    title,
    category_level_1,
    brand_name,
    seller_name,
    current_sell_price,
    current_discount_pct,
    units_sold_total,
    revenue_total,
    round(revenue_cum_share_pct, 1) AS cum_share_pct,
    abc_class,
    sales_trend,
    round(avg_star, 2) AS avg_star,
    n_reviews,
    inflated_events
FROM mart_product_scorecard
WHERE {category_l1:String} = '' OR category_level_1 = {category_l1:String}
ORDER BY revenue_total DESC, product_id
LIMIT 20;


-- [6.2] Sản phẩm đang lên: tăng trưởng 7 ngày cao nhất | Biểu đồ: table hoặc bar ngang
-- Tối thiểu 50 sản phẩm bán ở 7 ngày trước để % tăng không bị phóng đại từ nền quá nhỏ.
-- Lưu ý: nếu 7 ngày gần nhất chứa ngày sale thì gần như mọi sản phẩm đều "tăng".
SELECT
    product_id,
    title,
    category_level_1,
    brand_name,
    units_prev_7d,
    units_last_7d,
    round(growth_7d_pct, 1) AS growth_7d_pct,
    current_sell_price,
    current_discount_pct,
    round(avg_star, 2) AS avg_star,
    any(as_of_date) OVER () AS as_of_date
FROM mart_product_scorecard
WHERE units_prev_7d >= 50
  AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
ORDER BY growth_7d_pct DESC, units_last_7d DESC
LIMIT 20;


-- [6.3] Sản phẩm doanh thu cao nhưng bị chê | Biểu đồ: table có cờ cảnh báo
-- Nhóm A (tạo 80% doanh thu) có sao TB < 4 hoặc >= 15% review 1-2 sao (tối thiểu 10 review có sao).
SELECT
    product_id,
    title,
    category_level_1,
    brand_name,
    seller_name,
    revenue_total,
    revenue_rank,
    n_rated_reviews,
    round(avg_star, 2) AS avg_star,
    round(negative_pct, 1) AS negative_pct,
    star_1 + star_2 AS negative_reviews,
    n_with_image,
    rating_latest                                                  -- rating hiển thị trên trang Tiki
FROM mart_product_scorecard
WHERE abc_class = 'A'
  AND n_rated_reviews >= 10
  AND (avg_star < 4 OR negative_pct >= 15)
  AND ({category_l1:String} = '' OR category_level_1 = {category_l1:String})
ORDER BY revenue_total DESC
LIMIT 50;


-- [6.4] Thẻ thông tin sản phẩm | Biểu đồ: KPI cards / bảng dọc (1 dòng)
SELECT *
FROM mart_product_scorecard
WHERE product_id = {product_id:Int64};


-- [6.5] Diễn biến giá và lượng bán theo ngày | Biểu đồ: combo (đường = giá bán đóng cửa + giá gốc,
-- cột = units_sold_est) ; vùng tô min-max giá trong ngày. Cột price_change_cnt để đánh dấu ngày đổi giá.
SELECT
    d.full_date,
    d.sale_event,
    f.open_sell_price,
    f.close_sell_price,
    f.min_sell_price,
    f.max_sell_price,
    f.close_list_price,
    round(f.avg_discount_pct, 1) AS avg_discount_pct,
    f.units_sold_est,
    f.revenue_est,
    f.sold_eod,
    f.rating_eod,
    f.rating_cnt_eod,
    f.price_change_cnt,
    f.n_observations
FROM gold_fact_product_daily AS f FINAL
INNER JOIN gold_dim_date AS d FINAL ON d.date_key = f.date_key
WHERE f.product_id = {product_id:Int64}
  AND d.full_date BETWEEN {date_from:Date} AND {date_to:Date}
ORDER BY d.full_date;


-- [6.6] Nhật ký đổi giá | Biểu đồ: table (tô đỏ dòng nghi giảm giá ảo)
SELECT
    changed_at,
    direction,
    old_sell_price,
    new_sell_price,
    round(change_pct, 1) AS change_pct,
    old_list_price,
    new_list_price,
    old_discount_pct,
    new_discount_pct,
    round(old_price_duration_hours, 1) AS old_price_held_hours,
    is_list_price_inflated
FROM gold_fact_price_change FINAL
WHERE product_id = {product_id:Int64}
  AND old_sell_price > 0 AND new_sell_price > 0                   -- bỏ đổi giá giả do nguồn thiếu giá
ORDER BY changed_at;


-- [6.7] Lịch sử thuộc tính (SCD Type 2) | Biểu đồ: timeline / table
SELECT
    version_no,
    title,
    made_in,
    warranty,
    valid_from,
    valid_to,
    is_current,
    round(dateDiff('hour', valid_from, ifNull(valid_to, now())) / 24, 1) AS days_valid
FROM gold_dim_product FINAL
WHERE product_id = {product_id:Int64}
ORDER BY version_no;


-- [6.8] Sản phẩm trong các ngày sale | Biểu đồ: table (1 dòng / sự kiện)
SELECT
    sale_event,
    event_date,
    discount_verdict,
    ref_sell_median_30d,
    ref_sell_min_30d,
    event_sell_price,
    shown_discount_pct,
    round(real_discount_pct, 1) AS real_discount_pct,
    ref_list_median_30d,
    event_list_price,
    round(list_inflation_pct, 1) AS list_inflation_pct,
    is_lowest_30d,
    round(baseline_units_avg, 1) AS baseline_units_avg,
    units_event_day,
    units_next_day,
    round(uplift_ratio, 2) AS uplift_ratio
FROM mart_sale_event_product
WHERE product_id = {product_id:Int64}
ORDER BY event_date;
