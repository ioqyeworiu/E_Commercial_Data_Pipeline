-- M7. 1 dòng / ngày sale / sản phẩm có mặt vào ngày sale. Chạy sau 01.
-- Nguồn: F1 (giá theo từng lần crawl, bỏ giá 0), F2 (lượng bán), gold_dim_date, dim brand/seller/category.
--
-- Ngày sale = gold_dim_date.sale_event != '' và nằm trong khoảng ngày có dữ liệu.
-- Cửa sổ của sự kiện D: [D-30, D+1].
--   - Ngày baseline: các ngày trong [D-30, D-1] KHÔNG phải ngày sale.
--   - Giá tham chiếu:
--       ref_sell_median_30d = trung vị (theo ngày) của giá bán ngày baseline -> giá "bình thường"
--       ref_sell_min_30d    = giá bán thấp nhất mọi ngày trong [D-30, D-1]  -> như Chỉ thị Omnibus (EU)
--       ref_list_median_30d = trung vị giá gốc ngày baseline
--   - Lượng bán nền: TB units_sold_est các ngày baseline, bỏ thêm ngày ngay sau ngày sale (bị dồn số bán)
--     và ngày có days_since_prev_obs > 1 (gộp nhiều ngày).
--
-- discount_verdict:
--   'không đủ dữ liệu' : không có giá ngày sale hoặc không có ngày baseline
--   'giảm giá ảo'      : giá gốc bị nâng >= 10% so với bình thường NHƯNG giá bán thật giảm < 5%
--   'giảm thật'        : giá bán thấp hơn giá bình thường >= 5%
--   'giảm danh nghĩa'  : có ghi % giảm >= 10 nhưng giá bán không thấp hơn bình thường (giảm < 5%)
--   'không giảm'       : còn lại
DROP TABLE IF EXISTS mart_sale_event_product__stage;
CREATE TABLE mart_sale_event_product__stage AS mart_sale_event_product;

INSERT INTO mart_sale_event_product__stage (
    event_date,
    event_date_key,
    sale_event,
    product_id,
    category_level_1,
    category_level_2,
    brand_key,
    brand_name,
    seller_key,
    seller_name,
    seller_is_official,
    price_band,
    baseline_days,
    ref_sell_median_30d,
    ref_sell_min_30d,
    ref_list_median_30d,
    event_sell_price,
    event_list_price,
    shown_discount_pct,
    real_discount_pct,
    list_inflation_pct,
    is_lowest_30d,
    discount_verdict,
    baseline_units_days,
    baseline_units_avg,
    units_event_day,
    units_next_day,
    revenue_event_day,
    uplift_ratio
)
WITH
    (SELECT groupArray(full_date) FROM gold_dim_date FINAL WHERE sale_event != '') AS sale_dates,
    (SELECT min(full_date) FROM mart_kpi_daily) AS data_from,
    (SELECT max(full_date) FROM mart_kpi_daily) AS data_to,
    events AS (
        SELECT full_date AS e_date, date_key AS e_date_key, sale_event AS e_sale_event
        FROM gold_dim_date FINAL
        WHERE sale_event != '' AND full_date BETWEEN data_from AND data_to
    ),
    -- giá sạch theo ngày từ F1: bỏ lần crawl thiếu giá (sell_price = 0)
    day_price AS (
        SELECT
            product_id AS dp_product_id,
            toDate(observed_at) AS dp_date,
            toUInt64(medianExact(sell_price)) AS dp_med_sell,
            min(sell_price) AS dp_min_sell,
            toUInt64(medianExact(list_price)) AS dp_med_list,
            max(discount_pct) AS dp_max_discount
        FROM gold_fact_product_snapshot FINAL
        WHERE sell_price > 0
          AND toDate(observed_at) >= (SELECT min(e_date) FROM events) - 30
        GROUP BY dp_product_id, dp_date
    ),
    day_units AS (
        SELECT
            product_id AS du_product_id,
            toDate(first_observed_at) AS du_date,
            date_key AS du_date_key,
            units_sold_est AS du_units,
            revenue_est AS du_revenue,
            days_since_prev_obs AS du_gap,
            category_key AS du_category_key,
            brand_key AS du_brand_key,
            seller_key AS du_seller_key
        FROM gold_fact_product_daily FINAL
        WHERE toDate(first_observed_at) >= (SELECT min(e_date) FROM events) - 30
    ),
    daily AS (
        SELECT
            u.*,
            p.dp_med_sell,
            p.dp_min_sell,
            p.dp_med_list,
            p.dp_max_discount
        FROM day_units AS u
        LEFT JOIN day_price AS p ON p.dp_product_id = u.du_product_id AND p.dp_date = u.du_date
    ),
    per_event AS (
        SELECT
            e.e_date AS pe_date,
            any(e.e_date_key) AS pe_date_key,
            any(e.e_sale_event) AS pe_sale_event,
            x.du_product_id AS pe_product_id,
            -- khóa dim theo ngày sale
            anyIf(x.du_category_key, x.du_date = e.e_date) AS pe_category_key,
            anyIf(x.du_brand_key, x.du_date = e.e_date) AS pe_brand_key,
            anyIf(x.du_seller_key, x.du_date = e.e_date) AS pe_seller_key,
            -- giá tham chiếu
            toUInt8(countIf(x.du_date < e.e_date AND NOT has(sale_dates, x.du_date) AND x.dp_med_sell > 0)) AS pe_baseline_days,
            medianExactIf(x.dp_med_sell, x.du_date < e.e_date AND NOT has(sale_dates, x.du_date) AND x.dp_med_sell > 0) AS pe_ref_median,
            minIf(x.dp_min_sell, x.du_date < e.e_date AND x.dp_min_sell > 0) AS pe_ref_min,
            medianExactIf(x.dp_med_list, x.du_date < e.e_date AND NOT has(sale_dates, x.du_date) AND x.dp_med_sell > 0) AS pe_ref_list,
            -- giá ngày sale (0 = không có giá sạch)
            maxIf(x.dp_med_sell, x.du_date = e.e_date) AS pe_event_sell,
            maxIf(x.dp_med_list, x.du_date = e.e_date) AS pe_event_list,
            maxIf(x.dp_max_discount, x.du_date = e.e_date) AS pe_shown_discount,
            -- lượng bán
            toUInt8(countIf(x.du_date < e.e_date AND NOT has(sale_dates, x.du_date) AND NOT has(sale_dates, x.du_date - 1)
                            AND x.du_units IS NOT NULL AND x.du_gap = 1)) AS pe_baseline_units_days,
            avgIf(x.du_units, x.du_date < e.e_date AND NOT has(sale_dates, x.du_date) AND NOT has(sale_dates, x.du_date - 1)
                              AND x.du_units IS NOT NULL AND x.du_gap = 1) AS pe_baseline_units,
            toUInt32(ifNull(sumIf(x.du_units, x.du_date = e.e_date), 0)) AS pe_units_event,
            toUInt32(ifNull(sumIf(x.du_units, x.du_date = e.e_date + 1), 0)) AS pe_units_next,
            toUInt64(ifNull(sumIf(x.du_revenue, x.du_date = e.e_date), 0)) AS pe_revenue_event,
            countIf(x.du_date = e.e_date) AS pe_on_event_day
        FROM daily AS x
        CROSS JOIN events AS e
        WHERE x.du_date BETWEEN e.e_date - 30 AND e.e_date + 1
        GROUP BY pe_date, pe_product_id
        HAVING pe_on_event_day > 0
    ),
    calc AS (
        SELECT
            *,
            if(pe_baseline_days = 0, NULL, toUInt64(pe_ref_median)) AS c_ref_median,
            if(pe_ref_min = 0, NULL, pe_ref_min) AS c_ref_min,
            if(pe_baseline_days = 0, NULL, toUInt64(pe_ref_list)) AS c_ref_list,
            if(pe_event_sell = 0, NULL, pe_event_sell) AS c_event_sell,
            if(pe_event_sell = 0, NULL, pe_event_list) AS c_event_list,
            if(c_ref_median IS NULL OR c_event_sell IS NULL, NULL,
               toFloat32((toInt64(c_ref_median) - toInt64(c_event_sell)) / c_ref_median * 100)) AS c_real_discount,
            if(c_ref_list IS NULL OR c_event_list IS NULL OR c_ref_list = 0, NULL,
               toFloat32((toInt64(c_event_list) - toInt64(c_ref_list)) / c_ref_list * 100)) AS c_list_inflation,
            if(pe_baseline_units_days = 0 OR isNaN(pe_baseline_units), NULL, pe_baseline_units) AS c_baseline_units
        FROM per_event
    )
SELECT
    pe_date,
    pe_date_key,
    pe_sale_event,
    pe_product_id,
    if(c.category_level_1 = '', 'unknown', c.category_level_1),
    c.category_level_2,
    pe_brand_key,
    if(b.brand_name = '', 'Không rõ thương hiệu', b.brand_name),
    pe_seller_key,
    if(s.seller_name = '', 'Không rõ người bán', s.seller_name),
    s.is_official,
    mart_price_band(ifNull(c_ref_median, 0)),
    pe_baseline_days,
    c_ref_median,
    c_ref_min,
    c_ref_list,
    c_event_sell,
    c_event_list,
    pe_shown_discount,
    c_real_discount,
    c_list_inflation,
    ifNull(c_event_sell <= c_ref_min, false),
    multiIf(
        c_event_sell IS NULL OR c_ref_median IS NULL, 'không đủ dữ liệu',
        c_list_inflation >= 10 AND c_real_discount < 5, 'giảm giá ảo',
        c_real_discount >= 5, 'giảm thật',
        pe_shown_discount >= 10, 'giảm danh nghĩa',
        'không giảm'),
    pe_baseline_units_days,
    c_baseline_units,
    pe_units_event,
    pe_units_next,
    pe_revenue_event,
    if(c_baseline_units IS NULL OR c_baseline_units = 0, NULL, toFloat32(pe_units_event / c_baseline_units))
FROM calc
LEFT JOIN gold_dim_category AS c FINAL ON c.category_key = calc.pe_category_key
LEFT JOIN gold_dim_brand AS b FINAL ON b.brand_key = calc.pe_brand_key
LEFT JOIN gold_dim_seller AS s FINAL ON s.seller_key = calc.pe_seller_key;

ALTER TABLE mart_sale_event_product REPLACE PARTITION tuple() FROM mart_sale_event_product__stage;
DROP TABLE mart_sale_event_product__stage;
