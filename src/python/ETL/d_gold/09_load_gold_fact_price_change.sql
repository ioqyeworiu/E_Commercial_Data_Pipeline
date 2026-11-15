-- F3. 1 dòng / lần đổi giá bán HOẶC giá gốc.
-- Bắt cả trường hợp giá bán giữ nguyên nhưng giá gốc bị nâng lên để % giảm trông lớn hơn
-- (is_list_price_inflated) -> dùng để phát hiện "giảm giá ảo" trước các ngày sale.
INSERT INTO gold_fact_price_change (
    product_id,
    changed_at,
    date_key,
    product_key,
    brand_key,
    seller_key,
    category_key,
    old_sell_price,
    new_sell_price,
    change_amount,
    change_pct,
    direction,
    old_list_price,
    new_list_price,
    old_discount_pct,
    new_discount_pct,
    old_price_since,
    old_price_duration_hours,
    is_list_price_inflated
)
WITH
obs AS (
    SELECT
        product_id, observed_at, product_key, brand_key, seller_key, category_key,
        sell_price, list_price, discount_pct,
        lagInFrame(toNullable(sell_price), 1) OVER w AS prev_sell_price,
        lagInFrame(toNullable(list_price), 1) OVER w AS prev_list_price,
        lagInFrame(toNullable(discount_pct), 1) OVER w AS prev_discount_pct
    FROM gold_fact_product_snapshot FINAL
    WINDOW w AS (PARTITION BY product_id ORDER BY observed_at)
),
-- Điểm bắt đầu của mỗi mức giá (kể cả lần quan sát đầu tiên) -> lag cho biết mức giá cũ bắt đầu từ khi nào
segments AS (
    SELECT
        *,
        lagInFrame(observed_at, 1) OVER (PARTITION BY product_id ORDER BY observed_at) AS seg_old_since
    FROM obs
    WHERE prev_sell_price IS NULL
       OR sell_price != prev_sell_price
       OR list_price != prev_list_price
)
SELECT
    product_id,
    observed_at AS changed_at,
    toYYYYMMDD(observed_at) AS date_key,
    product_key,
    brand_key,
    seller_key,
    category_key,
    assumeNotNull(prev_sell_price) AS old_sell_price,
    sell_price AS new_sell_price,
    toInt64(new_sell_price) - toInt64(old_sell_price) AS change_amount,
    toFloat32(if(old_sell_price > 0, change_amount / old_sell_price * 100, 0)) AS change_pct,
    multiIf(new_sell_price > old_sell_price, 'up', new_sell_price < old_sell_price, 'down', 'unchanged') AS direction,
    assumeNotNull(prev_list_price) AS old_list_price,
    list_price AS new_list_price,
    assumeNotNull(prev_discount_pct) AS old_discount_pct,
    discount_pct AS new_discount_pct,
    seg_old_since AS old_price_since,
    toFloat32(dateDiff('second', seg_old_since, observed_at) / 3600) AS old_price_duration_hours,
    new_list_price > old_list_price AND new_discount_pct > old_discount_pct AS is_list_price_inflated
FROM segments
WHERE prev_sell_price IS NOT NULL;     -- bỏ lần quan sát đầu tiên (không phải là thay đổi)
