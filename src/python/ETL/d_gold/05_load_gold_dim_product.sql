-- SCD Type 2 cho sản phẩm.
-- Mỗi product_sk của silver là 1 version. Gold tính lại toàn bộ lịch sử version theo url_id:
--   valid_from = lần đầu version xuất hiện trong silver_snapshot
--   valid_to   = valid_from của version kế tiếp (NULL nếu là version mới nhất)
-- Vì tính lại toàn bộ + ReplacingMergeTree nên version cũ được "đóng" (valid_to, is_current)
-- mà không cần ALTER TABLE ... UPDATE.
INSERT INTO gold_dim_product (
    product_key,
    product_id,
    title,
    made_in,
    warranty,
    description,
    version_no,
    valid_from,
    valid_to,
    is_current
)
SELECT
    product_key,
    product_id,
    title,
    made_in,
    warranty,
    description,
    version_no,
    valid_from,
    valid_to,
    valid_to IS NULL AS is_current
FROM (
    SELECT
        *,
        toUInt16(row_number() OVER (PARTITION BY product_id ORDER BY valid_from, product_key)) AS version_no,
        leadInFrame(toNullable(valid_from), 1) OVER (
            PARTITION BY product_id ORDER BY valid_from, product_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS valid_to
    FROM (
        SELECT
            p.product_sk AS product_key,
            p.url_id AS product_id,
            p.title AS title,
            p.made_in AS made_in,
            p.warranty AS warranty,
            p.description AS description,
            if(s.product_sk = '', p.valid_from, s.first_seen) AS valid_from
        FROM (
            SELECT product_sk, url_id, title, made_in, warranty, description, valid_from
            FROM silver_product
            WHERE url_id > 0              -- dữ liệu nạp trước khi có cột url_id sẽ có url_id = 0
            ORDER BY valid_from ASC
            LIMIT 1 BY product_sk
        ) AS p
        LEFT JOIN (
            SELECT product_sk, min(observed_at) AS first_seen
            FROM silver_snapshot
            GROUP BY product_sk
        ) AS s ON s.product_sk = p.product_sk
    )
);
