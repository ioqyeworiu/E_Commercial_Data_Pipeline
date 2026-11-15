-- =====================================================================================
-- BÁO CÁO CHẤT LƯỢNG DỮ LIỆU - đọc dq_check_result, mặc định lần chạy mới nhất.
-- Cùng 1 file cho 2 dạng đầu ra, chọn bằng --format của clickhouse-client:
--   --format=PrettyCompactMonoBlock  -> báo cáo chữ cho terminal (<run_id>_report.txt)
--   --format=Markdown                -> báo cáo Markdown (<run_id>_report.md)
-- Vì vậy các truy vấn số liệu KHÔNG ghi FORMAT ; chỉ dòng tiêu đề ghi FORMAT TSVRaw (in nguyên văn).
-- Chạy: bash run_dq.sh report   (tự xuất cả .txt, .md và .csv)
-- Chạy tay: clickhouse-client ... --format=Markdown --multiquery < 99_dq_report.sql > report.md
-- Muốn xem 1 lần chạy cũ: thay biểu thức rid ở các truy vấn bên dưới bằng 'run_id cần xem'.
-- =====================================================================================

WITH (SELECT argMax(run_id, run_at) FROM dq_check_result) AS rid
SELECT concat(
    '# Báo cáo chất lượng dữ liệu\n\n',
    '- run_id: `', rid, '`\n',
    '- database: `', currentDatabase(), '`\n',
    '- thời gian chạy check: ', toString(min(run_at)), ' → ', toString(max(run_at)), '\n',
    '- xuất báo cáo: ', toString(now()), '\n',
    '- chấm điểm: measured_value càng lớn càng xấu ; > fail_threshold là FAIL, > warn_threshold là WARN, ',
    'fail_threshold < 0 là INFO (chỉ theo dõi) ; ngưỡng ghi dạng warn / fail\n',
    '- scope: real = dữ liệu crawl thật (url_id < 1.000.000), test = dữ liệu sinh, all = bảng không có url_id')
FROM dq_check_result
WHERE run_id = rid
FORMAT TSVRaw;

SELECT '\n## 1. Lần chạy\n' FORMAT TSVRaw;
WITH (SELECT argMax(run_id, run_at) FROM dq_check_result) AS rid
SELECT
    rid AS `lần chạy`,            -- KHÔNG đặt alias là run_id: alias sẽ che cột run_id, WHERE run_id = rid thành luôn đúng
    min(run_at) AS `bắt đầu`,
    max(run_at) AS `kết thúc`,
    count() AS `số check`,
    countIf(status = 'PASS') AS pass,
    countIf(status = 'WARN') AS warn,
    countIf(status = 'FAIL') AS fail,
    countIf(status = 'INFO') AS info,
    round(countIf(status = 'PASS') * 100 / greatest(countIf(status != 'INFO'), 1), 1) AS pct_pass
FROM dq_check_result
WHERE run_id = rid;

SELECT '\n## 2. Tổng hợp theo tầng\n\npct_pass tính trên các check có chấm điểm (bỏ INFO).\n' FORMAT TSVRaw;
WITH (SELECT argMax(run_id, run_at) FROM dq_check_result) AS rid
SELECT
    layer AS `tầng`,
    count() AS `số check`,
    countIf(status = 'PASS') AS pass,
    countIf(status = 'WARN') AS warn,
    countIf(status = 'FAIL') AS fail,
    countIf(status = 'INFO') AS info,
    round(countIf(status = 'PASS') * 100 / greatest(countIf(status != 'INFO'), 1), 1) AS pct_pass
FROM dq_check_result
WHERE run_id = rid
GROUP BY layer
ORDER BY indexOf(['raw', 'bronze', 'silver', 'gold', 'cross'], layer);

SELECT '\n## 3. Tổng hợp theo chiều chất lượng\n' FORMAT TSVRaw;
WITH (SELECT argMax(run_id, run_at) FROM dq_check_result) AS rid
SELECT
    dq_dimension AS `chiều`,
    count() AS `số check`,
    countIf(status = 'PASS') AS pass,
    countIf(status = 'WARN') AS warn,
    countIf(status = 'FAIL') AS fail,
    countIf(status = 'INFO') AS info
FROM dq_check_result
WHERE run_id = rid
GROUP BY dq_dimension
ORDER BY fail DESC, warn DESC, `chiều`;

SELECT '\n## 4. Các check FAIL / WARN\n\nFAIL trước, rồi theo tầng. Cột `giá trị` so với ngưỡng warn / fail, đơn vị ở cột `đơn vị`.\n' FORMAT TSVRaw;
WITH (SELECT argMax(run_id, run_at) FROM dq_check_result) AS rid
SELECT
    status,
    layer AS `tầng`,
    table_name AS `bảng`,
    check_id,
    dq_cell(column_name, 40) AS `cột`,
    scope,
    failed_rows AS `lỗi`,
    total_rows AS `tổng`,
    round(measured_value, 4) AS `giá trị`,
    unit AS `đơn vị`,
    concat(toString(warn_threshold), ' / ', toString(fail_threshold)) AS `ngưỡng`,
    dq_cell(check_name, 80) AS `kiểm tra`,
    dq_cell(sample_value, 60) AS `mẫu lỗi`,
    dq_cell(note, 100) AS `ghi chú`
FROM dq_check_result
WHERE run_id = rid AND status IN ('FAIL', 'WARN')
ORDER BY status = 'WARN', indexOf(['raw', 'bronze', 'silver', 'gold', 'cross'], layer), table_name, check_id, column_name;

SELECT '\n## 5. Các chỉ số theo dõi (INFO)\n\nChỉ đo, không chấm điểm ; chỉ liệt kê chỉ số có failed_rows > 0.\n' FORMAT TSVRaw;
WITH (SELECT argMax(run_id, run_at) FROM dq_check_result) AS rid
SELECT
    layer AS `tầng`,
    table_name AS `bảng`,
    check_id,
    dq_cell(column_name, 40) AS `cột`,
    scope,
    failed_rows AS `số lượng`,
    total_rows AS `tổng`,
    round(measured_value, 4) AS `giá trị`,
    unit AS `đơn vị`,
    dq_cell(check_name, 80) AS `chỉ số`,
    dq_cell(sample_value, 80) AS `mẫu`
FROM dq_check_result
WHERE run_id = rid AND status = 'INFO' AND failed_rows > 0
ORDER BY indexOf(['raw', 'bronze', 'silver', 'gold', 'cross'], layer), table_name, check_id, column_name;

SELECT '\n## 6. Thay đổi so với lần chạy trước\n\nCác check đổi trạng thái. Bảng trống (hoặc không có bảng) nghĩa là không check nào đổi trạng thái, hoặc chưa có lần chạy trước.\n' FORMAT TSVRaw;
WITH
    (SELECT argMax(run_id, run_at) FROM dq_check_result) AS rid,
    (SELECT argMax(run_id, run_at) FROM dq_check_result WHERE run_id != rid) AS prev_rid
SELECT
    cur.layer AS `tầng`,
    cur.table_name AS `bảng`,
    cur.check_id AS check_id,
    dq_cell(cur.column_name, 40) AS `cột`,
    cur.scope AS scope,
    prev.status AS `trước`,
    cur.status AS `nay`,
    prev.failed_rows AS `lỗi trước`,
    cur.failed_rows AS `lỗi nay`
FROM (SELECT * FROM dq_check_result WHERE run_id = rid) AS cur
INNER JOIN (SELECT * FROM dq_check_result WHERE run_id = prev_rid) AS prev
    ON prev.layer = cur.layer AND prev.table_name = cur.table_name AND prev.check_id = cur.check_id
   AND prev.column_name = cur.column_name AND prev.scope = cur.scope
   AND prev.check_name = cur.check_name   -- B05 có 2 check cùng cột brand_normalized ; thiếu điều kiện này sẽ nối chéo
WHERE prev.status != cur.status
ORDER BY indexOf(['raw', 'bronze', 'silver', 'gold', 'cross'], `tầng`), `bảng`, check_id;

SELECT '\n## 7. Xu hướng 10 lần chạy gần nhất\n' FORMAT TSVRaw;
SELECT
    run_id,
    min(run_at) AS `bắt đầu`,
    count() AS `số check`,
    countIf(status = 'PASS') AS pass,
    countIf(status = 'WARN') AS warn,
    countIf(status = 'FAIL') AS fail,
    round(countIf(status = 'PASS') * 100 / greatest(countIf(status != 'INFO'), 1), 1) AS pct_pass
FROM dq_check_result
GROUP BY run_id
ORDER BY `bắt đầu` DESC
LIMIT 10;
