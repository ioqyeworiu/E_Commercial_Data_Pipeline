-- =====================================================================================
-- DATA QUALITY: bảng lưu kết quả + hàm dùng chung. Chạy 1 lần trước các file 01..05
-- (run_dq.sh tự chạy file này ở mọi lần, an toàn vì dùng IF NOT EXISTS / OR REPLACE).
--
-- Mỗi check ghi 1 dòng (hoặc vài dòng: theo cột, theo scope) vào dq_check_result:
--   measured_value  : giá trị đem so ngưỡng. Mặc định là % dòng lỗi (unit = '%'),
--                     một số check đo giờ ('giờ'), phút ('phút') hoặc hệ số ('x').
--   Quy ước: measured_value CÀNG LỚN CÀNG XẤU.
--     measured_value >  fail_threshold -> FAIL
--     measured_value >  warn_threshold -> WARN
--     còn lại                          -> PASS
--     fail_threshold < 0               -> INFO (chỉ đo để theo dõi, không chấm điểm)
--   Check "phải bằng 0" dùng warn = 0, fail = 0.
--   scope: 'real' = dữ liệu crawl thật (url_id < 1.000.000), 'test' = dữ liệu sinh, 'all' = bảng không có url_id.
--
-- Chạy cả bộ: bash src/sql_script/clickhouse/data_quality/run_dq.sh all
-- Chạy tay 1 file: thêm --param_run_id=manual vào clickhouse-client (các file 01..05 dùng {run_id:String}).
-- =====================================================================================

CREATE TABLE IF NOT EXISTS dq_check_result(
    run_id String,
    run_at DATETIME('Asia/Ho_Chi_Minh') DEFAULT now(),
    layer LowCardinality(String),            -- raw | bronze | silver | gold | cross
    table_name LowCardinality(String),
    check_id LowCardinality(String),         -- R03, B01, ... (xem đầu mỗi file)
    check_name String,
    dq_dimension LowCardinality(String),     -- volume | timeliness | completeness | validity | uniqueness
                                             -- | consistency | integrity | accuracy | reconciliation
    column_name String,                      -- cột / biến thể của check ; '' nếu check cả dòng
    scope LowCardinality(String),            -- real | test | all
    total_rows UInt64,                       -- mẫu số (số dòng / số khóa / số ngày ... được kiểm tra)
    failed_rows UInt64,                      -- số phần tử vi phạm
    measured_value Float64,
    unit LowCardinality(String),
    warn_threshold Float64,
    fail_threshold Float64,
    status LowCardinality(String) DEFAULT multiIf(
        fail_threshold < 0, 'INFO',
        measured_value > fail_threshold, 'FAIL',
        measured_value > warn_threshold, 'WARN',
        'PASS'),
    sample_value String,                     -- ví dụ giá trị lỗi để debug
    note String
)
ENGINE MergeTree
PARTITION BY toYYYYMM(run_at)
ORDER BY (run_at, layer, table_name, check_id, column_name, scope);



-- ---------------------------------------------------------------- HÀM DÙNG CHUNG
-- SQL UDF là đối tượng toàn server (giống mart_price_band ở mart_ddl.sql).

-- Phân loại dòng theo nguồn: bộ dữ liệu test luôn có url_id >= 1.000.000
CREATE OR REPLACE FUNCTION dq_scope AS (id) -> if(id >= 1000000, 'test', 'real');

-- % lỗi. KHÔNG làm tròn: 1 dòng lỗi / 10 triệu vẫn phải > 0 để check "phải bằng 0" ra FAIL.
CREATE OR REPLACE FUNCTION dq_pct AS (failed, total) -> if(total = 0, 0., failed * 100. / total);

-- Giá trị mẫu chỉ có nghĩa khi có lỗi: anyIf() trả giá trị mặc định (0, '1970-01-01', (0,0)...) khi không có
-- dòng nào khớp, nên để trống trong trường hợp đó.
CREATE OR REPLACE FUNCTION dq_sample AS (failed, s) -> if(failed = 0, '', toString(s));

-- Ô văn bản trong báo cáo (99_dq_report.sql): cắt theo KÝ TỰ (substring cắt theo byte làm vỡ chữ tiếng Việt),
-- bỏ xuống dòng / tab, đổi '|' thành '¦' để không vỡ bảng Markdown.
CREATE OR REPLACE FUNCTION dq_cell AS (s, n) -> leftUTF8(replaceRegexpAll(replaceAll(s, '|', '¦'), '[\\r\\n\\t]+', ' '), n);

-- Ô raw được coi là thiếu: rỗng hoặc các chuỗi giữ chỗ hay gặp khi selector không tìm thấy phần tử
CREATE OR REPLACE FUNCTION dq_missing AS (s) ->
    upper(trimBoth(s)) IN ('', 'NULL', 'NAN', 'NONE', 'N/A', 'UNDEFINED', '[]');

-- Đọc observed_at dạng ISO-8601 của crawler ('2026-10-25T04:11:26.361471+07:00'); lỗi -> NULL
CREATE OR REPLACE FUNCTION dq_ts AS (s) -> parseDateTime64BestEffortOrNull(s, 6, 'Asia/Ho_Chi_Minh');

-- Giá trị kỳ vọng của bộ đếm dạng chữ, tính ĐỘC LẬP với Spark để đối chiếu bronze:
--   '1.234' -> 1234 ; '(37.5k+ đánh giá)' -> 37500 ; '(5.6tr+ đánh giá)' -> 5600000 ; '1k' -> 1000
CREATE OR REPLACE FUNCTION dq_expected_count AS (s) -> multiIf(
    match(lowerUTF8(s), '[0-9]([.,][0-9]+)?\\s*(tr|triệu)'),
        round(toFloat64OrZero(replaceOne(extract(lowerUTF8(s), '([0-9]+(?:[.,][0-9]+)?)\\s*(?:tr|triệu)'), ',', '.')) * 1000000),
    match(lowerUTF8(s), '[0-9]([.,][0-9]+)?\\s*(k|nghìn|ngàn)'),
        round(toFloat64OrZero(replaceOne(extract(lowerUTF8(s), '([0-9]+(?:[.,][0-9]+)?)\\s*(?:k|nghìn|ngàn)'), ',', '.')) * 1000),
    toFloat64OrZero(replaceRegexpAll(s, '[^0-9]', '')));

-- Số giờ kỳ vọng của chuỗi thời lượng review, cùng quy tắc với to_hours() ở bronze
-- (số đầu tiên x đơn vị, xét đơn vị theo thứ tự năm > tháng > tuần > ngày > giờ > phút):
--   'Đã tham gia 7 năm' -> 61320 ; 'Đánh giá vào 5 tháng trước' -> 3650 ; '' -> NULL
CREATE OR REPLACE FUNCTION dq_hours AS (s) ->
    toFloat64OrNull(extract(lowerUTF8(s), '([0-9]+)')) * multiIf(
        position(lowerUTF8(s), 'năm') > 0, 8760.,
        position(lowerUTF8(s), 'tháng') > 0, 730.,
        position(lowerUTF8(s), 'tuần') > 0, 168.,
        position(lowerUTF8(s), 'ngày') > 0, 24.,
        position(lowerUTF8(s), 'giờ') > 0, 1.,
        position(lowerUTF8(s), 'phút') > 0, 1. / 60,
        NULL);

-- Khóa so khớp tên (brand / seller) giống normalize_brand ở bronze: bỏ dấu, bỏ hậu tố official,
-- bỏ ký tự không phải chữ/số. Dùng để tìm các tên CHƯA được gộp.
CREATE OR REPLACE FUNCTION dq_name_key AS (s) -> replaceRegexpAll(
    replaceRegexpAll(
        replaceRegexpAll(
            translateUTF8(lowerUTF8(trimBoth(s)),
                'àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđäëïöüçñ',
                'aaaaaaaaaaaaaaaaaeeeeeeeeeeeiiiiiooooooooooooooooouuuuuuuuuuuyyyyydaeioucn'),
            '\\p{M}', ''),                                   -- dấu ở dạng tổ hợp (NFD)
        '\\b(official stores?|flagship store|official|offical|chinh hang)\\b', ' '),
    '[^\\p{L}\\p{N}]', '');
