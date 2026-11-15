-- Sinh lịch 2015-01-01 -> 2030-12-31 (review trên Tiki có thể từ nhiều năm trước).
-- Chạy 1 lần là đủ ; chạy lại vẫn an toàn (ReplacingMergeTree).
INSERT INTO gold_dim_date (
    date_key,
    full_date,
    day,
    day_of_week,
    day_name,
    week_of_year,
    month,
    quarter,
    year,
    year_month,
    is_weekend,
    is_double_day,
    is_black_friday,
    is_tet,
    holiday_name,
    sale_event
)
WITH
    -- Mùng 1 Tết Nguyên Đán (âm lịch, không tính được bằng hàm -> bảng tra)
    [
        toDate('2015-02-19'), toDate('2016-02-08'), toDate('2017-01-28'), toDate('2018-02-16'),
        toDate('2019-02-05'), toDate('2020-01-25'), toDate('2021-02-12'), toDate('2022-02-01'),
        toDate('2023-01-22'), toDate('2024-02-10'), toDate('2025-01-29'), toDate('2026-02-17'),
        toDate('2027-02-06'), toDate('2028-01-26'), toDate('2029-02-13'), toDate('2030-02-03')
    ] AS tet_days,
    toDate('2015-01-01') AS start_date,
    toDate('2030-12-31') AS end_date
SELECT
    toYYYYMMDD(d) AS date_key,
    d AS full_date,
    toDayOfMonth(d) AS day,
    toDayOfWeek(d) AS day_of_week,
    ['Thứ Hai', 'Thứ Ba', 'Thứ Tư', 'Thứ Năm', 'Thứ Sáu', 'Thứ Bảy', 'Chủ Nhật'][toDayOfWeek(d)] AS day_name,
    toISOWeek(d) AS week_of_year,
    toMonth(d) AS month,
    toQuarter(d) AS quarter,
    toYear(d) AS year,
    toYYYYMM(d) AS year_month,
    toDayOfWeek(d) >= 6 AS is_weekend,
    toDayOfMonth(d) = toMonth(d) AS is_double_day,
    -- Black Friday = ngày sau Lễ Tạ ơn (thứ Năm thứ 4 của tháng 11) -> thứ Sáu từ 23 đến 29/11
    toMonth(d) = 11 AND toDayOfWeek(d) = 5 AND toDayOfMonth(d) BETWEEN 23 AND 29 AS is_black_friday,
    arrayExists(t -> d >= t AND d < t + 3, tet_days) AS is_tet,
    multiIf(
        toMonth(d) = 1 AND toDayOfMonth(d) = 1, 'Tết Dương lịch',
        is_tet, 'Tết Nguyên Đán',
        toMonth(d) = 4 AND toDayOfMonth(d) = 30, 'Giải phóng miền Nam',
        toMonth(d) = 5 AND toDayOfMonth(d) = 1, 'Quốc tế Lao động',
        toMonth(d) = 9 AND toDayOfMonth(d) = 2, 'Quốc khánh',
        ''
    ) AS holiday_name,
    multiIf(
        is_black_friday, 'Black Friday',
        is_double_day, concat(toString(toDayOfMonth(d)), '.', toString(toMonth(d))),
        ''
    ) AS sale_event
FROM (
    SELECT start_date + number AS d
    FROM numbers(dateDiff('day', start_date, end_date) + 1)
);
