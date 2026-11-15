-- Xóa dữ liệu TEST (url_id >= 1.000.000) khỏi tầng raw. Dữ liệu crawl thật (url_id < 1.000.000) giữ nguyên.
-- bronze / silver / gold là dữ liệu dẫn xuất -> sau khi xóa raw thì dựng lại:
--     bash src/sql_script/clickhouse/test_data/run_test_pipeline.sh cleanup
-- (lệnh cleanup = chạy file này + bronze + silver + gold)
DELETE FROM raw_product_snapshot WHERE toInt64OrZero(url_id) >= 1000000;
DELETE FROM raw_user_review WHERE toInt64OrZero(url_id) >= 1000000;
