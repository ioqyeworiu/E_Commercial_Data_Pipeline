#!/usr/bin/env bash
# =====================================================================================
# Chạy thử pipeline raw -> bronze -> silver -> gold với bộ dữ liệu test ~10 triệu dòng.
# Chạy từ Git Bash (Windows) hoặc Linux ; cần các container clickhouse_server, spark_master, spark_worker_1.
#
#   bash src/sql_script/clickhouse/test_data/run_test_pipeline.sh all       # generate bronze silver gold mart check
#   bash src/sql_script/clickhouse/test_data/run_test_pipeline.sh silver gold check   # chạy từng bước
#   bash src/sql_script/clickhouse/test_data/run_test_pipeline.sh mart check          # chỉ dựng lại data mart
#   bash src/sql_script/clickhouse/test_data/run_test_pipeline.sh cleanup   # xóa dữ liệu test rồi dựng lại bronze -> mart
#
# Bước: generate | bronze | silver | gold | mart | check | cleanup | all
#
# - Tầng raw KHÔNG BAO GIỜ bị TRUNCATE (giữ dữ liệu crawl thật).
# - bronze / silver / gold là dữ liệu dẫn xuất: bị TRUNCATE rồi dựng lại toàn bộ từ raw, vì job bronze và
#   load_silver_product_snapshot.sql đang append toàn bộ -> chạy lại mà không truncate sẽ nhân đôi.
# - mart không cần TRUNCATE: mỗi loader e_mart tự dựng bảng stage rồi REPLACE PARTITION thay toàn bộ dữ liệu.
# - Job Spark bronze chạy đúng bản đã deploy trong spark_master (/opt/spark/work-dir/spark_job/...).
#
# Biến môi trường: CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB, LOG_DIR
# =====================================================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1          # Git Bash: không đổi /opt/... thành đường dẫn Windows

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
DDL="$ROOT/src/sql_script/clickhouse/ddl"
TEST="$ROOT/src/sql_script/clickhouse/test_data"
SILVER="$ROOT/src/python/ETL/c_silver"
GOLD="$ROOT/src/python/ETL/d_gold"
MART="$ROOT/src/python/ETL/e_mart"
# Đường dẫn TRONG container spark_master (bản deploy), khác với thư mục local src/python/ETL/b_bronze
BRONZE_DIR=/opt/spark/work-dir/spark_job/ETL/b_bronze

CH_USER="${CLICKHOUSE_USER:-clickhouse}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-123456}"
CH_DB="${CLICKHOUSE_DB:-ecommercial_warehouse}"
LOG_DIR="${LOG_DIR:-${TMPDIR:-/tmp}/pipeline_test_logs}"
mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------- helpers
chq() {   # 1 câu lệnh
    docker exec clickhouse_server clickhouse-client --user="$CH_USER" --password="$CH_PASSWORD" -d "$CH_DB" -q "$1"
}
# ClickHouse chỉ có ~2,7 GB RAM: với 10 triệu dòng, các loader silver (window rank() trên toàn bộ bronze)
# bị "memory limit exceeded" nếu không ép spill ra đĩa sớm. 300 MB đã đủ để load_silver_category chạy (26s).
CH_SPILL_BYTES="${CH_SPILL_BYTES:-300000000}"
chf() {   # chạy cả file SQL (nhiều câu lệnh)
    echo "    $(basename "$1")"
    docker exec -i clickhouse_server clickhouse-client --user="$CH_USER" --password="$CH_PASSWORD" -d "$CH_DB" \
        --max_bytes_before_external_sort="$CH_SPILL_BYTES" \
        --max_bytes_before_external_group_by="$CH_SPILL_BYTES" \
        --multiquery < "$1"
}
TIMINGS="$LOG_DIR/timings.txt"
timed() { # timed "nhãn" lệnh...
    local label=$1; shift
    local s; s=$(date +%s)
    "$@"
    local d=$(( $(date +%s) - s ))
    printf '%-45s %6ss\n' "$label" "$d" | tee -a "$TIMINGS"
}
run_spark() {
    local job=$1 log="$LOG_DIR/${1%.py}.log"
    echo "    spark-submit $job (log: $log)"
    if ! docker exec -w "$BRONZE_DIR" spark_master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 --deploy-mode client \
            --driver-memory 500m --executor-memory 500m --executor-cores 1 \
            --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,com.clickhouse:clickhouse-jdbc:0.8.3 \
            --conf spark.jars.ivy=/tmp/.ivy \
            "$job" > "$log" 2>&1; then
        echo "    !! $job lỗi, 40 dòng cuối log:"
        tail -40 "$log"
        return 1
    fi
}

# ---------------------------------------------------------------- steps
step_generate() {
    echo "== [generate] sinh dữ liệu test vào raw"
    local n; n=$(chq "SELECT count() FROM raw_product_snapshot WHERE toInt64OrZero(url_id) >= 1000000")
    if [ "$n" -gt 0 ]; then
        echo "    raw đã có $n dòng test -> bỏ qua (chạy 'cleanup' trước nếu muốn sinh lại)"
        return
    fi
    timed "generate raw_product_snapshot" chf "$TEST/generate_raw_product_snapshot.sql"
    timed "generate raw_user_review" chf "$TEST/generate_raw_user_review.sql"
}

step_bronze() {
    echo "== [bronze] TRUNCATE bronze_* rồi chạy job Spark (đọc toàn bộ raw)"
    chq "TRUNCATE TABLE bronze_product_snapshot"
    chq "TRUNCATE TABLE bronze_user_review"
    timed "spark load_bronze_product_snapshot" run_spark load_bronze_product_snapshot.py
    timed "spark load_bronze_review_snapshot" run_spark load_bronze_review_snapshot.py
}

step_silver() {
    echo "== [silver] migration url_id, TRUNCATE silver_* rồi nạp lại từ bronze"
    chq "ALTER TABLE silver_product ADD COLUMN IF NOT EXISTS url_id Int64 AFTER product_sk"
    chq "ALTER TABLE silver_snapshot ADD COLUMN IF NOT EXISTS url_id Int64 AFTER product_sk"
    for t in silver_category silver_brand silver_seller silver_product silver_snapshot; do
        chq "TRUNCATE TABLE $t"
    done
    # load_silver_user.sql chưa hoàn thiện -> bỏ qua
    for f in category brand seller product product_snapshot; do
        timed "silver load_silver_$f" chf "$SILVER/load_silver_$f.sql"
    done
}

step_gold() {
    echo "== [gold] tạo bảng (nếu chưa có), TRUNCATE gold_* rồi chạy d_gold/01..11"
    if [ "$(chq "EXISTS TABLE gold_fact_product_daily")" != "1" ]; then
        chf "$DDL/gold_ddl.sql"
    fi
    for t in $(chq "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 'gold_%' AND engine LIKE '%MergeTree'"); do
        chq "TRUNCATE TABLE $t"
    done
    for f in "$GOLD"/*.sql; do
        timed "gold $(basename "$f" .sql)" chf "$f"
    done
}

step_mart() {
    echo "== [mart] tạo bảng / hàm (IF NOT EXISTS, OR REPLACE) rồi chạy e_mart/01..08"
    chf "$DDL/mart_ddl.sql"
    for f in "$MART"/*.sql; do
        timed "mart $(basename "$f" .sql)" chf "$f"
    done
}

step_check() {
    echo "== [check]"
    chf "$TEST/check_test_pipeline.sql" | tee "$LOG_DIR/check_result.txt"
    if [ "$(chq "EXISTS TABLE mart_kpi_daily")" = "1" ]; then
        chf "$TEST/check_mart.sql" | tee -a "$LOG_DIR/check_result.txt"
    fi
}

step_cleanup() {
    echo "== [cleanup] xóa dữ liệu test khỏi raw rồi dựng lại bronze -> gold"
    timed "cleanup raw test rows" chf "$TEST/cleanup_test_data.sql"
    step_bronze
    step_silver
    step_gold
    step_mart
}

# ---------------------------------------------------------------- main
[ $# -eq 0 ] && set -- all
echo "### $(date '+%F %T') run_test_pipeline.sh $* (db=$CH_DB, log=$LOG_DIR)" | tee -a "$TIMINGS"
for step in "$@"; do
    case "$step" in
        all)      step_generate; step_bronze; step_silver; step_gold; step_mart; step_check ;;
        generate|bronze|silver|gold|mart|check|cleanup) "step_$step" ;;
        *) echo "Bước không hợp lệ: $step (generate|bronze|silver|gold|mart|check|cleanup|all)"; exit 2 ;;
    esac
done
echo "== thời gian các bước (cộng dồn trong $TIMINGS):"
cat "$TIMINGS"
