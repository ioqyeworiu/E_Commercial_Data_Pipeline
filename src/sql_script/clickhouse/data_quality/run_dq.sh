#!/usr/bin/env bash
# =====================================================================================
# Chạy bộ kiểm tra chất lượng dữ liệu (data quality) trên ClickHouse, ghi kết quả vào dq_check_result.
# Chạy từ Git Bash (Windows) hoặc Linux ; cần container clickhouse_server.
#
#   bash src/sql_script/clickhouse/data_quality/run_dq.sh all                 # raw bronze silver gold cross report
#   bash src/sql_script/clickhouse/data_quality/run_dq.sh raw bronze report   # chạy từng tầng
#   bash src/sql_script/clickhouse/data_quality/run_dq.sh report              # chỉ in báo cáo lần chạy mới nhất
#
# Bước: raw | bronze | silver | gold | cross | debug | report | all
#   - Mọi bước trong 1 lần gọi dùng chung 1 run_id, nên báo cáo gộp được nhiều tầng.
#   - Một tầng lỗi SQL thì vẫn chạy tiếp các tầng sau, cuối cùng exit 1.
#   - cross tự chạy thêm 06_lost_keys_debug.sql khi có check đối soát FAIL (debug = chạy ép).
#   - report in báo cáo và xuất <run_id>_report.txt, <run_id>_report.md, <run_id>_result.csv vào LOG_DIR (evidence).
#   - Chỉ ĐỌC các bảng pipeline ; chỉ GHI vào dq_check_result.
#
# Biến môi trường: CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB, LOG_DIR, RUN_ID
# Thời gian trên bộ test 10 triệu dòng: raw ~2 phút, bronze ~1,5 phút, silver ~35 giây, gold ~35 giây, cross ~30 giây.
# =====================================================================================
set -uo pipefail
export MSYS_NO_PATHCONV=1          # Git Bash: không đổi /opt/... thành đường dẫn Windows

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CH_USER="${CLICKHOUSE_USER:-clickhouse}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-123456}"
CH_DB="${CLICKHOUSE_DB:-ecommercial_warehouse}"
RUN_ID="${RUN_ID:-dq_$(date +%Y%m%d_%H%M%S)}"
LOG_DIR="${LOG_DIR:-${TMPDIR:-/tmp}/dq_logs}"
mkdir -p "$LOG_DIR"
# ClickHouse chỉ có ~2,7 GB RAM: các check GROUP BY / window trên 10 triệu dòng phải được spill ra đĩa sớm.
CH_SPILL_BYTES="${CH_SPILL_BYTES:-300000000}"

FAILED_STEPS=()

# ---------------------------------------------------------------- helpers
chq() {   # 1 câu lệnh
    docker exec clickhouse_server clickhouse-client --user="$CH_USER" --password="$CH_PASSWORD" -d "$CH_DB" \
        --param_run_id="$RUN_ID" -q "$1"
}
chf() {   # chf <file.sql> [tham số thêm cho clickhouse-client, vd --format=Markdown]
    local file=$1; shift
    docker exec -i clickhouse_server clickhouse-client --user="$CH_USER" --password="$CH_PASSWORD" -d "$CH_DB" \
        --param_run_id="$RUN_ID" \
        --max_bytes_before_external_sort="$CH_SPILL_BYTES" \
        --max_bytes_before_external_group_by="$CH_SPILL_BYTES" \
        "$@" --multiquery < "$file"
}
run_file() {   # run_file <bước> <file.sql>
    local step=$1 file=$2 log="$LOG_DIR/${RUN_ID}_$1.log"
    local s; s=$(date +%s)
    echo "== [$step] $(basename "$file") (log: $log)"
    if chf "$file" > "$log" 2>&1; then
        echo "   xong sau $(( $(date +%s) - s ))s ; $(chq "SELECT concat(toString(count()), ' check: ', toString(countIf(status = 'FAIL')), ' FAIL, ', toString(countIf(status = 'WARN')), ' WARN') FROM dq_check_result WHERE run_id = {run_id:String} AND layer = '$step'")"
    else
        echo "   !! $step lỗi sau $(( $(date +%s) - s ))s, 20 dòng cuối log:"
        tail -20 "$log"
        FAILED_STEPS+=("$step")
    fi
}

# ---------------------------------------------------------------- steps
step_setup()  { chf "$DIR/00_dq_setup.sql" > /dev/null || { echo "!! không tạo được dq_check_result / UDF"; exit 1; }; }
step_raw()    { run_file raw    "$DIR/01_raw_dq.sql"; }
step_bronze() { run_file bronze "$DIR/02_bronze_dq.sql"; }
step_silver() { run_file silver "$DIR/03_silver_dq.sql"; }
step_gold()   { run_file gold   "$DIR/04_gold_dq.sql"; }
step_cross()  {
    run_file cross "$DIR/05_cross_layer_dq.sql"
    local n; n=$(chq "SELECT count() FROM dq_check_result WHERE run_id = {run_id:String} AND status = 'FAIL'
                      AND (check_id LIKE 'X01%' OR check_id LIKE 'B01%' OR check_id IN ('S04a', 'S04b', 'G04a', 'G04b'))")
    if [ "${n:-0}" -gt 0 ]; then
        echo "   có $n check đối soát FAIL -> liệt kê khóa bị rơi"
        step_debug
    fi
}
step_debug()  {
    local log="$LOG_DIR/${RUN_ID}_lost_keys.txt"
    chf "$DIR/06_lost_keys_debug.sql" | tee "$log"
}
step_report() {
    # Báo cáo đọc lần chạy MỚI NHẤT trong bảng -> đặt tên file theo run_id đó (khi chỉ gọi `report`,
    # RUN_ID của lần gọi này không có check nào).
    local rid; rid=$(chq "SELECT argMax(run_id, run_at) FROM dq_check_result")
    rid="${rid:-$RUN_ID}"
    local report="$LOG_DIR/${rid}_report.txt" md="$LOG_DIR/${rid}_report.md" csv="$LOG_DIR/${rid}_result.csv"
    chf "$DIR/99_dq_report.sql" --format=PrettyCompactMonoBlock | tee "$report"
    # Markdown: cùng báo cáo + phụ lục toàn bộ kết quả (bản không cắt chữ nằm ở CSV)
    local appendix
    appendix=$(cat <<'SQL'
SELECT
    status, layer AS `tầng`, table_name AS `bảng`, check_id, dq_cell(column_name, 40) AS `cột`, scope,
    failed_rows AS `lỗi`, total_rows AS `tổng`, round(measured_value, 4) AS `giá trị`, unit AS `đơn vị`,
    concat(toString(warn_threshold), ' / ', toString(fail_threshold)) AS `ngưỡng`, dq_cell(check_name, 70) AS `kiểm tra`
FROM dq_check_result
WHERE run_id = (SELECT argMax(run_id, run_at) FROM dq_check_result)
ORDER BY indexOf(['raw', 'bronze', 'silver', 'gold', 'cross'], layer), table_name, check_id, column_name, scope
FORMAT Markdown
SQL
)
    {
        chf "$DIR/99_dq_report.sql" --format=Markdown
        chq "SELECT '\n## 8. Phụ lục: toàn bộ kết quả\n\nThứ tự: tầng, bảng, check. Bản đầy đủ (không cắt chữ, có mẫu lỗi và ghi chú) ở file CSV cùng thư mục.\n' FORMAT TSVRaw"
        chq "$appendix"
    } > "$md"
    chq "SELECT * EXCEPT (run_at), formatDateTime(run_at, '%F %T') AS run_at FROM dq_check_result
         WHERE run_id = (SELECT argMax(run_id, run_at) FROM dq_check_result)
         ORDER BY indexOf(['raw', 'bronze', 'silver', 'gold', 'cross'], layer), table_name, check_id, column_name, scope
         FORMAT CSVWithNames" > "$csv"
    echo "== báo cáo: $report"
    echo "== Markdown: $md"
    echo "== CSV:     $csv"
}

# ---------------------------------------------------------------- main
[ $# -eq 0 ] && set -- all
echo "### $(date '+%F %T') run_dq.sh $* (db=$CH_DB, run_id=$RUN_ID, log=$LOG_DIR)"
step_setup
for step in "$@"; do
    case "$step" in
        all) step_raw; step_bronze; step_silver; step_gold; step_cross; step_report ;;
        raw|bronze|silver|gold|cross|debug|report) "step_$step" ;;
        *) echo "Bước không hợp lệ: $step (raw|bronze|silver|gold|cross|debug|report|all)"; exit 2 ;;
    esac
done

if [ ${#FAILED_STEPS[@]} -gt 0 ]; then
    echo "!! các bước lỗi SQL: ${FAILED_STEPS[*]}"
    exit 1
fi
