#!/usr/bin/env bash
# Đo 1 lần chạy job bronze: thời gian, exit code, và lấy mẫu dung lượng đĩa mỗi 30s.
# usage: measure_bronze.sh <label> <workdir trong spark_master> <job.py>
set -u
export MSYS_NO_PATHCONV=1
label=$1; workdir=$2; job=$3
OUT="$(dirname "$0")/runs/$label"; mkdir -p "$OUT"
VHDX=/e/DockerDesktopWSL/disk/docker_data.vhdx
LOCAL_DIR_IN_WORKER=${LOCAL_DIR_IN_WORKER:-/tmp}

sample() {
    local vhdx vm tmp
    vhdx=$(stat -c %s "$VHDX" 2>/dev/null)
    vm=$(docker exec spark_worker_1 df -B1 --output=used / 2>/dev/null | tail -1 | tr -d ' ')
    tmp=$(docker exec spark_worker_1 du -sb "$LOCAL_DIR_IN_WORKER" 2>/dev/null | cut -f1)
    echo "$(date '+%F %T'),$(( vhdx/1048576 )),$(( ${vm:-0}/1048576 )),$(( ${tmp:-0}/1048576 ))"
}

echo "ts,vhdx_mb,vm_used_mb,worker_local_dir_mb" > "$OUT/disk_samples.csv"
sample >> "$OUT/disk_samples.csv"
( while true; do sleep 30; sample >> "$OUT/disk_samples.csv"; done ) &
SAMPLER=$!

start=$(date +%s); echo "start=$(date '+%F %T')" > "$OUT/timing.txt"
docker exec -w "$workdir" spark_master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 --deploy-mode client \
    --driver-memory 500m --executor-memory 500m --executor-cores 1 \
    --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,com.clickhouse:clickhouse-jdbc:0.8.3 \
    --conf spark.jars.ivy=/tmp/.ivy \
    "$job" > "$OUT/spark_submit.log" 2>&1
rc=$?
end=$(date +%s)
kill $SAMPLER 2>/dev/null; wait $SAMPLER 2>/dev/null
sample >> "$OUT/disk_samples.csv"
{ echo "end=$(date '+%F %T')"; echo "exit_code=$rc"; echo "duration_sec=$(( end - start ))"; } >> "$OUT/timing.txt"
cat "$OUT/timing.txt"
