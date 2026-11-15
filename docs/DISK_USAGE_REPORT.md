# Báo cáo: dung lượng đĩa phình ra sau mỗi lần chạy pipeline

Ngày thực hiện: 15/11/2026. Dữ liệu đo: bộ test 10 triệu dòng `raw_product_snapshot` (50k sản phẩm × 200 lần crawl + 50k dòng lặp).
Số liệu thô nằm trong [`docs/disk_usage/`](disk_usage/).

## 1. Tóm tắt

| | |
|---|---|
| **Triệu chứng** | Mỗi lần chạy cả pipeline, dung lượng ổ E: giảm và không lấy lại được. |
| **Nguyên nhân chính** | Job Spark `load_bronze_product_snapshot.py` spill khoảng **16,6 GB** (đo theo kích thước trong RAM) mỗi lần chạy vào `/tmp` của container `spark_worker_1`. `/tmp` nằm trong file đĩa ảo `E:\DockerDesktopWSL\disk\docker_data.vhdx`. Spark xoá file khi chạy xong, nhưng file vhdx không bao giờ tự co lại. |
| **Không phải nguyên nhân** | Spill của ClickHouse. Nó ghi thẳng ra NTFS qua bind mount, mỗi query tối đa khoảng 1 GB, và được xoá ngay khi query kết thúc. |
| **Nguyên nhân phụ** | Log ClickHouse ở mức `trace` (1,4 GB). Các bảng `system.*_log` không có TTL (khoảng 770 MB). Thư mục `work/app-*` của Spark worker tăng khoảng 74 MB sau mỗi lần submit (2 GB). |
| **Đã làm** | (1) Đưa thư mục shuffle/spill của Spark ra bind mount trên E:. (2) Loại trùng ngay trong câu đọc từ ClickHouse thay cho `dropDuplicates` của Spark. (3) Nén vhdx. (4) Giảm log ClickHouse, thêm TTL, đặt trần spill, bật tự dọn `work/` của Spark. |
| **Kết quả bước 2** | Spill **16,6 GB → 0**. vhdx tăng **+1.280 MiB → 0**. Thời gian **47,8 → 45,3 phút (−5,2%)**. Dữ liệu đầu ra **giống hệt** (cùng hash nội dung của 10 triệu dòng). |

## 2. Chẩn đoán

### 2.1 Spill của ClickHouse không phải thủ phạm

- `/var/lib/clickhouse` là bind mount `./docker_mount/clickhouse/data`, tức là một thư mục NTFS thật trên ổ E:. File spill ghi vào `tmp/` ở đó. Khi ClickHouse xoá file, Windows lấy lại chỗ ngay.
- Theo `system.query_log`, query spill nhiều nhất cũng chỉ khoảng 1 GB (bản nén):

  | Query | Spill (nén) |
  |---|---|
  | DQ cross-layer, check raw → bronze | ~1,0 GB |
  | DQ, join `seller_rate_cnt` raw ↔ bronze | ~770 MB |
  | Gold loader `08` (F2) | ~670 MB |

- Sau mỗi query, thư mục `tmp/` trống. Thư mục `store/` không có thư mục bảng mồ côi (chỉ có 3 thư mục, mỗi cái khoảng 1 MB). Part bị detach chỉ khoảng 1 MB. Part cũ (outdated) được xoá đúng hạn 8 phút.

### 2.2 Thủ phạm chính: Spark spill vào bên trong `docker_data.vhdx`

Các container Spark không mount `/tmp`. Vì vậy `spark.local.dir` mặc định là `/tmp` nằm trong lớp ghi (writable layer) của container, tức là trong ext4 của VM Docker, tức là trong file `docker_data.vhdx`.

**Vì sao xoá rồi mà vhdx không co lại?** vhdx là đĩa động: nó nở ra mỗi khi VM ghi vào một block chưa từng dùng. Khi Spark xoá file, ext4 chỉ đánh dấu các block đó là trống, nhưng không báo cho Windows. File không ở chế độ sparse (`fsutil sparse queryflag` báo *NOT set as sparse*), nên kích thước trên E: giữ nguyên. Lần chạy sau, ext4 dùng lại một phần các block trống cũ và cấp thêm một phần block mới, nên vhdx tiếp tục nở. Chỉ có nén thủ công mới lấy lại được dung lượng.

**Vì sao job bronze spill nhiều như vậy?**
- `--executor-memory 500m` chỉ để lại khoảng **110 MiB** bộ nhớ thực thi (log ghi `MemoryStore started with capacity 110.0 MiB`). Cứ đầy 108 MiB là Spark phải spill.
- `dropDuplicates(["idempotency_key"])` gom nhóm trên một key gần như duy nhất (10,05 triệu dòng còn 10 triệu). Bước gom một phần trước shuffle không giảm được dòng nào, nên gần như toàn bộ dữ liệu bị sắp xếp và spill, ở cả hai phía của shuffle.
- Bảng dưới lấy từ `spark_worker_1/work/app-*/*/stderr`, file đầy đủ ở `disk_usage/spark_spill_history.csv`:

  | App | Job | Số lần spill | Spill (trong RAM) |
  |---|---|---|---|
  | app-20261114005416-0002 | bronze product | 154 | 16.632 MiB |
  | app-20261114072249-0004 | bronze product | 154 | 16.632 MiB |
  | app-20261114092043-0006 | bronze product (có retry) | 288 | 31.104 MiB |
  | app-20261114111823-0001 | bronze product | 155 | 16.740 MiB |
  | app-20261114121200-0002 | bronze review | 3 | 324 MiB |

**Dòng thời gian của vhdx:**

| Thời điểm | Sự kiện | vhdx | VM thực dùng |
|---|---|---|---|
| 14/11 khoảng 18:00 | Vừa nén thủ công | 17 GB | — |
| 14/11 18:18 → 19:11 | 1 lần chạy bronze product (app `…111823-0001`) | — | — |
| 15/11 08:10 | Bắt đầu điều tra | **29,66 GB** (28.284 MiB) | 15 GB |
| 15/11 10:18 → 11:06 | Đo lại job cũ (A), `/tmp` lên đỉnh 6,8 GB | 28.284 → **29.564 MiB** (+1.280 MiB) | +3,8 GB tạm thời, rồi trở lại như cũ |
| 15/11 11:08 → 11:53 | Đo job mới (B), không spill | 29.564 → 29.564 MiB (**+0**) | +0 |

Lần chạy A nở thêm 1,25 GB dù bên trong vhdx đã có khoảng 13 GB block được cấp phát mà đang trống. Đó là bằng chứng ext4 không dùng lại hết các block cũ. Ngược lại, lần chạy hỏng trước A (10:01–10:16, `/tmp` lên tới 2,7 GB) không làm vhdx nở chút nào, vì lần đó ext4 dùng lại được block cũ.

### 2.3 Các nguồn phụ, tăng dần trên E:

| Nguồn | Trước khi sửa | Vì sao tăng |
|---|---|---|
| `docker_mount/clickhouse/logs` | 1.372 MiB | Mặc định của image là `level=trace`, xoay vòng `1000M × 10`. Khoảng 80 MB mỗi lần chạy, `err.log` toàn stack trace. |
| Bảng `system.*_log` | khoảng 770 MB | `trace_log` 274 MB, `text_log` 260 MB (ghi lại y hệt log file), `part_log` 71 MB, `metric_log` 64 MB, `asynchronous_metric_log` 66 MB… Không có TTL. |
| `spark_worker_1/work` | 2,0 GB, 23 app | Mỗi lần `spark-submit` thêm khoảng 74 MB (jar + log executor). Spark mặc định không dọn. |

## 3. Các bước đã làm

### Bước 1. Đưa thư mục shuffle/spill của Spark ra bind mount trên E:

`docker-compose.yml`, áp cho cả `spark_worker_1` và `spark_worker_2`:

```yaml
environment:
  SPARK_LOCAL_DIRS: /opt/spark/local
volumes:
  - ./docker_mount/spark_standalone/spark_worker_1/local:/opt/spark/local
```

**Vì sao:** sau thay đổi này, file shuffle/spill của executor nằm trên NTFS. Spark xoá xong là Windows lấy lại chỗ ngay, nên vhdx không còn nở vì Spark. Nếu executor bị kill giữa chừng (máy ngủ, OOM), file sót lại cũng nằm ngay trong `docker_mount/…/local/` để thấy và xoá tay, thay vì bị giấu trong vhdx. Thư mục này đã nằm trong `.gitignore` (`docker_mount/spark_standalone/spark_worker*`).

**Đánh đổi:** I/O qua bind mount Windows chậm hơn ext4. Sau bước 2, job bronze product không còn spill nên không bị ảnh hưởng (lần chạy B chỉ ghi 146 MB, là bản copy jar). Job bronze review vẫn spill khoảng 324 MB và giờ đi qua bind mount. Lượng này nhỏ, và tôi chưa đo riêng.

Kiểm tra: `docker exec spark_worker_1 sh -c 'echo $SPARK_LOCAL_DIRS'` trả `/opt/spark/local`. Executor của lần chạy B tạo `blockmgr-*` trong `/opt/spark/local`.

### Bước 2. Loại trùng ngay trong ClickHouse, bỏ `dropDuplicates` của Spark

File: `src/python/ETL/b_bronze/load_bronze_product_snapshot.py`. Job đọc bằng `.option("query", RAW_DEDUP_QUERY)` thay cho `dbtable`, và không còn `.dropDuplicates(...)`:

```sql
-- nhánh 1: dòng có hash key xuất hiện đúng 1 lần -> key chắc chắn duy nhất -> đi thẳng
SELECT idempotency_key, url_id, observed_at, title, ... , additional_info
FROM raw_product_snapshot
WHERE cityHash64(idempotency_key) NOT IN (
    SELECT h FROM (SELECT cityHash64(idempotency_key) AS h FROM raw_product_snapshot)
    GROUP BY h HAVING count() > 1)
UNION ALL
-- nhánh 2: khoảng 50k key bị trùng -> GROUP BY theo key THẬT, giữ 1 dòng
SELECT idempotency_key, any(url_id), any(observed_at), any(title), ... , any(additional_info)
FROM raw_product_snapshot
WHERE cityHash64(idempotency_key) IN (...cùng subquery...)
GROUP BY idempotency_key
```

**Vì sao đúng:**
- Mọi key xuất hiện nhiều lần đều có hash nằm trong tập "trùng", nên đi vào nhánh 2 và bị gộp còn 1 dòng.
- Nhánh 1 chỉ chứa các hash xuất hiện đúng 1 lần, nên key của chúng chắc chắn duy nhất.
- Nếu hai key *khác nhau* vô tình trùng hash 64-bit (xác suất khoảng 3·10⁻⁶ với 10 triệu key), cả hai chỉ bị đẩy sang nhánh 2. Nhánh 2 `GROUP BY` theo key thật nên vẫn ra 2 dòng riêng. Vì vậy kết quả luôn đúng, không phụ thuộc hash.
- Ngữ nghĩa giống `dropDuplicates(["idempotency_key"])`: mỗi key giữ một dòng bất kỳ. Các bản trùng là cùng một message Kafka gửi lại, nên giống hệt nhau.

**Vì sao chọn cách này.** Tôi đã thử nhiều phương án trực tiếp trên ClickHouse (dạng câu lệnh mà Spark gửi tới, `FORMAT Null`). Kết quả lưu trong `disk_usage/dedup_query_pretests.csv`:

| Phương án | Thời gian | RAM đỉnh | Spill | Ghi chú |
|---|---|---|---|---|
| `GROUP BY url_id, observed_at, idempotency_key` + `any()` mọi cột | 42 s | 1,25 GiB | 812 MiB | Gom hash phải giữ cả dòng |
| Như trên + `optimize_aggregation_in_order=1` (đọc theo sort key) | — | > 2,26 GiB | — | **MEMORY_LIMIT_EXCEEDED**: đọc theo thứ tự phải mở song song 121 part × 20 cột |
| Như trên + `max_block_size=1024` | 134 s | 1,42 GiB | 0 | Quá sát trần 2,7 GB |
| Chia 4 / 8 bucket `cityHash64(url_id)`, gom hash | 57 / 29 s | 1,12 GiB | 571 / 283 MiB | Vẫn spill |
| Chỉ dò key trùng bằng `GROUP BY` 3 cột key, theo thứ tự | 78 s | 1,18 GiB | 0 | Vẫn nặng do đọc theo thứ tự |
| **Dò key trùng bằng `cityHash64` (bản đã chọn)** | **42 s** | **537 MiB** | **0** | Không cần setting đặc biệt |

Lưu ý kỹ thuật: Spark JDBC luôn bọc câu lệnh thành `SELECT … FROM (query) SPARK_GEN_SUBQ_0`. Mệnh đề `SETTINGS` đặt bên trong subquery không có tác dụng. Muốn truyền setting phải dùng URL `?clickhouse_setting_<tên>=<giá trị>` (đã thử, chạy được). `sessionInitStatement "SET …"` thì không có tác dụng. Bản đã chọn không cần setting nào.

Sửa kèm: dòng import của `load_bronze_product_snapshot.py` và `load_bronze_review_snapshot.py` ở bản local đang là `from python.ETL.b_bronze.preprocessing_func…`. Tôi đổi lại thành `from preprocessing_func…`, giống bản đang chạy trong container và quy ước import phẳng của project. Nếu để nguyên, copy vào container sẽ bị `ModuleNotFoundError`.

### Bước 3. Nén `docker_data.vhdx`

**Vì sao:** khoảng 14 GB block đã xoá trong VM vẫn bị giữ trong file vhdx (VM dùng 17 GB, file dài 31 GB). Chỉ có nén thủ công mới trả lại được chỗ đó cho Windows.

Làm ngày 15/11 lúc 12:00. Script đầy đủ: `disk_usage/compact_vhdx.ps1` + `disk_usage/compact_vhdx.txt` (diskpart). Log kết quả: `disk_usage/compact_vhdx_result.log`. Các bước có thể tự làm lại:

1. `docker stop spark_worker_1 spark_master` để trả các container Spark về trạng thái tắt như lúc đầu.
2. fstrim trong VM:
   ```
   docker run --rm --privileged --pid=host --user root --entrypoint nsenter spark-custom:4.2.0-scala2.13-java21-python3-ubuntu -t 1 -m -- fstrim -av
   ```
   Lần này trả được 1,1 GiB block trống.
3. `docker desktop stop`, rồi `wsl --shutdown`.
4. Chạy `diskpart` với quyền admin (cần bấm Yes ở hộp thoại UAC):
   ```
   select vdisk file="E:\DockerDesktopWSL\disk\docker_data.vhdx"
   attach vdisk readonly
   compact vdisk
   detach vdisk
   ```
5. `docker desktop start`. Các container có `restart: unless-stopped` tự chạy lại (`clickhouse_server`, `kafka_1`, `streamlit_dashboard`).

| | Kích thước `docker_data.vhdx` |
|---|---|
| Trước khi nén | 31.000.100.864 B (31,0 GB) |
| **Sau khi nén** | **19.379.781.632 B (19,4 GB), giảm 11,6 GB** |

Docker tắt khoảng 1 phút (12:02:28 → 12:03:33). Phần 19,4 GB còn lại là dữ liệu thật trong VM: image và container, khoảng 17 GB theo `df`, cộng với phần dư của định dạng vhdx.

Windows Home không có `Optimize-VHD`. Chế độ sparse VHD (`wsl --manage … --set-sparse`, `sparseVhd=true`) đang bị Microsoft đánh dấu là có thể làm hỏng dữ liệu, và với `docker_data.vhdx` đã chuyển sang ổ khác thì lệnh còn bị từ chối. Vì vậy tôi không dùng nó.

### Bước 4. Giảm log ClickHouse, thêm TTL, đặt trần spill, bật tự dọn `work/` của Spark

**ClickHouse:** file mới `docker_mount/clickhouse/config.d/zz-low-disk.xml`. Trong `docker-compose.yml` chỉ mount **đúng file này**. Nếu mount cả thư mục `config.d`, nó sẽ che mất `docker_related_config.xml` có sẵn trong image.

| Thiết lập | Trước | Sau | Vì sao |
|---|---|---|---|
| `logger.level` / `size` / `count` | trace / 1000M / 10 | **warning / 100M / 3** | Log file tối đa khoảng 20 GB → khoảng 600 MB |
| `trace_log`, `text_log`, `metric_log`, `asynchronous_metric_log`, `query_metric_log` | bật, không TTL | **tắt** (`remove="1"`) | Project không dùng tới. `text_log` trùng với log file |
| `query_log`, `part_log` | không TTL | **TTL 7 ngày** | Vẫn giữ để đo spill, memory, thời gian của loader và DQ |
| `max_temporary_data_on_disk_size` | 0 (không giới hạn) | **10 GB** | Spill lớn nhất đo được khoảng 1 GB. Query lỗi còn hơn làm đầy ổ E: |

Tôi đã kiểm tra config gộp bằng `clickhouse extract-from-config` trong một container tạm trước khi áp. Sau đó tôi xoá các bảng log cũ không còn được ghi (`trace_log`, `text_log`, `metric_log`, `asynchronous_metric_log`, `query_metric_log`), các bản `part_log_0` / `query_log_0` mà ClickHouse tự đổi tên khi định nghĩa bảng thay đổi, và 5 file log `.gz` đã xoay vòng.

**Spark worker:** `SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=3600 -Dspark.worker.cleanup.appDataTtl=259200"`. Mỗi giờ worker xoá thư mục `work/app-*` của các app đã dừng quá 3 ngày. Số liệu spill của các app cũ đã lưu sẵn trong `disk_usage/spark_spill_history.csv` trước khi bật tính năng này.

## 4. Đo tốc độ bước 2: trước và sau

**Cách đo:**
- Hai lần chạy dùng cùng dữ liệu raw và cùng tham số `spark-submit` với `run_test_pipeline.sh` (driver/executor 500m, 1 worker, 1 core).
- Cả hai chạy dưới cùng config ClickHouse mới.
- Trước mỗi lần chạy, `TRUNCATE bronze_product_snapshot` (job chạy ở chế độ append).
- Script đo `disk_usage/measure_bronze.sh` lấy mẫu kích thước vhdx, dung lượng VM và thư mục local của worker mỗi 30 giây (`disk_usage/run_*_disk_samples.csv`). So sánh dữ liệu đầu ra bằng `disk_usage/fingerprint.sql`.
- Phía ClickHouse lấy từ `system.query_log` (`disk_usage/clickhouse_side_metrics.csv`).
- Job cũ chạy đúng bản đang deploy trong container. Job mới chạy từ một bản copy trong `/tmp/disk_test` của `spark_master`, không đụng vào bản deploy.

| | **A: job cũ** (`dropDuplicates`) | **B: job mới** (loại trùng trong ClickHouse) | Chênh lệch |
|---|---|---|---|
| Tổng thời gian `spark-submit` | 2.868 s (47,8 phút) | **2.719 s (45,3 phút)** | **−149 s (−5,2%)** |
| Các stage Spark | stage 0: 1.161 s, stage 2: 1.665 s (67 task) + shuffle | 1 stage, 1 task: 2.664 s, không shuffle | |
| Spill của executor | 154 × 108 MiB = **16.632 MiB** | **0** | −100% |
| Đỉnh thư mục local của worker | **6.793 MiB** | 146 MiB (bản copy jar) | −98% |
| Dung lượng VM tăng tạm thời | +3,8 GB (giữ đến khi app kết thúc) | 0 | |
| **vhdx nở thêm** | **+1.280 MiB** | **0** | |
| ClickHouse SELECT: thời gian, RAM đỉnh | 1.032 s, 144 MiB | 2.646 s (+26 s cho query dò schema), 543 MiB | Đọc xen kẽ với ghi trong cùng 1 task |
| ClickHouse INSERT: số lệnh, tổng thời gian | 10.035, 951 s | 10.000, 801 s | |
| Kết quả `bronze_product_snapshot` | 10.000.000 dòng, 10.000.000 key, hash `9980225000634678051` | **giống hệt** | |

Hash nội dung là `sum(cityHash64(<mọi cột trừ ingested_at>))`. Hash của cả hai lần chạy cũng trùng với bảng bronze ngày 14/11.

**Vì sao chỉ nhanh hơn 5%?** Tách phần thời gian không phải INSERT ra thì thấy cả hai gần như bằng nhau:
- A: 1.161 + 1.665 − 951 = **1.875 s**
- B: 2.664 − 801 = **1.863 s**

Phần này là đọc JDBC từng dòng, chuẩn hoá bằng regex, rồi gắn tham số JDBC cho 10 triệu dòng trên **một core**. Sắp xếp và spill lên ext4 cục bộ vốn khá rẻ về thời gian. Cái giá lớn của chúng là dung lượng đĩa, chứ không phải tốc độ. Nút thắt về tốc độ nằm ở chỗ khác:

1. **Ghi 10.000 lệnh INSERT, mỗi lệnh 1.000 dòng** (mặc định `batchsize=1000` của Spark JDBC). Mỗi lệnh tạo một part mới trong ClickHouse, rồi phải merge. Riêng phần này mất 800–950 s phía ClickHouse.
2. **Một partition JDBC duy nhất trên một core.** Hiện chỉ có 1 worker chạy, vì máy thiếu RAM.

Nếu muốn nhanh hơn, có thể thử thêm (chưa làm, chưa đo):
- `.option("batchsize", 50000)` khi ghi: 10.000 lệnh INSERT còn 200. Mỗi batch tốn khoảng 75 MB RAM của executor.
- Bật `spark_worker_2` và đọc song song bằng `predicates` theo `cityHash64(url_id) % N`. Cách này cần thêm RAM.
- Query dò schema của Spark (`… WHERE 1=0`) vẫn khiến ClickHouse dựng các tập `IN`, mất 26 s. Chỉ khoảng 1%, có thể bỏ qua.

**Kết luận bước 2:** mục tiêu chính là chặn spill, nên vhdx không còn nở vì job bronze. Mục tiêu này đạt hoàn toàn, với dữ liệu giống hệt và thời gian nhỉnh hơn một chút. Muốn tăng tốc đáng kể thì phải sửa phía ghi JDBC.

## 5. Tổng kết dung lượng

| Mục | Trước | Sau |
|---|---|---|
| `docker_data.vhdx` | 29,66 GB lúc bắt đầu, 31,0 GB sau lần đo A | **19,4 GB** (sau nén). Không còn nở vì job bronze |
| Spill mỗi lần chạy bronze product | 16,6 GB vào vhdx | 0 |
| `docker_mount/clickhouse/logs` | 1.372 MiB | 1 MiB |
| `docker_mount/clickhouse/data/store` | 6.915 MiB | 5.391 MiB |
| Bảng `system.*_log` | khoảng 770 MB, không TTL | còn `query_log`/`part_log` (TTL 7 ngày), `processors_profile_log` (TTL 30 ngày) và vài bảng nhỏ |
| `spark_worker_1/work` | 2,0 GB | Tự dọn app cũ hơn 3 ngày, mỗi giờ một lần, khi worker chạy |

## 6. Việc cần làm tiếp và lưu ý

1. **Deploy job mới.** Bản trong container vẫn là job cũ. Cần copy `src/python/ETL/b_bronze/load_bronze_product_snapshot.py` tới `spark_master:/opt/spark/work-dir/spark_job/ETL/b_bronze/`. `preprocessing_func/` không đổi. Bản review trong container đã dùng import phẳng sẵn nên không cần copy.
2. **Luôn dùng `docker compose up -d --no-deps <service>`.** Lúc 10:16, một phiên khác chạy `docker compose up -d dashboard` mà không có `--no-deps`, nên compose tạo lại `clickhouse_server` (do compose vừa thêm mount config mới). Lần đo A đầu tiên bị đứt kết nối JDBC ở phút thứ 14 và phải chạy lại.
3. **Máy thiếu RAM.** Trong lúc đo, RAM trống chỉ còn khoảng 1,7 GB. Claude Code đã tự dừng một shell nền, nhưng job Spark trong container vẫn chạy tiếp bình thường.
4. **Hoàn tác (rollback):**
   - Bỏ mount `zz-low-disk.xml` và các dòng `SPARK_LOCAL_DIRS` / `SPARK_WORKER_OPTS` trong `docker-compose.yml`, rồi `docker compose up -d --no-deps clickhouse_server spark_worker_1`.
   - Job cũ có trong git (`git diff src/python/ETL/b_bronze/`).
5. **Định kỳ:** sau những lần chạy nặng khác (Spark streaming, image mới), vhdx vẫn có thể nở. Khi cần, nén lại theo bước 3.

### Các file đã thay đổi

| File | Thay đổi |
|---|---|
| `src/python/ETL/b_bronze/load_bronze_product_snapshot.py` | Loại trùng trong query ClickHouse, bỏ `dropDuplicates`, sửa import |
| `src/python/ETL/b_bronze/load_bronze_review_snapshot.py` | Sửa import |
| `docker-compose.yml` | Spark worker: `SPARK_LOCAL_DIRS`, `SPARK_WORKER_OPTS`, mount `local/`. ClickHouse: mount `zz-low-disk.xml` |
| `docker_mount/clickhouse/config.d/zz-low-disk.xml` | Mới: log, bảng log hệ thống, trần spill |
| `docs/DISK_USAGE_REPORT.md`, `docs/disk_usage/*` | Báo cáo này và số liệu đo |
