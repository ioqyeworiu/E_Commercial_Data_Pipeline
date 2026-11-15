# Bộ kiểm tra chất lượng dữ liệu (Data Quality)

Bộ truy vấn SQL chạy trên ClickHouse để đo chất lượng dữ liệu ở từng tầng của pipeline:
**raw → bronze → silver → gold**, cộng thêm phần đối soát xuyên tầng.

- Có khoảng **114 check** (mã `R..`, `B..`, `S..`, `G..`, `X..`). Một lần chạy ghi khoảng **276 dòng kết quả**, vì nhiều check tách theo từng cột hoặc từng bảng.
- Mỗi dòng kết quả được chấm **PASS / WARN / FAIL / INFO** theo ngưỡng và lưu vào bảng **`dq_check_result`**, nên các lần chạy được giữ lại làm lịch sử.
- Bộ check **chỉ đọc** các bảng pipeline và chỉ ghi vào `dq_check_result`. Chạy lúc nào cũng an toàn.
- Trên bộ test 10 triệu dòng, một lần chạy đầy đủ mất khoảng **7 phút**.

---

## 1. Các file trong thư mục

| File | Vai trò | Ghi vào `dq_check_result`? |
|---|---|---|
| `00_dq_setup.sql` | Tạo bảng `dq_check_result` và 9 hàm dùng chung `dq_*`. Dùng `IF NOT EXISTS` / `OR REPLACE` nên chạy lại bao nhiêu lần cũng được. | Không (chỉ tạo bảng và hàm) |
| `01_raw_dq.sql` | Check tầng raw, R01–R10: lỗi của **nguồn** (trang web, selector, crawler, Kafka). | **Có** |
| `02_bronze_dq.sql` | Check tầng bronze, B01–B10: job Spark có làm mất hay nhân đôi dòng không, parse có đúng không, dữ liệu có hợp lệ về nghiệp vụ không, entity resolution brand / seller. | **Có** |
| `03_silver_dq.sql` | Check tầng silver, S01–S06: khóa SK, SCD2, khóa ngoại, đối soát với bronze. | **Có** |
| `04_gold_dq.sql` | Check tầng gold, G01–G10: grain, khóa ngoại, SCD2 `gold_dim_product`, công thức trong 5 bảng fact, đối soát. | **Có** |
| `05_cross_layer_dq.sql` | X01: phễu số dòng **theo ngày** qua các tầng. X03: độ trễ giữa các tầng. Cuối file in thêm bảng phễu (chỉ in). | **Có** (X01, X03) |
| `06_lost_keys_debug.sql` | X02 (debug): liệt kê tối đa 20 khóa bị rơi hoặc bị thừa ở mỗi bước. Nặng, nên chỉ chạy khi đối soát FAIL. | Không (chỉ in) |
| `99_dq_report.sql` | Báo cáo 7 mục của **lần chạy mới nhất**. Cùng một file ra báo cáo chữ hoặc Markdown, tùy `--format`. | Không (chỉ đọc) |
| `run_dq.sh` | Script chạy cả bộ: sinh `run_id`, chạy theo thứ tự, ghi log, in báo cáo, xuất CSV. | (gọi các file trên) |
| `raw_data_quality.sql` | Báo cáo cũ, chạy độc lập (thiếu giá trị và sai định dạng ở raw). Giữ lại làm tham khảo. | Không (chỉ SELECT) |

---

## 2. Chạy nhanh

Chạy từ thư mục gốc của repo, bằng Git Bash trên Windows hoặc Linux. Cần container `clickhouse_server` đang chạy.

```bash
# Toàn bộ: raw, bronze, silver, gold, cross, rồi report
bash src/sql_script/clickhouse/data_quality/run_dq.sh all

# Chỉ một vài tầng, trong cùng một run_id; nên để report ở cuối
bash src/sql_script/clickhouse/data_quality/run_dq.sh bronze silver report

# Chỉ in lại báo cáo của lần chạy mới nhất (không chạy check nào)
bash src/sql_script/clickhouse/data_quality/run_dq.sh report

# Lưu báo cáo và CSV vào thư mục evidence
LOG_DIR=evidence/w06/data_quality bash src/sql_script/clickhouse/data_quality/run_dq.sh all
```

Bộ check chạy hoàn toàn trong ClickHouse qua `docker exec`. **Không cần copy gì vào `spark_master`.**

---

## 3. `run_dq.sh` chi tiết

### Các bước

| Bước | Làm gì | Thời gian (10 triệu dòng) |
|---|---|---|
| *(luôn chạy)* | `00_dq_setup.sql`. Nếu lỗi thì dừng ngay với exit 1. | vài giây |
| `raw` | `01_raw_dq.sql` | ~2,5 phút |
| `bronze` | `02_bronze_dq.sql` | ~1,75 phút |
| `silver` | `03_silver_dq.sql` | ~40 giây |
| `gold` | `04_gold_dq.sql` | ~40 giây |
| `cross` | `05_cross_layer_dq.sql`. Nếu có check đối soát FAIL (`X01*`, `B01*`, `S04a/b`, `G04a/b`), tự chạy thêm `debug`. | ~30 giây |
| `debug` | Ép chạy `06_lost_keys_debug.sql`. | nặng: mỗi bước GROUP BY ~20 triệu khóa |
| `report` | `99_dq_report.sql`, xuất báo cáo `.txt` + `.md` và CSV. | vài giây |
| `all` | `raw bronze silver gold cross report` | ~7 phút |

- Mọi bước trong **một lần gọi** dùng chung một `run_id`, nên báo cáo gộp được nhiều tầng.
- Một bước lỗi SQL thì các bước sau **vẫn chạy**. Cuối cùng script in tên các bước lỗi và `exit 1`.
- **Check FAIL không làm script exit 1.** Exit code chỉ phản ánh lỗi SQL. Muốn chặn pipeline khi có FAIL thì query `dq_check_result` (xem mục 6).
- Exit code: `0` là chạy xong, `1` là có bước lỗi SQL hoặc không tạo được bảng / hàm, `2` là tên bước sai.

### Biến môi trường

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `RUN_ID` | `dq_YYYYmmdd_HHMMSS` | Mã lần chạy. **Mỗi lần chạy nên dùng một mã mới** (xem mục 4). |
| `LOG_DIR` | `${TMPDIR:-/tmp}/dq_logs` | Nơi lưu log, báo cáo, CSV. Thư mục tạm có thể bị dọn, nên khi cần giữ làm evidence hãy chỉ vào thư mục trong repo. |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DB` | `clickhouse` / `123456` / `ecommercial_warehouse` | Kết nối ClickHouse. |
| `CH_SPILL_BYTES` | `300000000` | Ngưỡng `max_bytes_before_external_sort/group_by`. ClickHouse chỉ có khoảng 3 GB RAM, nên các GROUP BY / window trên 10 triệu dòng phải spill ra đĩa sớm. Nếu vẫn hết RAM thì giảm số này. |

### File đầu ra trong `LOG_DIR`

| File | Nội dung |
|---|---|
| `<run_id>_<bước>.log` | Output và lỗi của từng file SQL (bảng phễu của `cross` cũng nằm ở đây). |
| `<run_id>_report.txt` | Báo cáo 7 mục (bản sao những gì in ra màn hình). File được đặt tên theo lần chạy được báo cáo, tức lần mới nhất trong bảng. |
| `<run_id>_report.md` | Cùng báo cáo đó dạng Markdown, kèm **mục 8: phụ lục toàn bộ kết quả** của lần chạy. Gọi lại `report` cho cùng lần chạy sẽ ghi đè file này. |
| `<run_id>_result.csv` | Toàn bộ dòng của lần chạy (`CSVWithNames`), dùng làm evidence hoặc mở bằng Excel. |
| `<run_id>_lost_keys.txt` | Danh sách khóa bị rơi. Chỉ có khi `debug` chạy. |

---

## 4. Chạy tay từng file (không qua script)

Các file `01`–`05` **có ghi vào bảng**. Khi chạy tay cần nhớ 4 điều:

1. **Truyền tham số `run_id`.** Mọi câu INSERT dùng `{run_id:String}`. Thiếu tham số thì ClickHouse báo `Substitution 'run_id' is not set` và không ghi gì.
2. **Mỗi lần chạy dùng một `run_id` mới.** `dq_check_result` là `MergeTree` thường, không tự loại trùng. Chạy một file hai lần với cùng `run_id` thì báo cáo đếm gấp đôi.
3. **Giữ hai tham số spill**, nếu không dễ bị `MEMORY_LIMIT_EXCEEDED`.
4. **Đã chạy `00_dq_setup.sql` ít nhất một lần.** Nếu chưa, sẽ gặp lỗi `Unknown function dq_...` hoặc `Table dq_check_result does not exist`.

```bash
cd src/sql_script/clickhouse/data_quality
docker exec -i clickhouse_server clickhouse-client \
    --user=clickhouse --password=123456 -d ecommercial_warehouse \
    --param_run_id=dq_manual_20261114_01 \
    --max_bytes_before_external_sort=300000000 --max_bytes_before_external_group_by=300000000 \
    --multiquery < 02_bronze_dq.sql
```

- **Trong clickhouse-client tương tác** có thể đặt tham số cho cả phiên: `SET param_run_id = 'dq_manual_01';`
- **DBeaver hoặc notebook** có thể không hiểu cú pháp `{run_id:String}`. Khi đó, thay nó bằng một chuỗi cố định, ví dụ `'dq_manual_01'`.
- **Git Bash** tự đổi `/opt/...` thành đường dẫn Windows. Đặt `export MSYS_NO_PATHCONV=1` trước khi gọi `docker exec` có đường dẫn.

---

## 5. Bảng `dq_check_result`

Mỗi dòng là kết quả của một check trên một cột (hoặc một bảng) trong một `scope`.

| Cột | Ý nghĩa |
|---|---|
| `run_id`, `run_at` | Mã lần chạy và thời điểm ghi (giờ Việt Nam). |
| `layer` | `raw` \| `bronze` \| `silver` \| `gold` \| `cross` |
| `table_name` | Bảng được kiểm tra. |
| `check_id`, `check_name` | Mã check (danh mục ở mục 7) và mô tả. |
| `dq_dimension` | Chiều chất lượng: `volume`, `timeliness`, `completeness`, `validity`, `uniqueness`, `consistency`, `integrity`, `accuracy`, `reconciliation`. |
| `column_name` | Cột hoặc biến thể của check. Để trống nếu check cả dòng. |
| `scope` | `real` là dữ liệu crawl thật (`url_id < 1.000.000`), `test` là dữ liệu sinh (`url_id >= 1.000.000`), `all` là bảng không có `url_id`. |
| `total_rows` | Mẫu số: số dòng, số khóa, số ngày... được kiểm tra. |
| `failed_rows` | Số phần tử vi phạm. |
| `measured_value`, `unit` | Giá trị đem so với ngưỡng. Thường là `%` dòng lỗi (không làm tròn), một số check đo `giờ`, `phút` hoặc hệ số `x`. |
| `warn_threshold`, `fail_threshold` | Ngưỡng cảnh báo và ngưỡng lỗi. |
| `status` | Tự tính khi INSERT (xem quy tắc bên dưới). |
| `sample_value` | Ví dụ giá trị lỗi để debug. Để trống khi không có lỗi. |
| `note` | Giải thích thêm: tổng, phân bố, gợi ý sửa. |

**Quy tắc chấm điểm.** `measured_value` **càng lớn càng xấu**.

```
fail_threshold < 0                -> INFO   (chỉ đo để theo dõi, không chấm điểm)
measured_value > fail_threshold   -> FAIL
measured_value > warn_threshold   -> WARN
còn lại                           -> PASS
```

- Check **"phải bằng 0"** dùng ngưỡng `0 / 0`. Chỉ cần một dòng lỗi trên 10 triệu là FAIL, vì `%` không bị làm tròn.
- Check **chỉ cảnh báo** dùng `fail_threshold = 101`, nên không bao giờ FAIL.
- `pct_pass` trong báo cáo tính trên các check có chấm điểm, **bỏ INFO**.

Bảng phân vùng theo tháng (`toYYYYMM(run_at)`). Xóa một lần chạy hỏng:

```sql
DELETE FROM dq_check_result WHERE run_id = 'dq_manual_20261114_01';
```

---

## 6. Đọc kết quả

### Báo cáo `99_dq_report.sql`

Báo cáo đọc lần chạy **mới nhất**: `argMax(run_id, run_at)`.

| Mục | Nội dung |
|---|---|
| 1. Lần chạy | Số check, PASS / WARN / FAIL / INFO, `pct_pass` |
| 2. Theo tầng | Như mục 1, chia theo `layer` |
| 3. Theo chiều chất lượng | Chia theo `dq_dimension` |
| 4. FAIL / WARN | Danh sách cần xử lý, có giá trị đo, ngưỡng, mẫu lỗi, ghi chú |
| 5. INFO | Các chỉ số theo dõi có `failed_rows > 0` |
| 6. Thay đổi so với lần trước | Check đổi trạng thái, ví dụ PASS → FAIL sau khi sửa job |
| 7. Xu hướng | 10 lần chạy gần nhất |
| 8. Phụ lục (chỉ có trong `.md`) | Toàn bộ dòng kết quả của lần chạy |

- Muốn xem báo cáo của một lần chạy cũ: trong `99_dq_report.sql`, thay biểu thức `rid` bằng `'<run_id>'`.
- Chạy tay ra Markdown: `clickhouse-client ... --format=Markdown --multiquery < 99_dq_report.sql > report.md`. Ra chữ cho terminal thì dùng `--format=PrettyCompactMonoBlock`. Các truy vấn số liệu không ghi `FORMAT`, nên dạng đầu ra do `--format` quyết định.

### Query hay dùng

```sql
-- Các check FAIL / WARN của lần chạy mới nhất
WITH (SELECT argMax(run_id, run_at) FROM dq_check_result) AS rid
SELECT status, layer, table_name, check_id, column_name, failed_rows, total_rows,
       round(measured_value, 3) AS v, unit, sample_value, note
FROM dq_check_result
WHERE run_id = rid AND status IN ('FAIL', 'WARN')
ORDER BY status, layer, check_id;

-- Lịch sử một check qua các lần chạy (ví dụ sau khi sửa loader silver)
SELECT run_id, run_at, status, failed_rows, round(measured_value, 4) AS v
FROM dq_check_result
WHERE check_id = 'S01' AND table_name = 'silver_seller'
ORDER BY run_at;

-- Dùng làm cổng chặn: trả về > 0 nghĩa là lần chạy mới nhất có FAIL
SELECT countIf(status = 'FAIL')
FROM dq_check_result
WHERE run_id = (SELECT argMax(run_id, run_at) FROM dq_check_result);
```

---

## 7. Danh mục check

Cột ngưỡng ghi theo dạng **warn / fail** (đơn vị `%` nếu không ghi khác). **INFO** là chỉ đo, không chấm điểm. **chỉ WARN** là `fail = 101`.

### Tầng raw: `01_raw_dq.sql`

Raw giữ nguyên chuỗi từ crawler, nên ở đây đo lỗi của **nguồn**.

| ID | Bảng | Kiểm tra gì | Chiều | Ngưỡng |
|---|---|---|---|---|
| R01 | product, review | Ngày không có dòng nào, hoặc tụt dưới 50% trung vị (bỏ ngày đầu và cuối) | volume | 0 / 10 |
| R02 | product, review | Độ tươi: số giờ từ lần crawl mới nhất đến hiện tại | timeliness | 24 / 72 giờ |
| R03 | product, review | Thiếu giá trị theo từng cột (rỗng, `NULL`, `undefined`, `N/A`, `[]`...) | completeness | theo mức cột ¹ |
| R04 | product, review | Selector failure: ≥ 3/5 trường lõi (product) hoặc ≥ 3/4 (review) cùng thiếu | completeness | 0,5 / 2 |
| R05 | product, review | Sai định dạng (giá, %, bộ đếm, điểm, chuỗi thời lượng). Không tính `Liên hệ`, `Chưa có đánh giá`. | validity | 0 / 0,5 |
| R06a | product, review | `observed_at` không đọc được | validity | 0 / 0 |
| R06b | product, review | `observed_at` không đúng ISO-8601 có múi giờ | validity | 0 / 0 |
| R06c | product, review | `observed_at` ở tương lai hoặc trước 2020 | validity | 0 / 0 |
| R06d | product, review | `ingested_at` sớm hơn `observed_at` quá 1 phút (lệch đồng hồ) | consistency | 0 / 0,1 |
| R06e | product, review | Độ trễ crawl → ClickHouse, phân vị 95 | timeliness | 10 / 60 phút |
| R07 | product, review | Dòng trùng `idempotency_key`, nội dung giống hệt (Kafka gửi lại; bronze phải loại) | uniqueness | 0 / 1 |
| R08 | product, review | Va chạm khóa: cùng `idempotency_key` nhưng **nội dung khác**, nên `dropDuplicates` sẽ làm mất dữ liệu | uniqueness | 0 / 0 |
| R08b | review | Tỉ lệ lần crawl chỉ còn đúng 1 review (dấu hiệu khóa review gộp mất review). Chỉ chạy với `scope = real`. | completeness | 90 / chỉ WARN |
| R09a | review | `status` ngoài 5 nhãn của Tiki | validity | 0 / 0,5 |
| R09b | review | `status` không khớp `star_score` | consistency | 0 / 1 |
| R09c | review | `images_urls` không phải mảng JSON | validity | 0 / 0 |
| R09d | review | `prod_properties` không phải mảng JSON | validity | 0 / 0 |
| R09e | review | Phần tử ảnh chưa phải URL thuần (tính trên số ảnh) | validity | 0 / chỉ WARN |
| R10 | product, review | `idempotency_key` tính lại từ `(url_id, observed_at)` phải khớp khóa đã lưu, tức khóa là tất định | integrity | 0 / 0 |

¹ Ngưỡng của R03 theo mức quan trọng của cột:

| Mức | Cột | Ngưỡng |
|---|---|---|
| Cột lõi | `url_id`, `title`, `sell_price`, `category_tree`, `observed_at`; review: `url_id`, `user_name`, `star_score`, `observed_at` | 0,5 / 2 |
| Cột quan trọng | `brand`, `seller`, `sold`, `seller_rate`, `seller_rate_cnt`; review: `status`, `joined_duration`, `rated_at`, `reviewed`, `thank_received` | 5 / 20 |
| Comment | `comment` của review | 60 / 95 |
| Còn lại | `price`, `discount`, `product_rate`, `product_rate_cnt`, `brand_origin`, `made_in`, `warranty`, `description`, `additional_info`, `images_urls`, `prod_properties`, `use_time` | INFO |

### Tầng bronze: `02_bronze_dq.sql`

| ID | Kiểm tra gì | Chiều | Ngưỡng |
|---|---|---|---|
| B01a | Khóa có ở raw nhưng không có ở bronze (job làm mất dòng) | reconciliation | 0 / 0 |
| B01b | Khóa ở bronze không có ở raw (orphan) | reconciliation | 0 / 0 |
| B01c | Dòng trùng `idempotency_key` ở bronze (thường do chạy lại job mà không TRUNCATE) | uniqueness | 0 / 0 |
| B02 | Giá trị bronze lệch giá trị **kỳ vọng tính lại độc lập bằng SQL** từ raw, theo từng cột. Sản phẩm: mẫu 5% (`url_id % 20 = 0`) cộng toàn bộ dữ liệu thật. Review: toàn bộ. | accuracy | `url_id`, `observed_at`, `brand_normalized`, `star_score`: 0 / 0; `comment`: 0 / 1; các cột còn lại: 0 / 0,1 |
| B03 | Miền giá trị: giá âm, giá > 10 tỷ, % giảm ngoài [0,100], rating ngoài [0,5], bộ đếm âm, `url_id <= 0`, thời gian lạ | validity | 0 / 0 (giá > 10 tỷ: 0 / 0,01) |
| B04a | Giá bán lớn hơn giá gốc | consistency | 0 / 0,1 |
| B04b | `discount` không khớp `(price - sell_price) / price` (lệch > 1 điểm %) | consistency | 0,1 / 1 |
| B04c | `product_rate` và `product_rate_cnt` mâu thuẫn (một bên 0, một bên > 0) | consistency | 0,1 / 1 |
| B04d | `seller_rate` và `seller_rate_cnt` mâu thuẫn | consistency | 0,1 / 1 |
| B04e | `sell_price = 0` sau chuẩn hóa (nguồn trống, `Liên hệ`, `undefined`) | completeness | 0,5 / 2 |
| B05 | Thiếu `brand` (5 / 20), thiếu `seller` (5 / 20), thiếu `category_tree` (0,5 / 2); có `brand` nhưng `brand_normalized` NULL (0 / 0); tỉ lệ hàng không thương hiệu (INFO) | completeness | theo cột |
| B06a | Brand gộp được nhiều cách viết về 1 `brand_normalized` | consistency | INFO |
| B06b | `brand_normalized` quá ngắn (≤ 2 ký tự) hoặc toàn chữ số | consistency | 0 / chỉ WARN |
| B06c | Cặp `brand_normalized` nghi cùng một brand nhưng chưa gộp (lỗi chính tả hiếm theo Damerau-Levenshtein = 1, hoặc thêm hậu tố) | consistency | INFO |
| B06d | Tên seller khác nhau nhưng trùng khóa chuẩn hóa (seller chưa được gộp) | consistency | 0 / 5 |
| B07a | Độ sâu cây danh mục ngoài [2, 6] | validity | 0,1 / 1 |
| B07b | Cây danh mục có cấp rỗng (`a >  > b`, `>` thừa) | validity | 0 / 0,1 |
| B07c | Cùng tên leaf nhưng thuộc nhiều nhánh cha | consistency | INFO |
| B08a | `sold` tụt về 0 giữa 2 lần crawl (ô "đã bán" trống) | consistency | 0,1 / 2 |
| B08b | `sold` giảm thật (bộ đếm lũy kế không được giảm) | consistency | 0,05 / 1 |
| B08c | `product_rate_cnt` tụt về 0 | consistency | 0,1 / 2 |
| B08d | `product_rate_cnt` giảm thật | consistency | 0,05 / 1 |
| B08e | Seller chập chờn: một trong 2 lần crawl liền nhau bị trống | consistency | 0,5 / 2 |
| B08f | Seller đổi giữa 2 lần crawl (cả 2 lần đều có tên) | consistency | INFO |
| B08g | `category_tree` đổi giữa 2 lần crawl | consistency | 0,01 / 1 |
| B08h | `brand_normalized` đổi giữa 2 lần crawl | consistency | 0,01 / 1 |
| B09a | `sell_price` nhảy quá ±80% giữa 2 lần crawl | validity | 0,1 / 1 |
| B09b | `sell_price` ngoài rào IQR ×3 (thang log) của danh mục cấp 1 | validity | 1 / 5 |
| B10a | `star_score` ngoài 1..5 | validity | 0 / 0,5 |
| B10b | `joined_duration <= 0` (không parse được ngày tham gia) | completeness | 0,5 / 2 |
| B10c | `rated_at <= 0` (không biết review viết lúc nào) | completeness | 1 / 5 |
| B10d | Review cũ hơn tài khoản quá 1 năm | consistency | 0,5 / 2 |
| B10e | `user_name` rỗng | completeness | 0,5 / 2 |
| B10f | `reviewed` / `thank_received` âm | validity | 0 / 0 |

### Tầng silver: `03_silver_dq.sql`

| ID | Kiểm tra gì | Chiều | Ngưỡng |
|---|---|---|---|
| S01 | SK trùng trong `silver_brand`, `silver_seller`, `silver_category`, `silver_product`; `(url_id, observed_at)` trùng trong `silver_snapshot` | uniqueness | 0 / 0 |
| S02a | `silver_product`: mỗi `url_id` có đúng 1 version `is_current` | consistency | 0 / 0 |
| S02b | `is_current` khớp với `valid_to IS NULL` | consistency | 0 / 0 |
| S03 | SK trong `silver_snapshot` / `silver_product` không có trong dimension (orphan) | integrity | 0 / 0 |
| S04a | Lần crawl có ở bronze nhưng không có ở `silver_snapshot` | reconciliation | 0 / 0 |
| S04b | Lần crawl ở `silver_snapshot` không có ở bronze | reconciliation | 0 / 0 |
| S04c | Bảo toàn `count()`, `sum(sell_price)`, `sum(sold)`, `sum(product_rate_cnt)` từ bronze sang silver | reconciliation | 0 / 0,000001 |
| S04d | Số SK của dimension bằng số tổ hợp thuộc tính khác nhau ở bronze | reconciliation | 0 / 0 |
| S05 | Phân mảnh entity: số SK trên một thực thể (brand, seller, product), đơn vị `x` | consistency | INFO |
| S06a | `silver_category`: cấp rỗng nằm giữa 2 cấp có giá trị | validity | 0 / 0 |
| S06b | Danh mục rỗng (sinh từ `category_tree` trống) | validity | 0 / chỉ WARN |
| S06c | `url_id = 0` (dữ liệu nạp trước migration thêm cột `url_id`) | validity | 0 / 0 |

### Tầng gold: `04_gold_dq.sql`

Mọi bảng gold được đọc bằng `FINAL`. Ký hiệu: F1 = `gold_fact_product_snapshot`, F2 = `gold_fact_product_daily`, F3 = `gold_fact_price_change`, F4 = `gold_fact_review`, F5 = `gold_fact_seller_daily`.

| ID | Kiểm tra gì | Chiều | Ngưỡng |
|---|---|---|---|
| G01 | Khóa grain trùng sau `FINAL` (11 bảng) | uniqueness | 0 / 0 |
| G01b | Dòng chưa được merge, tức đọc không có `FINAL` sẽ đếm trùng | uniqueness | INFO |
| G02 | Khóa ngoại của 5 fact không có trong dimension (kể cả `date_key` → `gold_dim_date`) | integrity | 0 / 0 (F4 `review_date_key`: 0 / 1) |
| G02b | F1 trỏ vào unknown member: brand (10 / 30), seller (5 / 20), danh mục rỗng (0,5 / 2) | completeness | theo cột |
| G03a | `gold_dim_product`: không có đúng 1 version current | consistency | 0 / 0 |
| G03b | `version_no` không liên tục 1..n | consistency | 0 / 0 |
| G03c | `is_current` không khớp `valid_to IS NULL` | consistency | 0 / 0 |
| G03d | `valid_to` không nối tiếp `valid_from` của version sau | consistency | 0 / 0 |
| G03e | Snapshot F1 nằm ngoài khoảng hiệu lực của version nó trỏ tới | consistency | 0 / 0,1 |
| G04a | Lần crawl có ở `silver_snapshot` nhưng không có ở F1 | reconciliation | 0 / 0 |
| G04b | Lần crawl ở F1 không có ở `silver_snapshot` (gold chưa dựng lại) | reconciliation | 0 / 0 |
| G04c | Sản phẩm × ngày của F1 không khớp F2 (`n_observations`) | reconciliation | 0 / 0 |
| G04d | Số dòng của dimension và F4 bằng số thực thể ở nguồn (kể cả unknown member) | reconciliation | 0 / 0 |
| G05a | F1: `list_price < sell_price` | validity | 0 / 0 |
| G05b | F1: `discount_amount` khác `list_price - sell_price` | validity | 0 / 0 |
| G05c | F1: `discount_pct` ngoài [0, 100] | validity | 0 / 0 |
| G05d | F1: `date_key` / `hour_of_day` không khớp `observed_at` | consistency | 0 / 0 |
| G05e | F1: `product_rate` / `seller_rate` ngoài [0, 5] | validity | 0 / 0 |
| G05f | F1: `sell_price = 0` | completeness | 0,5 / 2 |
| G06a | F2: open / close / avg nằm ngoài [min, max] của ngày | consistency | 0 / 0 |
| G06b | F2: `revenue_est` khác `round(units_sold_est × avg_sell_price)` | consistency | 0 / 0 |
| G06c | F2: `units_sold_est` NULL ở ngày không phải ngày quan sát đầu tiên | consistency | 0 / 0 |
| G06d | F2: `sold_eod` / `rating_cnt_eod` giảm so với ngày trước | consistency | 0 / 0 |
| G06e | F2: ngày không có lần crawl nào (`days_since_prev_obs > 1`) | completeness | 1 / 5 |
| G06f | F2: `n_observations`, `first/last_observed_at` không khớp ngày | consistency | 0 / 0 |
| G07a | F3: `direction` lệch dấu của thay đổi giá | consistency | 0 / 0 |
| G07b | F3: sự kiện không có thay đổi nào | consistency | 0 / 0 |
| G07c | F3: `change_amount` / `change_pct` tính sai | consistency | 0 / 0 |
| G07d | F3: `old_price_duration_hours` âm, hoặc `old_price_since` sau `changed_at` | consistency | 0 / 0 |
| G07e | F3: `is_list_price_inflated` không khớp quy tắc | consistency | 0 / 0 |
| G07f | F3: đổi giá giả do giá = 0 (nguồn thiếu giá) | validity | 1 / chỉ WARN |
| G08a | F4: `is_positive` / `is_negative` không khớp `star_score` | consistency | 0 / 0 |
| G08b | F4: `has_image` không khớp `image_count` | consistency | 0 / 0 |
| G08c | F4: review không có sao | completeness | 1 / 5 |
| G08d | F4: không xác định được ngày viết review | completeness | 1 / 5 |
| G08e | F4: review chưa gắn được sản phẩm | completeness | 1 / 5 |
| G08f | F4: ngày viết review ước tính sau lúc crawl | consistency | 0 / 0 |
| G08g | F4: `review_date_key` không khớp `reviewed_at_est` | consistency | 0 / 0 |
| G09a | F5: `active_products` khác số sản phẩm của seller trong ngày ở F1 | reconciliation | 0 / 0 |
| G09b | F5: `units_sold_est` / `revenue_est` khác tổng F2 theo seller × ngày | reconciliation | 0 / 0 |
| G09c | F5: `seller_rate_eod` ngoài [0, 5] | validity | 0 / 0 |
| G10a | `gold_dim_date`: lịch bị hở | validity | 0 / 0 |
| G10b | `gold_dim_date`: thuộc tính ngày sai (thứ, tuần ISO, quý, cuối tuần...) | validity | 0 / 0 |
| G10c | `gold_dim_date`: `is_double_day` sai (1.1 ... 12.12) | validity | 0 / 0 |

### Xuyên tầng: `05_cross_layer_dq.sql` và `06_lost_keys_debug.sql`

B01, S04 và G04 so tổng trên toàn bộ dữ liệu. X01 chia **theo ngày** để chỉ ra dữ liệu rơi ở ngày nào, bước nào.

| ID | Kiểm tra gì | Chiều | Ngưỡng |
|---|---|---|---|
| X01a | Ngày có số dòng bronze khác số khóa raw | reconciliation | 0 / 0 |
| X01b | Ngày có số dòng `silver_snapshot` khác bronze | reconciliation | 0 / 0 |
| X01c | Ngày có số dòng F1 khác `silver_snapshot` | reconciliation | 0 / 0 |
| X01d | Ngày có số review bronze khác số khóa review raw | reconciliation | 0 / 0 |
| X02 | *(file 06, không ghi bảng)* Liệt kê ≤ 20 khóa bị rơi / thừa mỗi bước | reconciliation | – |
| X03 | Tầng sau chậm hơn raw bao nhiêu giờ (so `max(observed_at)`) | timeliness | 6 / 24 giờ |

---

## 8. Hàm dùng chung (`00_dq_setup.sql`)

Đây là SQL UDF **toàn server**: tạo một lần, dùng được ở mọi database và mọi phiên.

| Hàm | Làm gì |
|---|---|
| `dq_scope(url_id)` | `'test'` nếu `url_id >= 1.000.000`, ngược lại `'real'` |
| `dq_pct(failed, total)` | `%` lỗi, không làm tròn; trả 0 khi `total = 0` |
| `dq_sample(failed, x)` | Giá trị mẫu dạng chuỗi; trả chuỗi rỗng khi `failed = 0` |
| `dq_missing(s)` | Ô raw được coi là thiếu: rỗng, `NULL`, `NaN`, `None`, `N/A`, `undefined`, `[]` |
| `dq_ts(s)` | Đọc `observed_at` ISO-8601 thành `DateTime64(6, 'Asia/Ho_Chi_Minh')`; lỗi thì trả NULL |
| `dq_expected_count(s)` | Bộ đếm dạng chữ thành số: `'1.234'` → 1234, `'1k'` → 1000, `'(5.6tr+ đánh giá)'` → 5600000 |
| `dq_hours(s)` | Chuỗi thời lượng của review thành số giờ (`năm`, `tháng`, `tuần`, `ngày`, `giờ`, `phút`) |
| `dq_name_key(s)` | Khóa so khớp tên: bỏ dấu tiếng Việt, bỏ hậu tố `official` / `chính hãng`, bỏ ký tự không phải chữ hoặc số |
| `dq_cell(s, n)` | Ô văn bản trong báo cáo: cắt `n` **ký tự** (`substring` cắt theo byte nên làm vỡ chữ tiếng Việt), bỏ xuống dòng, đổi `\|` thành `¦` để không vỡ bảng Markdown |

Các hàm này tính giá trị kỳ vọng **độc lập với code Spark**. Nhờ vậy B02 mới bắt được lỗi của hàm `normalize_*`, thay vì lặp lại chính lỗi đó.

---

## 9. Thêm một check mới

1. Chọn file theo tầng và đặt mã tiếp theo trong nhóm (ví dụ `B11`). Thêm một dòng mô tả vào phần đầu file.
2. Viết theo mẫu dưới đây. Câu SELECT phải trả **đúng 16 cột theo thứ tự**.

```sql
-- ---------------------------------------------------------------- B11 giá bán quá lớn
INSERT INTO dq_check_result (run_id, layer, table_name, check_id, check_name, dq_dimension, column_name, scope,
                             total_rows, failed_rows, measured_value, unit, warn_threshold, fail_threshold, sample_value, note)
SELECT
    {run_id:String}, 'bronze', 'bronze_product_snapshot', 'B11', 'Giá bán > 1 tỷ đồng', 'validity', 'sell_price',
    dq_scope(url_id) AS scope,
    count() AS total, countIf(sell_price > 1e9) AS failed, dq_pct(failed, total), '%', 0, 0.1,
    dq_sample(failed, anyIf(url_id, sell_price > 1e9)), ''
FROM bronze_product_snapshot
GROUP BY scope;
```

3. Giữ các quy ước sau:
   - **Giá trị càng lớn càng xấu.** Nếu cần đo "càng nhỏ càng xấu", hãy đổi thành phần thiếu hụt (ví dụ `100 - %`).
   - **Ngưỡng:** check phải bằng 0 dùng `0, 0`; chỉ theo dõi dùng `fail = -1`; chỉ cảnh báo dùng `fail = 101`.
   - **Chia theo `scope`** khi bảng có `url_id`. Dữ liệu thật và dữ liệu test có chất lượng rất khác nhau.
   - **Bảng lớn:**
     - Gộp nhiều check vào **một lần quét**: tính các `countIf` trong một subquery, rồi `ARRAY JOIN [(...), (...)]` để bung thành nhiều dòng. Các phần tử tuple phải cùng kiểu, nên ép bằng `toUInt64` hoặc `toString` khi cần.
     - So hai tập khóa bằng `UNION ALL` + `GROUP BY cityHash64(khóa)`, không `JOIN` 10 triệu × 10 triệu.
     - Gold luôn đọc bằng `FINAL`.
4. Chạy thử một tầng với `run_id` riêng, rồi xóa lần chạy đó (mục 5):
   `RUN_ID=dq_dev bash run_dq.sh bronze report`.

---

## 10. Xử lý sự cố

| Hiện tượng | Nguyên nhân / cách xử lý |
|---|---|
| `Substitution 'run_id' is not set` | Chạy tay mà thiếu `--param_run_id=...` (mục 4). |
| `Unknown function dq_...` / bảng `dq_check_result` không tồn tại | Chưa chạy `00_dq_setup.sql`. `run_dq.sh` luôn tự chạy file này. |
| `MEMORY_LIMIT_EXCEEDED` | Thiếu hai tham số spill khi chạy tay, hoặc cần giảm `CH_SPILL_BYTES`. |
| Báo cáo đếm gấp đôi | Chạy cùng file hai lần với cùng `run_id`. Xóa lần chạy đó rồi chạy lại với mã mới. |
| B01c, S01 (`silver_snapshot`) hoặc X01 FAIL sau khi chạy lại job | Job bronze và loader `silver_snapshot` **append** toàn bộ dữ liệu đọc được. Phải TRUNCATE rồi dựng lại (xem `test_data/run_test_pipeline.sh`). |
| X03 hoặc G04b FAIL | Đã crawl hoặc nạp thêm raw nhưng chưa chạy lại bronze, silver, gold. |
| Mọi dòng đều `scope = test`, không có R08b | Bảng raw chính không còn dữ liệu crawl thật (`url_id < 1.000.000`). Ngày 14/11/2026, dữ liệu thật đã được chuyển sang database `backup_20261114`. |
| Báo cáo không phải lần chạy mình muốn | Báo cáo luôn lấy lần chạy mới nhất. Xem mục 6 để chọn `run_id` khác. |

---

## 11. Kết quả tham chiếu

Lần chạy **`dq_20261114_201128`** (14/11/2026) trên bộ test 10 triệu dòng:
276 dòng kết quả, gồm **228 PASS, 10 WARN, 7 FAIL, 31 INFO**; `pct_pass` = 93,1%.

Một số check ra đúng con số đã biết trước, cho thấy bộ check đo đúng:
- R07 bắt đúng 50.000 dòng lặp mà bộ sinh test cố ý cài vào.
- B08a bắt 47.780 ô "đã bán" trống, khớp GOLD_LAYER.md 13.16.
- G07f bắt 29.794 lần đổi giá giả, khớp GOLD_LAYER.md 13.3.

Các FAIL của lần chạy này:

| Check | Kết quả | Nguyên nhân |
|---|---|---|
| S01, S04d (`silver_seller` 51, `silver_brand` 1) | SK bị chèn 2 lần | Loader silver dùng `rank() = 1`, bị hòa khi 2 dòng cùng SK và cùng `observed_at`. Nên đổi sang `row_number()`. |
| S02a (1.479 sản phẩm) | 2 version cùng `is_current` | Loader `silver_product` chưa có bước đóng version cũ. Gold tự tính lại nên không bị ảnh hưởng. |
| B10d (6,9%) | Review cũ hơn tài khoản | Lỗi của bộ sinh dữ liệu test. |
| R01 (review) | 3/29 ngày (09–11/11) chỉ có ~1.000 review, trong khi trung vị là ~20.700/ngày | Bộ sinh dữ liệu test: lượng review những ngày gần cuối thấp. |
