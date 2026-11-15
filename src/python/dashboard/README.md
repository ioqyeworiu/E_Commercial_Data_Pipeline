# Dashboard Streamlit: thị trường TMĐT (Tiki.vn)

Đây là app web 6 trang, đọc lớp data mart (`mart_*`) và một số bảng gold trong ClickHouse. Nó chạy đúng **43 truy vấn** trong [`src/sql_script/clickhouse/query_dashboard/`](../../sql_script/clickhouse/query_dashboard/README.md). App không chép SQL vào code Python: file `.sql` là nguồn duy nhất.

| Trang | File SQL | Truy vấn |
|---|---|---|
| Tổng quan thị trường | `01_overview.sql` | [1.1]–[1.6] |
| Danh mục & thương hiệu | `02_category_brand.sql` | [2.1]–[2.8] |
| Người bán | `03_seller.sql` | [3.1]–[3.6] |
| Giá & khuyến mãi | `04_pricing_promotion.sql` | [4.1]–[4.8] |
| Đánh giá khách hàng | `05_review.sql` | [5.1]–[5.7] |
| Sản phẩm (danh sách + chi tiết) | `06_product.sql` | [6.1]–[6.8] |

- Tác dụng và cách lấy số liệu của từng biểu đồ: xem mục [Các biểu đồ: tác dụng và nguồn số liệu](#các-biểu-đồ-tác-dụng-và-nguồn-số-liệu).
- Ý nghĩa, công thức và cách đọc của từng chỉ số: xem [Từ điển chỉ số](#từ-điển-chỉ-số).
- Giải thích các khái niệm phân tích (HHI, Gini, Pareto, Wilson, uplift, giảm giá ảo...) và tác dụng của chúng: xem [Giải thích thuật ngữ](#giải-thích-thuật-ngữ) ở cuối file.

## Chạy

App chạy dưới dạng service `dashboard` trong `docker-compose.yml` ở gốc dự án. Container tên `streamlit_dashboard`, cổng **8501**.

```bash
# từ gốc dự án; --no-deps để KHÔNG recreate clickhouse_server / spark (stack dùng chung)
docker compose up -d --build --no-deps dashboard
# mở http://localhost:8501
```

- **Thư mục mount read-only:** `src/python/dashboard` → `/app` và `query_dashboard/` → `/sql`.
  - Sửa file **SQL**: có hiệu lực ở lần rerun kế tiếp. Catalog được cache theo mtime của file.
  - Sửa file trong **`views/`**: bấm *Rerun* hoặc F5.
  - Sửa file trong **`core/`** hoặc **`.streamlit/config.toml`**: chạy `docker restart streamlit_dashboard`. Bind mount từ Windows không phát sự kiện file, nên module đã import không tự nạp lại.
  - Sửa `requirements.txt` hoặc `dashboard.dockerfile`: build lại bằng lệnh `up --build` ở trên.
- **Kết nối:**
  - ClickHouse ở `clickhouse-server:8123`, qua mạng `clickhouse-net`.
  - User và mật khẩu đọc từ docker secrets (`docker_secrets/clickhouse_admin_*.txt`).
- **Yêu cầu dữ liệu:** các mart phải được nạp trước (`src/python/ETL/e_mart/01..08`). Nếu chưa có mart, app báo lỗi kết nối hoặc thiếu dữ liệu ngay đầu trang.

### Chạy trên host (không cần Docker)

```bash
pip install -r src/python/dashboard/requirements.txt
cd src/python/dashboard && streamlit run app.py
```

Khi chạy trên host, app tự tìm SQL ở `../../sql_script/clickhouse/query_dashboard`. Kết nối mặc định là `localhost:8123`. User và mật khẩu lấy theo thứ tự: biến môi trường `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`, rồi `.env` ở gốc dự án, rồi `docker_secrets/`.

| Biến môi trường | Mặc định | Ý nghĩa |
|---|---|---|
| `CLICKHOUSE_HOST` / `CLICKHOUSE_PORT` / `CLICKHOUSE_DB` | `localhost` / `8123` / `ecommercial_warehouse` | Địa chỉ ClickHouse (HTTP) |
| `CLICKHOUSE_USER_FILE` / `CLICKHOUSE_PASSWORD_FILE` | không có | File secret, được ưu tiên hơn `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` |
| `DASHBOARD_SQL_DIR` | `../../sql_script/clickhouse/query_dashboard` | Thư mục 6 file SQL |

## Kiểm tra

```bash
# Git Bash: thêm MSYS_NO_PATHCONV=1 để /app không bị đổi thành đường dẫn Windows
MSYS_NO_PATHCONV=1 docker exec -w /app streamlit_dashboard python smoke_test.py           # cả hai phần
MSYS_NO_PATHCONV=1 docker exec -w /app streamlit_dashboard python smoke_test.py queries   # 43 truy vấn: toàn sàn + 1 danh mục
MSYS_NO_PATHCONV=1 docker exec -w /app streamlit_dashboard python smoke_test.py pages     # render 6 trang (streamlit AppTest)
```

Phần `queries` in số dòng và thời gian của từng truy vấn, và đánh dấu truy vấn chậm hơn 2,5 giây. Phần `pages` báo exception và `st.error` trên mỗi trang. Lệnh thoát với mã 1 nếu có lỗi.

## Cách app dùng file SQL

`core/sql_catalog.py` tách mỗi file theo dòng header:

```sql
-- [1.2] Xu hướng doanh thu theo ngày | Biểu đồ: combo (...)
-- các dòng comment giải thích...
SELECT ...;
```

- **Tên** sau `]` là tiêu đề của card. Header phải có `| Biểu đồ:` thì mới được tính là một truy vấn.
- **Các dòng comment** ngay sau header trở thành tooltip ⓘ cạnh tiêu đề.
- **Câu SQL** chạy nguyên văn. Mỗi lần chạy, app chỉ truyền các tham số `{tên:Kiểu}` có trong câu đó, và binding làm phía server qua clickhouse-connect (không nối chuỗi vào SQL).
- **Thêm, xóa hoặc đổi ID truy vấn:** sửa `EXPECTED_QUERIES` trong `sql_catalog.py` và trang tương ứng trong `views/`.

Bộ lọc dùng chung nằm trên đầu mỗi trang và được giữ khi chuyển trang:

| Bộ lọc | Mặc định | Ghi chú |
|---|---|---|
| Khoảng ngày (preset 7 / 14 / 30 ngày / toàn kỳ) | 14 ngày tin cậy gần nhất | Kỳ so sánh = cùng số ngày liền trước |
| Danh mục cấp 1 | Tất cả (`''`) | |
| Ngày sale | Sự kiện mới nhất | Chỉ trang 4, áp dụng cho [4.5]–[4.7] |
| Sản phẩm | Sản phẩm có doanh thu cao nhất | Chỉ trang 6 |

Tương tác:

- **Drill-down:** chọn một dòng ở [2.1], hoặc bấm một thanh ở [5.4], để lọc theo danh mục đó.
- **Drill-through:** chọn một dòng ở [4.7], [6.1], [6.2] hoặc [6.3] để mở chi tiết sản phẩm.

## Quy ước trình bày

Theo skill dataviz:

- **Không dùng biểu đồ 2 trục Y.** Các truy vấn mà comment SQL gợi ý "combo / trục phụ" ([1.2], [4.1], [5.3], [6.5]) được vẽ thành 2 panel chung trục X.
- **Màu:**
  - Bảng categorical 8 màu đã kiểm định cho người mù màu, dùng theo thứ tự cố định. Mỗi danh mục cấp 1 giữ một màu trên mọi trang (`core/theme.py`).
  - Dữ liệu có thứ tự (phân khúc giá, mức giảm, nhóm sao, thâm niên) dùng ramp một sắc xanh.
  - Dữ liệu có dấu (tăng trưởng, tăng / giảm giá, 5→1 sao) dùng cặp xanh ↔ đỏ. Chênh lệch quá nhỏ để xám.
  - Màu status (xanh lá / vàng / đỏ) chỉ dùng cho kết luận khuyến mãi, cấu trúc thị trường và điểm rủi ro, và luôn kèm nhãn chữ.
- **Theme sáng / tối:** đổi trong menu ⋮ › Settings. Mặc định theo hệ điều hành. Biểu đồ đổi bộ màu tương ứng.
- **Bảng dữ liệu:** mỗi card có mục "Xem bảng dữ liệu" kèm nút tải CSV.
- **Số liệu:** định dạng kiểu Việt Nam (1.234,5). Tiền tệ rút gọn thành tr / tỷ / nghìn tỷ ₫.

## Các biểu đồ: tác dụng và nguồn số liệu

Mục này mô tả lần lượt 43 biểu đồ, theo thứ tự xuất hiện trên từng trang. Mỗi biểu đồ có 4 phần:

- **Tác dụng:** biểu đồ trả lời câu hỏi gì, đọc thế nào.
- **Nguồn:** bảng ClickHouse được đọc và bộ lọc nào có tác dụng với biểu đồ.
- **Cách tính:** truy vấn làm gì với dữ liệu. Câu SQL đầy đủ nằm trong file `.sql` cùng ID. Định nghĩa từng cột của mart ở [`MART_LAYER.md`](../../../MART_LAYER.md), của gold ở [`GOLD_LAYER.md`](../../../GOLD_LAYER.md).
- **Trình bày:** app vẽ số liệu ra sao: loại biểu đồ, màu, nhãn, tương tác.

Ý nghĩa, công thức và cách đọc của từng chỉ số được gom vào [Từ điển chỉ số](#từ-điển-chỉ-số), nằm ngay trước phần mô tả các dashboard.

### Số liệu đi từ đâu tới biểu đồ

```
Tiki.vn ─► crawler ─► Kafka ─► raw ─► bronze ─► silver ─► gold ─► mart_* ─► 43 truy vấn SQL ─► app Streamlit
```

1. **Crawler** đọc trang sản phẩm khoảng 4,2 giờ một lần. Mỗi lần ghi lại giá bán, giá gốc, % giảm, số "đã bán" lũy kế, rating, số lượt đánh giá, seller và danh mục. Review được crawl riêng.
2. **Gold** là star schema. Dashboard dùng tới 5 bảng fact:

   | Fact | Bảng | Grain |
   |---|---|---|
   | F1 | `gold_fact_product_snapshot` | 1 lần crawl × 1 sản phẩm |
   | F2 | `gold_fact_product_daily` | 1 sản phẩm × 1 ngày: giá mở / đóng / min / max, số bán, doanh thu |
   | F3 | `gold_fact_price_change` | 1 lần đổi giá |
   | F4 | `gold_fact_review` | 1 review |
   | F5 | `gold_fact_seller_daily` | 1 seller × 1 ngày: rating của seller |

3. **Mart** (loader `src/python/ETL/e_mart/01`–`08`) gom gold thành 8 bảng nhỏ. Các quy tắc nghiệp vụ đã được áp sẵn ở đây, nên mỗi truy vấn chạy dưới 1 giây.
4. **App:** giá trị bộ lọc trở thành tham số `{date_from:Date}`, `{date_to:Date}`, `{category_l1:String}`, `{sale_event:String}`, `{product_id:Int64}`.
   - `core/queries.run()` chỉ truyền những tham số có trong câu SQL.
   - ClickHouse chạy câu SQL nguyên văn và trả kết quả về dạng DataFrame. File trong `views/` vẽ từ DataFrame đó.
   - Kết quả được cache 10 phút. Nút **Làm mới dữ liệu** ở thanh bên xóa cache.

| Mart | Grain | Biểu đồ dùng |
|---|---|---|
| M1 `mart_kpi_daily` | 1 ngày, toàn sàn | 1.1, 1.2, 1.5, 1.6, 4.1. Nhiều biểu đồ khác đọc M1 để đếm số ngày tin cậy |
| M2 `mart_category_daily` | ngày × danh mục lá | 1.3, 1.4, 2.1, 2.2 |
| M3 `mart_brand_daily` | ngày × brand × danh mục cấp 1 | 1.1, 2.3, 2.4, 2.5 |
| M4 `mart_seller_daily` | ngày × seller | 1.1, 3.1–3.6 |
| M5 `mart_price_discount_daily` | ngày × cấp 1 × mức giảm × phân khúc giá | 2.6, 4.2 |
| M6 `mart_product_scorecard` | 1 sản phẩm, toàn kỳ đã nạp | 2.7, 2.8, 3.3, 3.6, 4.7, 5.6, 5.7, 6.1–6.4 |
| M7 `mart_sale_event_product` | ngày sale × sản phẩm | 4.3–4.7, 6.8 |
| M8 `mart_review_monthly` | tháng × danh mục lá × thâm niên reviewer | 5.1–5.5 |
| Gold trực tiếp | F2, F3, `gold_dim_product` | 4.8, 6.5–6.7 |

### Quy tắc chung của mọi biểu đồ

- **Doanh thu và số bán là ước tính.** Tiki không công bố doanh số theo ngày, chỉ hiện số "đã bán" lũy kế.
  - Gold tính `units_sold_est` = phần tăng của "đã bán" so với ngày quan sát trước. Gold lấy giá trị lớn nhất tính tới thời điểm đó, nên một lần crawl bị trống không làm số tụt về 0.
  - `revenue_est` = `units_sold_est × giá bán trung bình trong ngày`.
  - Ngày đầu tiên của mỗi sản phẩm chưa có số bán (`NULL`).
- **Chỉ tính ngày tin cậy.** Mọi biểu đồ theo ngày đều lọc `is_reliable_day`. Một ngày bị loại khi crawl được dưới 90% số sản phẩm của ngày đông nhất, hoặc dưới 90% sản phẩm có số bán. Trên dữ liệu test, các ngày bị loại là 07/10, 08/10 và 12/11.
- **Kỳ so sánh** là cùng số ngày lịch, liền trước `date_from`.
  - Tăng trưởng so sánh **trung bình / ngày tin cậy** của hai kỳ, để kỳ trước thiếu ngày (đầu dữ liệu) không làm % bị phóng đại.
  - Kỳ trước không có ngày nào thì ô tăng trưởng để trống.
- **Tỉ lệ tính từ tổng:** `sum(tử) / sum(mẫu)`, không lấy trung bình của các tỉ lệ.
- **Có hai loại "đánh giá" khác nhau:**
  - *Rating* là điểm Tiki hiện trên trang sản phẩm hoặc trang seller. Chỉ tính sản phẩm / seller đã có lượt đánh giá.
  - *Sao review* là trung bình số sao của các review đã crawl. Chỉ tính review có sao (1–5).
- **Thị phần và xếp hạng brand / seller** bỏ nhóm "Không rõ thương hiệu" / "Không rõ người bán".
- **Đổi giá** đã bỏ các lần "đổi giá giả" do nguồn thiếu giá (giá → 0 → giá).
- **Giá bán bình quân và giá niêm yết TB là hai số khác nhau:**
  - Giá bán bình quân = doanh thu / số bán, tức bình quân theo lượng bán.
  - Giá niêm yết TB = trung bình giá đang niêm yết của các sản phẩm, không trọng số.
  - Hàng rẻ bán được nhiều hơn, nên giá bán bình quân thường thấp hơn giá niêm yết TB.

### Từ điển chỉ số

Mọi chỉ số xuất hiện trên dashboard, chia theo 8 nhóm. Mỗi bảng có 4 cột:

- **Ý nghĩa và cách đọc:** chỉ số đo cái gì, cao hay thấp nghĩa là gì, và lưu ý khi đọc.
- **Cách tính:** công thức trên cột của mart hoặc gold. Chữ `Σ` là tổng trên các dòng thuộc phạm vi đang xem (khoảng ngày, danh mục...).
- **Biểu đồ:** ID các biểu đồ dùng chỉ số này.

Viết tắt dùng trong bảng: DT = doanh thu, SP = sản phẩm, TB = trung bình. Khái niệm phía sau các chỉ số (HHI, Gini, Wilson...) được giải thích kỹ hơn, kèm ví dụ, ở [Giải thích thuật ngữ](#giải-thích-thuật-ngữ) cuối file.

#### Doanh số và quy mô

| Chỉ số | Ý nghĩa và cách đọc | Cách tính | Biểu đồ |
|---|---|---|---|
| Doanh thu ước tính | Số tiền khách trả cho hàng đã bán trong kỳ (₫). Vì là ước tính, nên dùng để so sánh tương đối giữa ngày, danh mục, brand, seller, không dùng như số kế toán | Σ `revenue_est`. Với mỗi SP-ngày, `revenue_est` = `units_sold_est × giá bán TB các lần crawl trong ngày` | 1.1–1.4, 2.1–2.5, 3.2–3.5, 4.3, 4.4, 6.1, 6.4, 6.5 |
| Số bán ước tính | Số đơn vị hàng bán ra | Σ `units_sold_est`. Với mỗi SP-ngày, `units_sold_est` = phần tăng của "đã bán" lũy kế so với ngày quan sát trước. Ngày đầu tiên của sản phẩm là `NULL` và được cộng như 0 | 1.1, 1.3, 2.1, 2.3, 3.3, 6.1, 6.4, 6.5 |
| Doanh thu / ngày, số bán / ngày | Quy mô trung bình một ngày. Dùng được để so hai kỳ có số ngày khác nhau | Tổng của kỳ / số ngày tin cậy trong kỳ. [1.5], [1.6] lấy `avg(revenue_est)` theo loại ngày hoặc theo thứ | 1.1, 1.5, 1.6 |
| TB trượt 7 ngày | Làm mượt dao động từng ngày để thấy xu hướng | `avg(revenue_est)` của ngày đó và 6 ngày tin cậy liền trước, chỉ trong khoảng đang lọc | 1.2 |
| Tăng trưởng so với kỳ trước | Dương = đang tăng. So theo TB / ngày nên kỳ thiếu ngày không bị thiệt | (DT kỳ này / số ngày tin cậy kỳ này) ÷ (DT kỳ trước / số ngày tin cậy kỳ trước) − 1, nhân 100. Để trống khi kỳ trước không có ngày tin cậy hoặc có DT = 0 | 1.1, 1.3, 2.1, 2.3, 2.5, 3.3 |
| Chênh lệch so với TB tuần | Thứ nào trong tuần bán mạnh hoặc yếu hơn bình thường. Trong khoảng ±2% coi như không khác | DT TB của thứ đó ÷ TB của 7 thứ − 1. Đã bỏ ngày sale và ngày ngay sau sale | 1.6 |
| Sản phẩm TB / ngày | Độ rộng của danh mục, brand hoặc seller: TB mỗi ngày có bao nhiêu sản phẩm đang bày bán (được crawl) | Σ `products_observed` / số ngày tin cậy | 1.3, 2.1, 2.3, 3.2, 3.3 |
| Sản phẩm có bán / ngày | TB mỗi ngày có bao nhiêu sản phẩm bán được ít nhất 1 đơn vị | TB của `products_selling` qua các ngày | 1.1 |
| % SP có bán (sell-through) | Tỉ lệ sản phẩm có đơn trong ngày. Thấp nghĩa là nhiều sản phẩm bày bán mà không bán được | Σ `products_selling` / Σ `products_sales_known` × 100 | 1.5, 2.1, 4.2 |
| Bán TB / sản phẩm / ngày | Sức bán của một sản phẩm điển hình. Dùng để so các nhóm có số sản phẩm khác nhau (loại ngày, mức giảm, official / thường) | Σ `units_sold_est` / Σ số SP-ngày đã biết số bán | 1.5, 3.2, 4.2 |
| Bán TB / ngày của một sản phẩm | Như trên nhưng tính riêng từng sản phẩm, trên toàn kỳ | `units_sold_total / days_sales_known` (M6). [5.7] lấy TB của chỉ số này qua các sản phẩm trong nhóm | 5.7, 6.4 |
| Doanh thu TB / sản phẩm | DT toàn kỳ của một sản phẩm điển hình trong nhóm | TB `revenue_total` (M6) | 5.7 |

#### Giá và giảm giá

| Chỉ số | Ý nghĩa và cách đọc | Cách tính | Biểu đồ |
|---|---|---|---|
| Giá bán bình quân | Giá TB của một đơn vị đã bán, có trọng số theo lượng bán. Phản ánh khách thực sự mua ở mức giá nào | Σ DT / Σ số bán | 1.1, 1.3, 2.3, 3.2 |
| Giá niêm yết TB | Giá TB của các sản phẩm đang bày bán, mỗi SP-ngày tính một lần, không trọng số. Thường cao hơn giá bán bình quân vì hàng rẻ bán được nhiều hơn | Σ `sell_price_sum` / Σ `priced_products`, chỉ tính giá đóng cửa > 0 | 2.1 |
| Giá hiện tại, giá gốc hiện tại | Giá bán và giá gốc (giá gạch ngang) của ngày cuối cùng có giá > 0 | M6 `current_sell_price`, `current_list_price` | 6.1–6.4 |
| Giá TB của nhóm sản phẩm | Mức giá điển hình của một nhóm ABC hoặc một nhóm sao | TB `current_sell_price` | 2.7, 5.7 |
| Giá thấp nhất, cao nhất, TB của sản phẩm | Biên độ giá của sản phẩm trên cả kỳ, đã bỏ giá 0 | M6 `min_sell_price`, `max_sell_price`, `avg_sell_price` | 6.4 |
| Giá mở cửa, đóng cửa, min, max trong ngày | Giá bán ở lần crawl đầu và cuối ngày, thấp nhất và cao nhất trong ngày. min = 0 nghĩa là trong ngày có lần crawl thiếu giá | gold F2 | 6.5 |
| % giảm giá TB | Mức giảm **hiển thị** TB (so với giá gốc), tính trên mọi sản phẩm, kể cả sản phẩm không giảm (= 0%). % giảm cao chưa chắc là rẻ thật: xem % giảm thật ở nhóm "Ngày sale" | Σ `discount_pct_sum` / Σ `products_observed`. Mỗi SP-ngày góp % giảm TB của các lần crawl trong ngày | 1.1, 1.2, 1.5, 2.1, 2.3, 3.2, 3.3 |
| % giảm hiện tại | % giảm TB của ngày cuối cùng có giá | M6 `current_discount_pct` | 6.1, 6.2, 6.4 |
| % SP-ngày có giảm giá | Mức độ phổ biến của khuyến mãi | Σ `discounted_products` / Σ `products_observed` × 100 | 1.1 (tooltip) |
| % SP giảm ≥ 30% | Mức độ phổ biến của giảm giá sâu | Σ `deep_discount_products` / Σ `products_observed` × 100. Giảm sâu = % giảm cao nhất trong ngày ≥ 30 | 1.5 (tooltip) |
| Phân khúc giá | Nhóm theo giá bán: < 100k, 100k–300k, 300k–1tr, 1tr–3tr, 3tr–10tr, ≥ 10tr. "Không rõ giá" (giá = 0) bị bỏ khỏi biểu đồ | Hàm `mart_price_band(giá)`. M5 dùng giá đóng cửa của SP-ngày ; M6 dùng giá hiện tại ; M7 dùng giá bình thường | 2.6, 6.4 |
| Tỉ trọng DT của phân khúc | Danh mục kiếm tiền chủ yếu ở mức giá nào | DT của phân khúc / DT của danh mục × 100 | 2.6 |
| Mức giảm giá | Nhóm theo % giảm hiển thị: không giảm, < 10%, 10–20%, 20–30%, 30–40%, ≥ 40% | Hàm `mart_discount_band(avg_discount_pct)` | 4.2 |
| Chỉ số so với nhóm không giảm | Ví dụ "×1,10" = sản phẩm ở mức giảm này bán nhiều hơn 10% so với sản phẩm không giảm, xét ở ngày thường. Đây là tương quan, không phải nhân quả | Bán TB / SP / ngày của mức giảm ÷ bán TB / SP / ngày của nhóm "không giảm" | 4.2 |
| % DT ngày thường của mức giảm | Phần doanh thu ngày thường đến từ mỗi mức giảm | DT của mức / tổng DT ngày thường trong phạm vi × 100 | 4.2 |

#### Đổi giá và nghi giảm giá ảo theo ngày

Nhóm này dùng gold F3: mỗi dòng là một lần giá bán hoặc giá gốc khác lần crawl trước. Mọi chỉ số đều đã bỏ "đổi giá giả", tức các lần giá cũ hoặc giá mới bằng 0 do nguồn thiếu giá.

| Chỉ số | Ý nghĩa và cách đọc | Cách tính | Biểu đồ |
|---|---|---|---|
| Số lần đổi giá | Nhịp thay đổi giá. Tăng vọt trước ngày sale là dấu hiệu chuẩn bị khuyến mãi (hoặc nâng giá gốc) | Đếm dòng F3 có giá cũ > 0 và giá mới > 0 | 1.1 (tooltip), 2.1, 3.6, 4.1, 6.4 |
| Tăng giá, giảm giá | Chiều thay đổi của giá bán | Đếm theo `direction` = 'up' / 'down' | 4.1, 4.8, 6.4 (tooltip), 6.6 |
| Chỉ đổi giá gốc | Giá bán giữ nguyên, chỉ giá gạch ngang thay đổi. Trên dữ liệu test luôn bằng 0 | Tổng số lần đổi giá − tăng giá − giảm giá | 4.1 |
| Đổi giá / 100 SP | Nhịp đổi giá đã chia theo số sản phẩm, để so các ngày có số sản phẩm khác nhau | `price_changes / products_observed × 100` | 4.1 (tooltip) |
| % đổi giá | Biên độ của một lần đổi giá bán. Âm = giảm giá | (giá mới − giá cũ) / giá cũ × 100 | 4.8, 6.6 |
| Giá cũ giữ (giờ) | Mức giá trước đó đã tồn tại bao lâu trước khi đổi. Giữ rất ngắn rồi đổi lại là dấu hiệu thử giá | `old_price_duration_hours` | 6.6 |
| Số ngày có đổi giá | Ngày có ít nhất một lần giá bán thay đổi, đánh dấu bằng chấm trên đường giá | gold F2 `price_change_cnt > 0` | 6.5 |
| Nghi giảm giá ảo (số lần) | Lần đổi giá mà giá gốc tăng **và** % giảm cũng tăng: dấu hiệu shop nâng giá gốc để % giảm trông lớn hơn. Đây là quy tắc heuristic của gold, trên dữ liệu test đúng khoảng 97% số lần báo và bắt được khoảng 83% trường hợp thật. Thô hơn kết luận "giảm giá ảo" của M7 | Đếm dòng F3 có `is_list_price_inflated` | 1.1 (tooltip), 2.1, 3.3, 3.6, 4.1, 4.8, 6.1, 6.4, 6.6 |
| Nghi giảm giá ảo / 1.000 lần đổi giá | So mức độ giảm giá ảo giữa các nhóm seller có quy mô khác nhau | `inflated_events / price_changes × 1000` | 3.2 |

#### Ngày sale (M7)

M7 xét từng sản phẩm × từng ngày sale D. Mốc so sánh là 30 ngày trước D, theo cách của Chỉ thị Omnibus của EU: khi quảng cáo giảm giá, "giá trước giảm" phải là giá thấp nhất trong 30 ngày trước đó.

| Chỉ số | Ý nghĩa và cách đọc | Cách tính | Biểu đồ |
|---|---|---|---|
| Ngày nền | Những ngày dùng làm mốc "bình thường". Càng nhiều ngày nền thì giá tham chiếu càng đáng tin. 11.11 có 30 ngày nền, 10.10 chỉ có khoảng 2 ngày vì dữ liệu bắt đầu 07/10 | Các ngày trong [D − 30, D − 1] không phải ngày sale. [4.3] hiện số ngày nền TB | 4.3 |
| Giá bình thường | Giá bán thường ngày của sản phẩm | Trung vị giá bán các ngày nền (giá mỗi ngày = trung vị các lần crawl, bỏ giá 0) | 4.7, 6.8 |
| Giá gốc thường | Giá gạch ngang thường ngày | Trung vị giá gốc các ngày nền | 4.7, 6.8 |
| Giá thấp nhất 30 ngày | Mốc theo chuẩn Omnibus | Giá bán thấp nhất mọi ngày trong [D − 30, D − 1], kể cả ngày sale trước đó | 6.8 |
| Giá ngày sale, giá gốc ngày sale | Giá thực tế trong ngày D | Trung vị giá bán / giá gốc các lần crawl ngày D, bỏ giá 0 | 4.7, 6.8 |
| % giảm hiển thị | Con số % giảm mà trang sản phẩm quảng cáo | % giảm lớn nhất trang ghi trong ngày D (`shown_discount_pct`). [4.3]–[4.5] lấy TB qua các sản phẩm | 4.3–4.5, 4.7, 6.8 |
| % giảm thật | Giá ngày sale rẻ hơn giá bình thường bao nhiêu. Âm nghĩa là ngày sale còn **đắt hơn** ngày thường | (giá bình thường − giá ngày sale) / giá bình thường × 100. [4.3]–[4.6] lấy TB qua các sản phẩm | 4.3–4.7, 6.8 |
| % nâng giá gốc | Giá gạch ngang ngày sale cao hơn giá gốc thường bao nhiêu. Dương lớn là dấu hiệu giảm giá ảo | (giá gốc ngày sale − giá gốc thường) / giá gốc thường × 100 | 4.6, 4.7, 6.8 |
| Chênh lệch hiển thị − thật | Phần "giảm trên giấy", tính bằng điểm % | % giảm hiển thị TB − % giảm thật TB | 4.5 |
| Kết luận khuyến mãi | Phân loại mỗi sản phẩm trong ngày sale. Xét theo thứ tự: (1) **không đủ dữ liệu**: không có giá ngày D hoặc không có ngày nền ; (2) **giảm giá ảo**: nâng giá gốc ≥ 10% và giảm thật < 5% ; (3) **giảm thật**: giảm thật ≥ 5% ; (4) **giảm danh nghĩa**: ghi giảm ≥ 10% nhưng giá bằng ngày thường, tức mức giảm đã có từ trước ; (5) **không giảm**: còn lại | `discount_verdict` | 4.3–4.7, 6.8 |
| Thấp nhất 30 ngày (có / không) | Giá ngày sale có thật sự là giá tốt nhất tháng không | Giá ngày sale ≤ giá thấp nhất 30 ngày | 6.8 |
| SP có mặt, SP tham gia, % tham gia | SP có mặt = sản phẩm được crawl trong ngày D. SP tham gia = sản phẩm ghi giảm ≥ 10%. % tham gia = tham gia / có mặt | Đếm trên M7 | 4.3, 4.5, 4.6 |
| % giảm thật ≥ 5% | Trong các sản phẩm tham gia, bao nhiêu % rẻ hơn ngày thường thật sự | Số SP tham gia có kết luận "giảm thật" / số SP tham gia × 100 | 4.3 |
| % giá thấp nhất 30 ngày | Trong các sản phẩm tham gia, bao nhiêu % có giá tốt nhất tháng | Số SP tham gia có giá ≤ giá thấp nhất 30 ngày / số SP tham gia × 100 | 4.3 |
| Số và % sản phẩm giảm giá ảo | Mức độ giảm giá ảo trong một sự kiện, danh mục hoặc seller. **Mẫu số khác nhau:** [4.3] và [4.5] chia cho số SP tham gia ; [4.6] chia cho mọi sản phẩm của seller có mặt ngày sale | Đếm SP có kết luận "giảm giá ảo" / mẫu số × 100 | 4.3, 4.5, 4.6 |
| Tỉ lệ SP theo kết luận | Cơ cấu khuyến mãi của một sự kiện, tổng = 100% | Số SP của kết luận / số SP có mặt × 100 | 4.4 |
| Doanh thu, số bán ngày sale | Doanh số trong ngày D. [4.6] chỉ cộng các sản phẩm giảm giá ảo của seller | Σ `revenue_event_day`, Σ `units_event_day` | 4.3, 4.4, 4.6, 4.7, 6.8 |
| Bán ngày sau | Số bán ngày D + 1. Crawler chạy khoảng 4,2 giờ một lần nên một phần số bán của ngày sale được ghi sang hôm sau | Σ `units_next_day` | 4.3, 6.8 |
| Số bán nền | Sản phẩm bán TB bao nhiêu mỗi ngày thường | TB `units_sold_est` các ngày nền, bỏ thêm ngày ngay sau một ngày sale và ngày mà số bán gộp nhiều ngày không crawl | 6.8 |
| Uplift (từng sản phẩm) | Ngày sale bán gấp bao nhiêu lần ngày thường. "×3,7" = gấp 3,7 lần. Khi gộp nhiều sản phẩm thì lấy **trung vị**, vì vài sản phẩm có nền rất nhỏ cho uplift cực lớn | Số bán ngày D / số bán nền. Để trống khi số bán nền = 0 | 4.3, 4.4, 6.8 |
| Uplift (toàn sàn) | Khác với uplift từng sản phẩm: so số TB của cả loại ngày (ngày sale, ngày sau sale) với ngày thường | Chỉ số của loại ngày ÷ chỉ số của ngày thường | 1.5 |

#### Thị phần và độ tập trung

| Chỉ số | Ý nghĩa và cách đọc | Cách tính | Biểu đồ |
|---|---|---|---|
| Thị phần | % doanh thu của một danh mục, brand hoặc seller trong một tổng. Mẫu số khác nhau theo biểu đồ, xem bảng ngay dưới | DT của thực thể / mẫu số × 100 | 1.3, 2.1, 2.3, 2.4, 3.1–3.3 |
| HHI | Mức độ một danh mục bị vài brand chi phối, thang 0–10.000. Dưới 1.000 là phân tán, 1.000–1.800 là tập trung vừa, trên 1.800 là tập trung cao (ngưỡng của Bộ Tư pháp Mỹ, 2023). Một brand chiếm 100% thì HHI = 10.000 | Σ (thị phần % của từng brand)², bỏ brand unknown và brand có DT 0 | 2.4 |
| Cấu trúc thị trường | Kết luận từ HHI: phân tán, tập trung vừa, tập trung cao | Theo ngưỡng của HHI | 2.4 |
| Top 1 / 5 / 10 brand | Brand lớn nhất, 5 brand và 10 brand lớn nhất chiếm bao nhiêu % DT danh mục | Σ thị phần của k brand đầu | 2.4 |
| Số brand | Số brand có DT trong danh mục | Đếm brand có DT > 0 | 2.4 |
| Top x% seller | x% seller lớn nhất chiếm bao nhiêu % DT. Ví dụ "top 10% → 60%" là phân bố rất lệch | DT của `ceil(n × x%)` seller đầu / tổng DT | 3.5 |
| Gini | Mức chênh lệch doanh thu giữa các seller: 0 = đều nhau, 1 = một seller chiếm hết. Seller không bán được gì vẫn được tính, nên Gini cao một phần vì có nhiều seller DT 0 | (2 × Σ i·xᵢ / Σ xᵢ − (n + 1)) / n, với xᵢ là DT seller, i là thứ hạng tăng dần | 3.5 |
| Nhóm ABC | Phân loại sản phẩm theo đóng góp doanh thu **toàn sàn, toàn kỳ**. A = các sản phẩm đầu bảng tạo 80% DT, B = 80–95%, C = phần còn lại, N = không có DT. Lọc danh mục không xếp lại nhóm | Xếp sản phẩm theo DT giảm dần. Xét % DT lũy kế **trước** sản phẩm: < 80% là A, < 95% là B, còn lại là C | 2.7, 5.7, 6.1, 6.3, 6.4 |
| % sản phẩm, % DT của nhóm ABC | Quy luật Pareto: ít sản phẩm tạo phần lớn doanh thu | Số SP (DT) của nhóm / tổng × 100 | 2.7 |
| Hạng doanh thu | 1 = sản phẩm có DT toàn kỳ cao nhất toàn sàn | M6 `revenue_rank` | 6.1, 6.3, 6.4 |
| % DT lũy kế | Từ sản phẩm hạng 1 tới hết sản phẩm này chiếm bao nhiêu % DT toàn sàn | M6 `revenue_cum_share_pct` | 6.1, 6.4 (tooltip) |
| Đường Pareto | Top x% sản phẩm tạo y% DT. **Khác ABC:** thứ hạng được tính lại trong danh mục đang lọc | Bách phân vị `ceil(hạng / n × 100)` → % DT lũy kế ở cuối mỗi bách phân vị | 2.8 |
| Nhóm của brand trong ma trận | Ngôi sao: tăng trưởng ≥ 0 và rating ≥ TB nhóm. Tăng nhưng rating thấp: tăng trưởng ≥ 0, rating dưới TB. Rating tốt nhưng chững lại: tăng trưởng < 0, rating ≥ TB. Cần chú ý: còn lại | TB nhóm = TB rating của 30 brand trên biểu đồ | 2.5 |

Mẫu số của thị phần theo từng biểu đồ:

| Biểu đồ | Mẫu số |
|---|---|
| [1.3] | Tổng DT kỳ này của mọi danh mục cấp 1, toàn sàn |
| [2.1] | Tổng DT các dòng trong bảng: mọi danh mục cấp 1, hoặc các danh mục cấp 2 của danh mục đang chọn |
| [2.3] | Tổng DT các brand đã biết trong phạm vi lọc |
| [2.4] | Tổng DT các brand đã biết trong từng danh mục cấp 1 |
| [3.1], [3.2] | Tổng DT các seller đã biết trong phạm vi lọc |
| [3.3] | Tổng DT **toàn sàn** trong kỳ: không lọc danh mục, tính cả "Không rõ người bán" |

#### Rating và review

Dashboard có hai thước đo mức hài lòng, đến từ hai nguồn khác nhau:
- **Rating** là điểm Tiki hiện trên trang sản phẩm hoặc trang seller. Nó tổng hợp mọi lượt đánh giá từ trước tới nay.
- **Sao review** được tính từ các review mà crawler thu được. Nhờ vậy có thể tách theo sao, danh mục, tháng và thâm niên người viết.

Hai số này có thể lệch nhau.

| Chỉ số | Ý nghĩa và cách đọc | Cách tính | Biểu đồ |
|---|---|---|---|
| Rating TB (sản phẩm) | Điểm sao trung bình Tiki hiển thị, lấy qua các sản phẩm đã có đánh giá. Không trọng số theo số lượt đánh giá | Σ `rating_sum` / Σ `rated_products`. Mỗi SP-ngày góp rating cuối ngày, chỉ khi số lượt đánh giá > 0 | 1.1, 2.1, 2.3, 2.5, 3.2 |
| Rating trên Tiki, số lượt đánh giá (một sản phẩm) | Điểm và số lượt Tiki hiển thị ở ngày quan sát cuối | M6 `rating_latest`, `rating_cnt_latest` | 6.3, 6.4 |
| Lượt đánh giá mới | Số lượt đánh giá tăng thêm so với ngày trước. Có thể dùng thay cho số đơn đã giao và được chấm điểm | Σ `new_ratings` cho sản phẩm, Σ `new_seller_ratings` cho seller | 1.1 (tooltip), 3.3, 3.4 |
| Rating seller | Điểm seller Tiki hiển thị cuối ngày. Seller chưa có lượt đánh giá (điểm 0) bị bỏ | `seller_rate_eod` (gold F5). [3.1], [3.3] lấy ngày cuối kỳ ; [3.2] lấy TB qua các dòng seller-ngày ; [3.4] vẽ từng ngày | 3.1–3.4, 3.6 |
| Thay đổi rating seller | Uy tín seller đang lên hay xuống trong kỳ. Giảm ≥ 0,2 điểm được tính là một dấu hiệu rủi ro | Rating ngày cuối − rating ngày đầu kỳ, chỉ xét ngày seller có đánh giá | 3.6 |
| Số review, review có sao | Số review đã crawl. Review có sao (1–5) là mẫu số của mọi tỉ lệ review | `n_reviews`, `n_rated_reviews` | 3.6, 5.1, 5.3–5.6, 6.3, 6.4 |
| Sao TB (review) | TB số sao của các review đã crawl | Σ `star_sum` / Σ `n_rated_reviews` | 2.7, 3.3, 5.1, 5.3–5.6, 6.1–6.4 |
| % tích cực, % tiêu cực | Review ≥ 4 sao và review 1–2 sao. Review 3 sao là trung tính, không thuộc nhóm nào. Dashboard coi **% tiêu cực ≥ 15%** là đáng lo | Σ `n_positive` / Σ `n_rated_reviews` × 100 ; Σ `n_negative` / Σ `n_rated_reviews` × 100 | 3.3, 3.6, 5.1, 5.3–5.6, 6.3, 6.4 |
| Phân bố sao | Khách hài lòng đồng đều hay chia thành hai phe | Số review k sao / tổng review có sao × 100, với k = 1…5 | 5.2, 6.4 |
| Net sentiment | Thang −100 … 100. Dương = số review 5 sao nhiều hơn số review 1–3 sao. Khắt khe hơn % tích cực vì coi 3 sao là chưa hài lòng | (5 sao − 1 sao − 2 sao − 3 sao) / review có sao × 100 | 5.1 |
| % review có ảnh | Mức đầu tư của người viết. Review có ảnh thường đáng tin hơn | Σ `n_with_image` / Σ `n_reviews` × 100 | 5.1, 5.3–5.5 |
| % review có bình luận | Bao nhiêu review có nội dung chữ, không chỉ chấm sao | Σ `n_with_comment` / Σ `n_reviews` × 100 | 5.1 |
| Số từ TB / bình luận | Độ dài nội dung review | Σ `word_sum` / Σ `n_with_comment` | 5.1, 5.4, 5.5 |
| Cận dưới Wilson 95% | % tiêu cực "ít nhất là bao nhiêu", với độ tin cậy 95%. Nhóm ít review bị kéo xuống nhiều hơn, nên xếp hạng theo cận dưới công bằng hơn xếp theo % thô | (p + z²/2n − z·√(p(1−p)/n + z²/4n²)) / (1 + z²/n), với p = % tiêu cực, n = số review có sao, z = 1,96 | 5.6 |
| Thâm niên reviewer | Tài khoản của người viết review đã tham gia Tiki bao lâu: dưới 1 năm, 1–3 năm, 3–5 năm, trên 5 năm, không rõ | `tenure_band`, lấy từ trang review | 5.5 |
| Tháng viết review | Tháng ước tính, vì nguồn chỉ ghi kiểu "4 năm trước". Review không xác định được ngày bị bỏ khỏi biểu đồ theo tháng | `review_month` (M8) | 5.3 |
| Nhóm sao của sản phẩm | Xếp sản phẩm theo sao TB từ review: chưa có review, < 3, 3–3,5, 3,5–4, 4–4,5, ≥ 4,5 | Theo `avg_star` của M6 | 5.7 |
| % thuộc nhóm A | Trong một nhóm sao, bao nhiêu % sản phẩm thuộc nhóm tạo 80% DT | Số SP nhóm A / số SP của nhóm × 100 | 5.7 |

#### Sản phẩm

| Chỉ số | Ý nghĩa và cách đọc | Cách tính | Biểu đồ |
|---|---|---|---|
| Bán 7 ngày gần nhất, bán 7 ngày trước | Số bán của tuần gần nhất và tuần liền trước, tính tới ngày tin cậy cuối cùng (`as_of_date`) | Σ `units_sold_est` trong [as_of − 6, as_of] và [as_of − 13, as_of − 7] | 6.2, 6.4 |
| Tăng trưởng 7 ngày | Sản phẩm đang tăng tốc hay chững lại. Nếu 7 ngày gần nhất chứa ngày sale thì gần như mọi sản phẩm đều "tăng" | (bán 7 ngày gần nhất − bán 7 ngày trước) / bán 7 ngày trước × 100. Để trống khi bán 7 ngày trước = 0 | 6.2, 6.4 |
| Xu hướng | Nhãn tóm tắt của tăng trưởng 7 ngày: tăng (≥ +20%), giảm (≤ −20%), ổn định, mới có doanh số (7 ngày trước = 0), không bán (cả hai tuần = 0) | M6 `sales_trend` | 6.1, 6.4 |
| Số bán toàn kỳ, DT toàn kỳ | Doanh số của sản phẩm trong toàn bộ dữ liệu đã nạp | M6 `units_sold_total`, `revenue_total` | 6.1, 6.3, 6.4 |
| "Đã bán" lũy kế trên Tiki | Con số Tiki hiện ở ngày cuối. Lớn hơn số bán toàn kỳ vì gồm cả lượng bán trước khi bắt đầu crawl | M6 `sold_cum_latest` | 6.4 (tooltip) |
| Ngày quan sát | Sản phẩm có mặt trong dữ liệu từ ngày nào tới ngày nào, bao nhiêu ngày | M6 `first_seen_date`, `last_seen_date`, `days_observed` | 6.4 |
| Số lần crawl trong ngày | Số lần crawler thấy sản phẩm trong ngày. Ít lần crawl thì giá và số bán của ngày đó kém chắc chắn hơn | gold F2 `n_observations` | 6.5 (tooltip) |
| Số phiên bản thuộc tính | Lớn hơn 1 nghĩa là sản phẩm đã đổi tiêu đề, xuất xứ hoặc bảo hành | M6 `n_versions` ; [6.7] liệt kê từng phiên bản | 6.4, 6.7 |
| Số ngày hiệu lực | Một phiên bản thuộc tính tồn tại bao lâu. Bản hiện tại được tính tới lúc chạy truy vấn | (`valid_to`, hoặc bây giờ) − `valid_from`, đổi ra ngày | 6.7 |

#### Người bán

| Chỉ số | Ý nghĩa và cách đọc | Cách tính | Biểu đồ |
|---|---|---|---|
| Seller hoạt động, brand hoạt động | Số seller hoặc brand có ít nhất một sản phẩm được crawl trong các ngày tin cậy của kỳ. Không tính "Không rõ" | Đếm khóa khác nhau (`uniqExact`) trong M4 / M3 | 1.1, 3.1 |
| Seller có doanh thu | Số seller bán được hàng trong kỳ | Đếm seller có DT > 0 | 3.1 |
| Shop official | Shop chính hãng: tên chứa "official" / "chính hãng", hoặc là "tiki trading" | Cờ `is_official` / `seller_is_official` | 3.1–3.3, 3.6, 4.6, 6.4 |
| % seller official, thị phần official | Shop official chiếm bao nhiêu % số seller và bao nhiêu % doanh thu. Thị phần lớn hơn tỉ lệ số lượng nghĩa là shop official bán tốt hơn trung bình | Số shop official / tổng seller × 100 ; DT shop official / tổng DT seller × 100 | 3.1 |
| Doanh thu trung vị / seller | DT của seller "điển hình", không bị vài seller lớn kéo lệch. Tính cả seller không bán được, nên có thể bằng 0 | Trung vị DT kỳ này qua các seller | 3.1 |
| Doanh thu TB / seller có bán | DT TB của các seller có bán được hàng | Σ DT / số seller có DT > 0 | 3.1 |
| Doanh thu / seller / ngày | Để so nhóm official và nhóm thường trên cùng một thước đo | Σ DT / số seller / số ngày tin cậy | 3.2 |
| Sản phẩm / seller | Số sản phẩm một seller đang bày bán | [3.1]: TB qua các seller của (số SP TB / ngày). [3.2]: Σ `products_observed` / số seller / số ngày tin cậy | 3.1, 3.2 |
| Danh mục chính | Danh mục cấp 1 chiếm nhiều sản phẩm-ngày nhất của seller trên cả kỳ. Bộ lọc danh mục của trang Người bán lọc theo cột này | M4 `main_category_level_1` | 3.1–3.6 |
| Điểm rủi ro (0–3) | Số dấu hiệu cảnh báo của seller. 🔴 3 = cao, 🟠 2 = vừa, 🟡 1 = thấp | Cộng 1 điểm cho mỗi dấu hiệu: ≥ 3 lần nghi giảm giá ảo trong kỳ ; ≥ 15% review 1–2 sao (từ 30 review có sao) ; rating seller giảm ≥ 0,2 trong kỳ | 3.6 |

### Dashboard 1: Tổng quan thị trường (`01_overview.sql`)

Bộ lọc: khoảng ngày. Trang này tính trên toàn sàn, không lọc danh mục.

#### [1.1] Thẻ KPI có so sánh kỳ trước

- **Tác dụng:** cho biết trong một lần nhìn thị trường lớn cỡ nào và đang tăng hay giảm. Đây là điểm bắt đầu trước khi xem chi tiết.
- **Nguồn:** M1 cho các số tổng, M4 và M3 để đếm seller và brand khác nhau. Bộ lọc: khoảng ngày.
- **Cách tính:** đọc M1 từ đầu kỳ trước tới cuối kỳ này, đánh dấu từng ngày là "kỳ này" hoặc "kỳ trước", rồi tính:
  - Doanh thu, số bán = tổng `revenue_est`, `units_sold_est` của các ngày kỳ này.
  - Doanh thu / ngày và số bán / ngày = tổng / số ngày tin cậy. % thay đổi = (TB / ngày kỳ này) ÷ (TB / ngày kỳ trước) − 1.
  - Giá bán bình quân = doanh thu / số bán.
  - Sản phẩm có bán / ngày = trung bình `products_selling` của các ngày.
  - % giảm giá TB = `sum(discount_pct_sum) / sum(products_observed)`: trung bình % giảm của mọi sản phẩm-ngày, kể cả sản phẩm không giảm.
  - Rating TB = `sum(rating_sum) / sum(rated_products)`.
  - Seller / brand hoạt động = số khóa seller (M4) / brand (M3) khác nhau xuất hiện trong kỳ, bỏ nhóm "Không rõ".
- **Trình bày:**
  - 8 thẻ chia 2 hàng. Chỉ doanh thu / ngày và số bán / ngày có mũi tên % so với kỳ trước.
  - Tooltip ⓘ của từng thẻ chứa số phụ: số ngày tin cậy, tổng số bán, % sản phẩm-ngày có giảm giá, số lượt đánh giá mới, số lần đổi giá, số lần nghi giảm giá ảo.

#### [1.2] Xu hướng doanh thu theo ngày

- **Tác dụng:** xem doanh thu đi lên hay đi xuống, ngày nào bất thường, ngày sale nhảy vọt bao nhiêu. Panel dưới cho thấy % giảm giá có tăng theo ngày sale không.
- **Nguồn:** M1, mỗi dòng 1 ngày. Bộ lọc: khoảng ngày.
- **Cách tính:**
  - Doanh thu của từng ngày là `revenue_est`.
  - TB trượt 7 ngày = `avg(revenue_est)` của ngày đó và 6 dòng trước nó (window function). Window chỉ chạy trên các ngày trong khoảng đang lọc, nên 6 ngày đầu khoảng là trung bình của ít hơn 7 ngày.
  - % giảm TB = `discount_pct_sum / products_observed`.
- **Trình bày:** 2 panel chung trục ngày, thay cho biểu đồ 2 trục Y.
  - Panel trên: cột doanh thu và đường TB trượt 7 ngày.
  - Panel dưới: đường % giảm TB.
  - Ngày sale tô nền vàng, kèm nhãn tên sự kiện.
  - Đơn vị tiền (tr / tỷ / nghìn tỷ ₫) tự đổi theo độ lớn. Rê chuột để xem thứ, số bán và số sản phẩm có bán.

#### [1.3] Doanh thu theo danh mục cấp 1, thị phần và tăng trưởng

- **Tác dụng:** cho biết danh mục nào đóng góp nhiều doanh thu nhất và danh mục nào đang tăng nhanh hay đang giảm. Một danh mục lớn mà tăng chậm hơn các danh mục khác đang mất thị phần.
- **Nguồn:** M2 gộp lên cấp 1, M1 để đếm số ngày tin cậy của hai kỳ. Bộ lọc: khoảng ngày.
- **Cách tính:** đọc M2 từ đầu kỳ trước tới cuối kỳ này, `GROUP BY category_level_1`.
  - Thị phần = doanh thu kỳ này của danh mục / tổng doanh thu kỳ này của mọi danh mục.
  - Tăng trưởng = (doanh thu kỳ này / số ngày kỳ này) ÷ (doanh thu kỳ trước / số ngày kỳ trước) − 1.
  - Kèm theo: số bán, giá bán bình quân, số sản phẩm TB / ngày.
- **Trình bày:** 2 cột bar ngang dùng chung danh sách danh mục, xếp theo doanh thu.
  - Cột trái: doanh thu, nhãn là % thị phần.
  - Cột phải: % tăng trưởng. Xanh = tăng, đỏ = giảm, xám = trong khoảng ±1%.

#### [1.4] Cơ cấu doanh thu danh mục cấp 1 > cấp 2

- **Tác dụng:** cho thấy trong mỗi danh mục lớn, nhóm hàng con nào tạo ra doanh thu. Diện tích ô tỉ lệ với doanh thu.
- **Nguồn:** M2. Bộ lọc: khoảng ngày.
- **Cách tính:**
  - `GROUP BY category_level_1, category_level_2` trên các ngày trong kỳ, bỏ nhóm có doanh thu 0.
  - Danh mục không có cấp 2 hiện là "(không phân cấp)".
  - App cộng các dòng cấp 2 lại để ra ô cấp 1.
- **Trình bày:** treemap 3 tầng: Toàn sàn > cấp 1 > cấp 2.
  - Nhãn ô = tên và % doanh thu toàn sàn.
  - Ô càng chiếm tỉ trọng lớn thì càng xanh đậm. Màu lấy theo căn bậc hai của tỉ trọng để các ô nhỏ vẫn phân biệt được.
  - Bấm một ô cấp 1 để phóng to, bấm thanh tiêu đề phía trên để quay lại.

#### [1.5] Hiệu ứng ngày sale

- **Tác dụng:** đo ngày sale bán gấp bao nhiêu lần ngày thường. Ngày ngay sau sale được tách riêng: crawler chạy khoảng 4,2 giờ một lần, nên một phần số bán của ngày sale bị ghi sang hôm sau.
- **Nguồn:** M1. Bộ lọc: khoảng ngày.
- **Cách tính:**
  - Mỗi ngày được gắn một loại: "Ngày sale <tên>" (có `sale_event`), "Ngày sau sale" (hôm trước là ngày sale) hoặc "Ngày thường".
  - Theo từng loại ngày, tính:
    - doanh thu TB / ngày ;
    - bán TB / sản phẩm / ngày ;
    - % sản phẩm có bán ;
    - % giảm TB ;
    - % sản phẩm giảm ≥ 30%.
  - Uplift = chỉ số của loại ngày đó ÷ chỉ số của ngày thường.
- **Trình bày:** 2 bar ngang cạnh nhau: doanh thu / ngày và bán TB / sản phẩm / ngày.
  - Nhãn "×3,35" là uplift.
  - Ngày sale màu xanh, ngày sau sale xanh nhạt, ngày thường xám (mốc so sánh).
  - Loại ngày nào không có trong khoảng đang chọn thì không hiện.

#### [1.6] Mùa vụ theo thứ trong tuần

- **Tác dụng:** cho biết khách mua nhiều hơn vào thứ mấy trong tuần, sau khi đã bỏ ảnh hưởng của ngày sale.
- **Nguồn:** M1. Bộ lọc: khoảng ngày.
- **Cách tính:**
  - Bỏ ngày sale và ngày ngay sau sale, rồi `GROUP BY day_of_week`.
  - Tính doanh thu TB / ngày của từng thứ.
  - Chênh lệch = TB của thứ đó ÷ trung bình của 7 thứ − 1.
- **Trình bày:** cột từ Thứ Hai tới Chủ Nhật, nhãn là % chênh lệch so với TB tuần.
  - Xanh = cao hơn TB quá 2%, đỏ = thấp hơn quá 2%, xám = chênh không đáng kể.
  - Với khoảng 14 ngày, mỗi thứ chỉ có 1–2 ngày (rê chuột để xem số ngày). Chọn "Toàn kỳ" để kết luận chắc hơn.

### Dashboard 2: Danh mục & thương hiệu (`02_category_brand.sql`)

Bộ lọc: khoảng ngày và danh mục cấp 1. Có 2 ngoại lệ: [2.4] luôn hiện mọi danh mục cấp 1 ; [2.7] và [2.8] tính trên toàn kỳ đã nạp, không theo khoảng ngày.

#### [2.1] Bảng hiệu suất danh mục

- **Tác dụng:** so sánh các danh mục trên mọi chỉ số cùng lúc. Đây cũng là điểm vào để drill-down: chọn một danh mục cấp 1 thì cả trang này và các trang khác chuyển sang lọc theo danh mục đó.
- **Nguồn:** M2, M1 để đếm số ngày tin cậy. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:** khi chưa chọn danh mục, mỗi dòng là một danh mục cấp 1. Khi đã chọn, mỗi dòng là một danh mục cấp 2 của danh mục đó. Các cột:
  - doanh thu, thị phần (trong phạm vi bảng), tăng trưởng doanh thu / ngày so với kỳ trước ;
  - số bán, số sản phẩm TB / ngày, % sản phẩm có bán ;
  - giá niêm yết TB (`sell_price_sum / priced_products`, không trọng số), % giảm TB, rating TB ;
  - số lần đổi giá, số lần nghi giảm giá ảo.
- **Trình bày:** bảng có thanh tỉ lệ ở cột thị phần, cột tăng trưởng tô xanh / đỏ. Chọn ô đầu dòng để drill-down, bấm nút "← Về tất cả danh mục" để quay lại.

#### [2.2] Xu hướng doanh thu của 5 danh mục lớn nhất

- **Tác dụng:** cho thấy danh mục nào kéo tăng trưởng theo thời gian, và cơ cấu doanh thu có thay đổi quanh ngày sale không.
- **Nguồn:** M2. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:**
  - Tìm 5 danh mục có doanh thu cao nhất trong kỳ: cấp 1, hoặc cấp 2 khi đã drill-down.
  - Các danh mục còn lại gộp thành "khác".
  - Cộng `revenue_est` theo ngày × danh mục.
- **Trình bày:** stacked area. Chiều cao tổng = doanh thu của cả phạm vi đang xem.
  - Mỗi danh mục cấp 1 giữ một màu cố định trên mọi trang, theo thứ hạng doanh thu toàn kỳ. Khi đã drill-down xuống cấp 2, màu được gán theo thứ hạng trong biểu đồ. "khác" màu xám.
  - Ngày sale tô nền vàng. Rê chuột để xem giá trị của từng lớp trong ngày.

#### [2.3] Top 15 thương hiệu theo doanh thu

- **Tác dụng:** cho biết brand nào dẫn đầu toàn sàn hoặc một danh mục. Cột giá, % giảm, rating và tăng trưởng cho biết brand đó dẫn đầu nhờ giá rẻ, giảm giá sâu hay chất lượng.
- **Nguồn:** M3, M1 để đếm ngày. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:**
  - Bỏ "Không rõ thương hiệu", `GROUP BY brand_key`. Khi chưa lọc, doanh thu của một brand được cộng qua mọi danh mục.
  - Thị phần = doanh thu của brand / tổng doanh thu mọi brand đã biết trong phạm vi. Lấy 15 brand đầu.
  - Giá bán bình quân, % giảm TB, rating TB và tăng trưởng / ngày tính như [1.1].
- **Trình bày:**
  - Bar ngang thị phần, nhãn là %, tên brand kèm xuất xứ.
  - Bảng phụ bên dưới: giá, % giảm, rating, tăng trưởng (tô xanh / đỏ).

#### [2.4] Mức độ tập trung thị trường theo danh mục

- **Tác dụng:** cho biết trong mỗi danh mục, thị trường có bị vài brand chi phối không. HHI cao nghĩa là brand mới khó chen vào.
- **Nguồn:** M3. Bộ lọc: khoảng ngày. Biểu đồ không lọc danh mục, luôn so mọi danh mục cấp 1.
- **Cách tính:**
  - Tính doanh thu từng brand trong từng danh mục cấp 1, bỏ brand unknown và brand có doanh thu 0.
  - Đổi doanh thu thành thị phần % trong danh mục. HHI = tổng bình phương các thị phần.
  - Phân loại theo ngưỡng của Bộ Tư pháp Mỹ (2023): < 1.000 phân tán, 1.000–1.800 tập trung vừa, > 1.800 tập trung cao.
  - Kèm thị phần cộng dồn của top 1, top 5 và top 10 brand.
- **Trình bày:**
  - Bar ngang HHI, tô màu status theo kết luận: xanh lá = phân tán, vàng = tập trung vừa, đỏ = tập trung cao. Nhãn ghi cả số và kết luận.
  - Có vạch ngưỡng 1.000 và 1.800. Vạch chỉ được vẽ khi HHI lớn nhất đã gần tới ngưỡng.
  - Bảng bên dưới: số brand, thị phần top 1 / 5 / 10.

#### [2.5] Ma trận thương hiệu

- **Tác dụng:** đặt 30 brand lớn nhất lên mặt phẳng tăng trưởng × rating để chia 4 nhóm:
  - ngôi sao: tăng và rating cao ;
  - tăng nhưng rating thấp ;
  - rating tốt nhưng chững lại ;
  - cần chú ý.
- **Nguồn:** M3, M1 để đếm ngày. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:**
  - Lấy 30 brand có doanh thu kỳ này cao nhất, bỏ unknown.
  - X = tăng trưởng doanh thu / ngày so với kỳ trước. Y = rating TB của sản phẩm thuộc brand.
  - Góc phần tư dựa trên hai đường X = 0 và Y = rating TB của 30 brand.
  - Kỳ trước không có ngày tin cậy nào thì truy vấn trả về rỗng.
- **Trình bày:**
  - Bubble một màu, diện tích bóng theo doanh thu. Tên ghi trực tiếp cho 8 brand lớn nhất.
  - Có 2 đường chia và tên của 4 góc.
  - Brand không tính được tăng trưởng (doanh thu kỳ trước = 0) bị bỏ. Nếu không còn brand nào, card nhắc chọn khoảng ngày muộn hơn.

#### [2.6] Cơ cấu phân khúc giá theo danh mục

- **Tác dụng:** cho biết mỗi danh mục kiếm tiền chủ yếu ở phân khúc giá nào: hàng rẻ bán nhiều hay hàng đắt.
- **Nguồn:** M5. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:**
  - Loader đã xếp mỗi sản phẩm-ngày vào một phân khúc theo giá bán đóng cửa: < 100k, 100k–300k, 300k–1tr, 1tr–3tr, 3tr–10tr, ≥ 10tr. Nhóm "không rõ giá" bị bỏ.
  - Tỉ trọng = doanh thu của phân khúc / doanh thu của danh mục.
- **Trình bày:**
  - Thanh ngang 100%, mỗi hàng một danh mục (xếp theo doanh thu), mỗi đoạn một phân khúc.
  - Màu đi từ xanh nhạt tới xanh đậm khi giá tăng.
  - Nhãn % chỉ ghi khi đoạn chiếm ≥ 7%.

#### [2.7] Phân loại ABC sản phẩm

- **Tác dụng:** kiểm tra quy luật Pareto: bao nhiêu % sản phẩm tạo ra 80% doanh thu, và bao nhiêu % sản phẩm không bán được gì.
- **Nguồn:** M6, tính trên toàn kỳ đã nạp, không theo khoảng ngày. Bộ lọc: danh mục.
- **Cách tính:**
  - Đếm sản phẩm và cộng doanh thu theo `abc_class`, chia cho tổng để ra % sản phẩm và % doanh thu.
  - `abc_class` chỉ được xếp một lần trên **toàn sàn**. Vì vậy khi lọc một danh mục, "A" vẫn nghĩa là sản phẩm thuộc nhóm tạo 80% doanh thu của toàn sàn.
  - Bảng dữ liệu có thêm giá hiện tại TB và sao review TB của mỗi nhóm.
- **Trình bày:** cột nhóm cho từng nhóm A / B / C / N: xanh = % số sản phẩm, cam = % doanh thu.

#### [2.8] Đường cong Pareto

- **Tác dụng:** đọc trực tiếp "x% sản phẩm bán chạy nhất tạo ra y% doanh thu" trong phạm vi đang lọc.
- **Nguồn:** M6, toàn kỳ. Bộ lọc: danh mục.
- **Cách tính:**
  - Xếp sản phẩm trong phạm vi lọc theo doanh thu giảm dần, rồi tính doanh thu lũy kế.
  - Chia sản phẩm thành 100 bách phân vị theo thứ hạng (`ceil(hạng / n × 100)`). Mỗi bách phân vị lấy % doanh thu lũy kế ở sản phẩm cuối nhóm.
  - Khác với [2.7], thứ hạng ở đây được tính lại trong danh mục đang lọc.
- **Trình bày:** đường có tô nền, có chấm và chú thích tại mốc 10% và 20% sản phẩm, có vạch ngang ở mức 80% doanh thu.

### Dashboard 3: Người bán (`03_seller.sql`)

Bộ lọc: khoảng ngày và danh mục cấp 1.
- Danh mục ở trang này là **danh mục chính của seller**, tức danh mục chiếm nhiều sản phẩm-ngày nhất của seller trên cả kỳ. Nó không phải danh mục của từng sản phẩm.
- Seller được nhận diện theo tên. Nhóm "Không rõ người bán" bị loại.
- Shop "official" là seller có tên chứa "official" / "chính hãng", hoặc là "tiki trading".

#### [3.1] Thẻ KPI seller

- **Tác dụng:** cho biết quy mô và cơ cấu nhóm người bán: có bao nhiêu seller, shop official chiếm bao nhiêu, doanh thu của một seller điển hình là bao nhiêu.
- **Nguồn:** M4. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:** đầu tiên gộp mỗi seller thành một dòng gồm tổng doanh thu, rating và số lượt đánh giá ở ngày cuối kỳ, số sản phẩm TB / ngày. Sau đó:
  - seller hoạt động = mọi seller có sản phẩm được crawl trong kỳ ; seller có doanh thu = seller có doanh thu > 0 ;
  - % official = số shop official / tổng số seller ; thị phần official = doanh thu shop official / tổng doanh thu các seller ;
  - doanh thu trung vị / seller (tính cả seller không bán được) và doanh thu TB / seller có bán ;
  - rating seller TB = trung bình rating ngày cuối kỳ của các seller đã có đánh giá.
- **Trình bày:** 8 thẻ, tooltip giải thích cách tính.

#### [3.2] Official so với seller thường

- **Tác dụng:** trả lời shop official có bán tốt hơn, bán đắt hơn, giảm giá ít hơn, được đánh giá cao hơn và ít giảm giá ảo hơn shop thường không.
- **Nguồn:** M4, M1 để đếm ngày. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:** `GROUP BY is_official`. Với mỗi nhóm, tính:
  - số seller, thị phần doanh thu ;
  - doanh thu / seller / ngày = doanh thu / số seller / số ngày tin cậy ;
  - sản phẩm / seller, giá bán bình quân, bán TB / sản phẩm / ngày, % giảm TB ;
  - rating sản phẩm TB ; rating seller TB = trung bình trên các dòng seller-ngày đã có đánh giá ;
  - số lần nghi giảm giá ảo trên 1.000 lần đổi giá.
- **Trình bày:**
  - Bảng xoay: mỗi hàng là một chỉ số, mỗi cột là một nhóm, để so sánh trực tiếp.
  - Bên dưới là bar doanh thu / seller / ngày: official màu xanh, seller thường màu xám.

#### [3.5] Độ tập trung doanh thu theo seller

- **Tác dụng:** cho biết doanh thu có dồn vào vài seller lớn không. So Gini giữa các kỳ hoặc giữa các danh mục để thấy thị trường đang tập trung hơn hay phân tán hơn.
- **Nguồn:** M4. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:**
  - Tính tổng doanh thu mỗi seller (kể cả seller có doanh thu 0) rồi xếp giảm dần.
  - Top x% = `ceil(n × x%)` seller đầu tiên. Tỉ trọng = doanh thu của nhóm đó / tổng doanh thu.
  - Gini = (2 × Σ i·xᵢ / Σ xᵢ − (n + 1)) / n, với i là thứ hạng xếp tăng dần.
- **Trình bày:** 2 thẻ (Gini, số seller được xét) và biểu đồ cột: top 1 / 5 / 10 / 20 / 50% seller → % doanh thu.

#### [3.3] Bảng xếp hạng top 20 seller

- **Tác dụng:** liệt kê các seller dẫn đầu kèm tín hiệu chất lượng (rating, % review tiêu cực, giảm giá ảo), để thấy seller lớn nào đang có vấn đề.
- **Nguồn:** M4 cho doanh số, M6 cho chỉ số review, M1 để đếm ngày. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:** lấy 20 seller có doanh thu kỳ này cao nhất.
  - Thị phần = doanh thu seller / tổng doanh thu **toàn sàn** trong kỳ. Mẫu số không lọc theo danh mục.
  - Tăng trưởng / ngày so với kỳ trước.
  - Rating seller và số lượt đánh giá lấy ở ngày cuối kỳ. Lượt đánh giá mới là tổng trong kỳ.
  - Cột review (sao TB, % review 1–2 sao) cộng từ M6 theo seller. Các cột này tính trên **toàn kỳ** và theo seller của sản phẩm ở ngày quan sát cuối.
- **Trình bày:** bảng. Dấu ✓ = official, cột thị phần có thanh tỉ lệ, cột tăng trưởng tô xanh / đỏ, % review tiêu cực ≥ 15% tô đỏ.

#### [3.4] Diễn biến rating của 5 seller lớn nhất

- **Tác dụng:** cho biết uy tín của các seller lớn có ổn định không, và ngày sale có kéo theo nhiều đánh giá mới không.
- **Nguồn:** M4. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:** chọn 5 seller có doanh thu kỳ này cao nhất. Với mỗi ngày, lấy rating seller cuối ngày, số lượt đánh giá lũy kế, số lượt đánh giá mới và doanh thu.
- **Trình bày:** 2 panel.
  - Panel trên: đường rating của từng seller, chỉ vẽ những ngày seller đã có đánh giá. Rating không cộng được nên không gộp các seller.
  - Panel dưới: cột chồng số lượt đánh giá mới mỗi ngày.
  - Mỗi seller một màu, chú giải ở dưới. Ngày sale tô nền vàng.

#### [3.6] Seller cần theo dõi

- **Tác dụng:** danh sách cảnh báo sớm các seller có dấu hiệu giảm giá ảo, bị chê nhiều hoặc rating đang tụt.
- **Nguồn:** M4, M6 cho review. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:** mỗi dấu hiệu được 1 điểm, tối đa 3 điểm:
  - (a) có ≥ 3 lần nghi giảm giá ảo trong kỳ ;
  - (b) có ≥ 15% review 1–2 sao, với tối thiểu 30 review có sao. Review tính trên toàn kỳ, lấy từ M6 ;
  - (c) rating seller giảm ≥ 0,2 điểm giữa ngày đầu và ngày cuối kỳ mà seller có đánh giá.

  Chỉ giữ seller có điểm > 0, xếp theo điểm rồi theo doanh thu, tối đa 50 seller.
- **Trình bày:**
  - 3 thẻ đếm số seller ở từng mức 🔴 3 / 🟠 2 / 🟡 1. Số đếm chỉ tính trong tối đa 50 seller được trả về.
  - Bảng có cột "Dấu hiệu" ghi lý do. % review tiêu cực ≥ 15% tô đỏ, cột thay đổi rating tô xanh / đỏ.

### Dashboard 4: Giá & khuyến mãi (`04_pricing_promotion.sql`)

Bộ lọc: khoảng ngày, danh mục cấp 1, và ngày sale (chỉ cho [4.5]–[4.7]). Hai ngoại lệ:
- [4.1], [4.5] và [4.8] không lọc theo danh mục.
- [4.3]–[4.7] không theo khoảng ngày mà theo các ngày sale đã nạp.

#### [4.1] Số lần đổi giá theo ngày

- **Tác dụng:** theo dõi nhịp đổi giá của thị trường: ngày nào nhiều shop giảm giá, ngày nào tăng giá, và từ khi nào bắt đầu có dấu hiệu nâng giá gốc trước ngày sale.
- **Nguồn:** M1. Bộ lọc: khoảng ngày.
- **Cách tính:**
  - Với mỗi ngày: số lần giảm giá, số lần tăng giá, số lần chỉ đổi giá gốc (= tổng số lần đổi − tăng − giảm), số lần nghi giảm giá ảo, số lần đổi giá / 100 sản phẩm.
  - Các lần đổi giá giả do thiếu giá đã bị loại ở loader.
- **Trình bày:** 2 panel.
  - Panel trên: cột chồng. Giảm giá màu xanh, tăng giá màu đỏ, chỉ đổi giá gốc màu xám.
  - Panel dưới: đường số lần nghi giảm giá ảo, màu đỏ cảnh báo.
  - Ngày sale tô nền vàng.

#### [4.2] Giảm giá sâu hơn có bán nhiều hơn không?

- **Tác dụng:** kiểm tra mức giảm giá có đi kèm lượng bán cao hơn ở ngày thường không. Đây là tương quan, không phải nhân quả: sản phẩm vốn đã bán chạy có thể được giảm giá nhiều hơn.
- **Nguồn:** M5. Bộ lọc: khoảng ngày, danh mục.
- **Cách tính:**
  - Chỉ lấy các ngày không phải ngày sale, để hiệu ứng ngày sale không lẫn vào.
  - Chia theo mức giảm hiển thị: không giảm, < 10%, 10–20%, 20–30%, 30–40%, ≥ 40%.
  - Mỗi mức tính bán TB / sản phẩm / ngày, % sản phẩm có bán, % doanh thu.
  - Chỉ số = bán TB / sản phẩm của mức đó ÷ bán TB / sản phẩm của nhóm không giảm.
- **Trình bày:** 2 panel dùng chung trục mức giảm, màu từ xanh nhạt tới xanh đậm.
  - Panel trên: bán TB / sản phẩm / ngày, nhãn "×1,05" là so với nhóm không giảm.
  - Panel dưới: % doanh thu của ngày thường.

#### [4.8] Phân bố mức đổi giá

- **Tác dụng:** cho biết các lần đổi giá thường lớn cỡ nào, và các lần nghi giảm giá ảo rơi vào mức đổi giá nào.
- **Nguồn:** gold F3 `gold_fact_price_change` (từng lần đổi giá), vì mart không giữ từng lần. Bộ lọc: khoảng ngày, theo ngày đổi giá. Không lọc danh mục, không lọc ngày tin cậy.
- **Cách tính:**
  - Bỏ đổi giá giả (giá cũ hoặc giá mới = 0) và các lần giá không đổi.
  - Chia `change_pct` thành các khoảng 5%. Hai đầu được gộp thành ≤ −50% và ≥ +50%.
  - Mỗi khoảng đếm số lần giảm giá, số lần tăng giá, và số lần gold gắn cờ nghi giảm giá ảo.
- **Trình bày:** histogram đặt chồng nhau quanh vạch 0%. Xanh = giảm giá, đỏ = tăng giá. Hình thoi đỏ là số lần nghi giảm giá ảo trong khoảng đó.

#### [4.3] Bảng điểm các ngày sale

- **Tác dụng:** chấm điểm từng sự kiện sale: bao nhiêu sản phẩm tham gia, giảm thật bao nhiêu so với con số quảng cáo, bao nhiêu sản phẩm giảm giá ảo, ngày sale bán gấp bao nhiêu lần.
- **Nguồn:** M7. Bộ lọc: danh mục. Bảng lấy mọi ngày sale đã nạp, không theo khoảng ngày.
- **Cách tính:** mỗi dòng là một sự kiện, gồm:
  - số sản phẩm có mặt ; số sản phẩm tham gia (ghi giảm ≥ 10%) ; % tham gia ;
  - % giảm hiển thị TB và % giảm thật TB của các sản phẩm tham gia ;
  - trong số sản phẩm tham gia: % giảm thật ≥ 5%, % có giá thấp nhất 30 ngày ;
  - số và % sản phẩm giảm giá ảo ;
  - số bán ngày sale, số bán ngày sau, doanh thu ngày sale ;
  - uplift trung vị (dùng trung vị vì tỉ lệ này lệch phải) ; số ngày nền TB.

  Sự kiện 10.10 chỉ có khoảng 2 ngày nền (dữ liệu bắt đầu từ 07/10), nên giá tham chiếu kém tin cậy hơn 11.11.
- **Trình bày:** bảng, uplift ghi dạng "×3,7".

#### [4.4] Phân loại khuyến mãi trong ngày sale

- **Tác dụng:** cho biết trong mỗi ngày sale, bao nhiêu % sản phẩm giảm thật, giảm danh nghĩa, giảm giá ảo hoặc không giảm. Bảng uplift cho biết giảm giá ảo có thật sự giúp bán được nhiều hơn không.
- **Nguồn:** M7. Bộ lọc: danh mục.
- **Cách tính:** `GROUP BY sale_event, discount_verdict`. Mỗi nhóm có số sản phẩm, % trong sự kiện, % giảm hiển thị TB, % giảm thật TB, uplift trung vị, doanh thu.
- **Trình bày:**
  - Thanh ngang 100%, mỗi sự kiện một hàng. Màu status theo kết luận: giảm thật = xanh lá, giảm danh nghĩa = vàng, giảm giá ảo = đỏ, không giảm = xám.
  - Bảng bên dưới: uplift trung vị của từng kết luận ở từng sự kiện.

#### [4.5] % giảm hiển thị so với % giảm thật theo danh mục

- **Tác dụng:** cho biết danh mục nào "giảm trên giấy" nhiều nhất, tức con số % giảm quảng cáo cách xa mức giảm thật bao nhiêu.
- **Nguồn:** M7. Bộ lọc: ngày sale. Không lọc danh mục, luôn so mọi danh mục cấp 1.
- **Cách tính:**
  - Chỉ tính sản phẩm ghi giảm ≥ 10%, và chỉ giữ danh mục có ≥ 20 sản phẩm tham gia.
  - Mỗi danh mục: % giảm hiển thị TB, % giảm thật TB, chênh lệch (điểm %), % giảm giá ảo.
  - Xếp theo chênh lệch.
- **Trình bày:** biểu đồ dumbbell. Chấm xanh = % giảm thật, chấm cam = % giảm hiển thị, đoạn nối giữa hai chấm = phần chênh.

#### [4.6] Seller có nhiều sản phẩm giảm giá ảo nhất

- **Tác dụng:** chỉ ra seller nào có nhiều sản phẩm giảm giá ảo nhất trong một ngày sale, để kiểm tra hoặc cảnh báo.
- **Nguồn:** M7. Bộ lọc: ngày sale, danh mục.
- **Cách tính:**
  - Bỏ "Không rõ người bán".
  - Mỗi seller: số sản phẩm có mặt ngày sale ; số và % sản phẩm giảm giá ảo ; % nâng giá gốc TB và % giảm thật TB của các sản phẩm đó ; doanh thu từ các sản phẩm đó.
  - Lấy 15 seller có nhiều sản phẩm giảm giá ảo nhất. Nếu bằng nhau thì xếp theo doanh thu.
- **Trình bày:** bar ngang số sản phẩm giảm giá ảo, nhãn "n SP · tỉ lệ", dấu ✓ = official.

#### [4.7] Danh sách sản phẩm giảm giá ảo

- **Tác dụng:** đưa ra bằng chứng cụ thể cho từng trường hợp: giá gốc bị nâng từ bao nhiêu lên bao nhiêu, giá bán thật có giảm không. Bảng cũng là điểm vào để xem lịch sử giá chi tiết của sản phẩm.
- **Nguồn:** M7, tên sản phẩm ghép từ M6. Bộ lọc: ngày sale, danh mục.
- **Cách tính:** lấy các sản phẩm có kết luận "giảm giá ảo" trong sự kiện đã chọn, gồm:
  - giá gốc thường (trung vị 30 ngày) → giá gốc ngày sale, % nâng giá gốc ;
  - % giảm hiển thị ;
  - giá bán thường → giá bán ngày sale, % giảm thật ;
  - số bán và doanh thu ngày sale.

  Lấy 100 sản phẩm có doanh thu ngày sale cao nhất.
- **Trình bày:** bảng. Chọn một dòng để chuyển sang trang Sản phẩm và mở đúng sản phẩm đó (drill-through).

### Dashboard 5: Đánh giá khách hàng (`05_review.sql`)

Bộ lọc: danh mục cấp 1.
- Trang này không lọc theo ngày. Ngày viết review chỉ là ước tính ("4 năm trước") và review trải dài nhiều năm, trong khi dữ liệu bán chỉ có từ 10/2026.
- Mọi tỉ lệ đều tính trên review có sao.

#### [5.1] Thẻ KPI review

- **Tác dụng:** cho biết mức hài lòng tổng thể của khách.
- **Nguồn:** M8, mọi tháng, kể cả review không xác định được ngày viết. Bộ lọc: danh mục.
- **Cách tính:**
  - số review ;
  - sao TB = tổng số sao / số review có sao ;
  - % tích cực (4–5 sao) và % tiêu cực (1–2 sao) ;
  - net sentiment = % 5 sao − % 1–3 sao ;
  - % review có ảnh và % review có bình luận, tính trên mọi review ;
  - số từ TB / bình luận, tính trên review có bình luận.
- **Trình bày:** 8 thẻ.

#### [5.2] Phân bố số sao

- **Tác dụng:** cho biết khách nhìn chung hài lòng hay chia thành hai phe khen và chê.
- **Nguồn:** M8. Bộ lọc: danh mục.
- **Cách tính:** cộng `star_1` … `star_5`, rồi xoay thành 5 dòng. % = số review của mức sao đó / tổng số review có sao.
- **Trình bày:** bar ngang từ 5 sao xuống 1 sao: 5 sao xanh đậm, 4 sao xanh nhạt, 3 sao xám, 2 sao đỏ nhạt, 1 sao đỏ đậm. Nhãn "% · số review".

#### [5.5] Thâm niên người review và mức độ khó tính

- **Tác dụng:** trả lời người dùng lâu năm có chấm điểm khắt khe hơn người mới không.
- **Nguồn:** M8. Bộ lọc: danh mục.
- **Cách tính:** `GROUP BY tenure_band`, với thâm niên tài khoản chia thành dưới 1 năm, 1–3 năm, 3–5 năm, trên 5 năm, không rõ. Mỗi nhóm có số review và tỉ trọng, sao TB, % tiêu cực, % có ảnh, số từ TB.
- **Trình bày:** 2 bar ngang, màu từ xanh nhạt tới xanh đậm theo thâm niên.
  - Bar trái: sao TB. Trục bắt đầu gần giá trị nhỏ nhất để thấy rõ chênh lệch nhỏ, có ghi chú dưới biểu đồ.
  - Bar phải: % tiêu cực.

#### [5.3] Số review và sao TB theo tháng

- **Tác dụng:** cho biết lượng review tăng hay giảm theo thời gian, và cảm nhận về chất lượng có thay đổi không.
- **Nguồn:** M8, bỏ tháng không xác định (1970-01). Bộ lọc: danh mục.
- **Cách tính:** `GROUP BY review_month`: số review, sao TB, % tiêu cực, % có ảnh.
- **Trình bày:** 2 panel theo tháng.
  - Panel trên: cột số review.
  - Panel dưới: đường sao TB.
  - Rê chuột vào cột để xem % review có ảnh và % tiêu cực.

#### [5.4] Chất lượng review theo danh mục

- **Tác dụng:** cho biết danh mục nào bị chê nhiều nhất. Bấm vào một danh mục để xem danh mục con nào gây ra.
- **Nguồn:** M8. Bộ lọc: danh mục.
- **Cách tính:**
  - Khi chưa chọn danh mục, mỗi dòng là một danh mục cấp 1. Khi đã chọn, mỗi dòng là một danh mục cấp 2 của nó.
  - Chỉ giữ nhóm có ≥ 100 review có sao, để tỉ lệ không dao động vì mẫu nhỏ.
  - Tính % tiêu cực, sao TB, % tích cực, % có ảnh, số từ TB, rồi xếp theo % tiêu cực.
- **Trình bày:** bar ngang màu đỏ, nhãn "% tiêu cực · sao TB". Bấm một thanh để drill-down (đặt bộ lọc danh mục), bấm nút "← Về tất cả danh mục" để quay lại.

#### [5.6] Thương hiệu bị đánh giá tiêu cực nhiều nhất

- **Tác dụng:** tìm brand thật sự bị chê nhiều, không bị đánh lừa bởi brand có ít review mà tỉ lệ cao do ngẫu nhiên.
- **Nguồn:** M6, review cộng theo brand trên toàn kỳ. Bộ lọc: danh mục.
- **Cách tính:**
  - Cộng số review có sao, số review tiêu cực, tổng số sao và doanh thu theo brand, bỏ brand unknown. Chỉ giữ brand có ≥ 100 review có sao.
  - Cận dưới Wilson 95% của tỉ lệ tiêu cực p, với n review: (p + z²/2n − z·√(p(1−p)/n + z²/4n²)) / (1 + z²/n), z = 1,96.
  - Lấy 15 brand có cận dưới cao nhất.
- **Trình bày:** thanh đỏ = cận dưới Wilson, vạch dọc = % tiêu cực thực tế. Hai giá trị cách xa nhau nghĩa là brand có ít review.

#### [5.7] Rating có đi kèm bán chạy không?

- **Tác dụng:** cho biết sản phẩm được chấm sao cao có bán chạy hơn không. Đây là tương quan, tính trên toàn kỳ.
- **Nguồn:** M6. Bộ lọc: danh mục.
- **Cách tính:**
  - Xếp mỗi sản phẩm vào một nhóm theo sao TB từ review: chưa có review, < 3, 3–3,5, 3,5–4, 4–4,5, ≥ 4,5.
  - Mỗi nhóm: số sản phẩm, bán TB / ngày (trung bình `avg_daily_units` của các sản phẩm), doanh thu TB / sản phẩm, % sản phẩm thuộc nhóm A, giá TB.
- **Trình bày:** biểu đồ cột và bảng bên cạnh. Nhóm chưa có review màu xám, các nhóm sao đi từ xanh nhạt tới xanh đậm.

### Dashboard 6: Sản phẩm (`06_product.sql`)

Bộ lọc áp dụng khác nhau theo phần:
- Danh mục cấp 1 lọc 3 danh sách.
- Sản phẩm đang chọn dùng cho phần chi tiết. Mặc định là sản phẩm có doanh thu cao nhất.
- Khoảng ngày chỉ áp dụng cho [6.5].

M6 tính trên toàn kỳ đã nạp. "7 ngày" được tính tới ngày tin cậy cuối cùng (`as_of_date`).

#### [6.1] Top 20 sản phẩm theo doanh thu

- **Tác dụng:** cho biết sản phẩm nào mang lại nhiều doanh thu nhất và chúng có đang tăng không.
- **Nguồn:** M6. Bộ lọc: danh mục.
- **Cách tính:**
  - Lấy 20 sản phẩm có `revenue_total` cao nhất.
  - Cột hạng, % doanh thu lũy kế và nhóm ABC là thứ hạng **toàn sàn**, không tính lại theo danh mục.
  - Cột xu hướng (`sales_trend`) so số bán 7 ngày gần nhất với 7 ngày trước đó: tăng (≥ +20%), giảm (≤ −20%), ổn định, mới có doanh số, không bán.
- **Trình bày:** bảng. Chọn một dòng để mở chi tiết sản phẩm ở phía dưới.

#### [6.2] Sản phẩm đang lên

- **Tác dụng:** phát hiện sớm sản phẩm đang tăng tốc.
- **Nguồn:** M6. Bộ lọc: danh mục.
- **Cách tính:**
  - Chỉ xét sản phẩm bán được ≥ 50 trong 7 ngày trước đó, để % tăng không bị phóng đại từ nền quá nhỏ.
  - Xếp theo tăng trưởng 7 ngày = (bán 7 ngày gần nhất − bán 7 ngày trước) / bán 7 ngày trước.
  - Nếu 7 ngày gần nhất chứa ngày sale thì gần như mọi sản phẩm đều "tăng".
- **Trình bày:** bảng, cột tăng trưởng tô xanh / đỏ, cột "Tính tới" là `as_of_date`. Chọn một dòng để mở chi tiết.

#### [6.3] Sản phẩm doanh thu cao nhưng bị chê

- **Tác dụng:** danh sách ưu tiên xử lý chất lượng: sản phẩm quan trọng về doanh thu nhưng khách không hài lòng.
- **Nguồn:** M6. Bộ lọc: danh mục.
- **Cách tính:**
  - Điều kiện: thuộc nhóm A, có ≥ 10 review có sao, và có sao TB < 4 hoặc ≥ 15% review 1–2 sao.
  - Tối đa 50 sản phẩm, xếp theo doanh thu.
  - Kèm rating hiển thị trên Tiki để so với sao tính từ review.
- **Trình bày:** bảng, % tiêu cực ≥ 15% tô đỏ. Chọn một dòng để mở chi tiết.

#### [6.4] Thẻ thông tin sản phẩm

- **Tác dụng:** hồ sơ một trang của sản phẩm đang chọn: đang bán giá nào, đã bán bao nhiêu, đang lên hay xuống, khách đánh giá thế nào, có dấu hiệu giảm giá ảo không.
- **Nguồn:** M6, đúng 1 dòng (`SELECT *`). Bộ lọc: sản phẩm.
- **Cách tính:** truy vấn không tính thêm gì, mọi số đã có sẵn trong M6:
  - thuộc tính lấy từ phiên bản mới nhất ;
  - brand, seller và danh mục lấy ở ngày quan sát cuối ;
  - giá hiện tại = giá của ngày cuối cùng có giá > 0.
- **Trình bày:**
  - Phần đầu: tiêu đề, đường dẫn danh mục, brand, seller, xuất xứ, bảo hành, khoảng ngày quan sát. Các nhãn: Official, nhóm ABC, xu hướng, phân khúc giá, số phiên bản thuộc tính.
  - 8 thẻ:
    - giá hiện tại, kèm −x% so với giá gốc ; tooltip có giá thấp nhất / cao nhất / TB ;
    - doanh thu toàn kỳ, kèm thứ hạng ;
    - số bán toàn kỳ, kèm số bán / ngày ; tooltip có số "đã bán" lũy kế trên Tiki ;
    - bán 7 ngày gần nhất, kèm % so với 7 ngày trước ;
    - rating trên Tiki, kèm số lượt đánh giá ;
    - sao TB từ review, kèm số review có sao ;
    - % review tiêu cực, có cảnh báo khi ≥ 15% ;
    - số lần đổi giá, kèm số lần nghi giảm giá ảo.
  - Bar phân bố 5 → 1 sao của review sản phẩm.
  - Mục "Xem bảng dữ liệu" hiện mọi cột của dòng M6 theo chiều dọc.

#### [6.5] Diễn biến giá và lượng bán theo ngày

- **Tác dụng:** xem giá của sản phẩm thay đổi thế nào, giá gốc có bị nâng trước ngày sale không, và lượng bán phản ứng ra sao.
- **Nguồn:** gold F2 `gold_fact_product_daily`, ghép `gold_dim_date`. Bộ lọc: sản phẩm, khoảng ngày. Không lọc ngày tin cậy.
  - Biểu đồ đọc thẳng gold vì cần từng ngày của một sản phẩm.
  - Truy vấn lọc theo `product_id`, là cột đầu của khóa sắp xếp, nên chạy dưới 0,1 giây.
- **Cách tính:** mỗi ngày lấy:
  - giá bán mở / đóng / thấp nhất / cao nhất, giá gốc đóng cửa, % giảm TB ;
  - số bán và doanh thu ước tính ;
  - "đã bán" và rating cuối ngày ;
  - số lần đổi giá, số lần crawl.
- **Trình bày:** 2 panel.
  - Panel trên: vùng nhạt = giá thấp nhất – cao nhất trong ngày, đường xám = giá gốc, đường xanh = giá bán đóng cửa, chấm = ngày có đổi giá.
  - Panel dưới: cột số bán.
  - Ngày sale tô nền vàng.

#### [6.6] Nhật ký đổi giá

- **Tác dụng:** liệt kê mọi lần đổi giá của sản phẩm, để kiểm chứng kết luận giảm giá ảo: giá gốc tăng cùng lúc với % giảm tăng.
- **Nguồn:** gold F3. Bộ lọc: sản phẩm. Bảng lấy toàn bộ lịch sử, không theo khoảng ngày.
- **Cách tính:** bỏ đổi giá giả (giá = 0). Mỗi dòng gồm:
  - thời điểm và chiều đổi giá ;
  - giá cũ → giá mới, % đổi ;
  - giá gốc cũ → giá gốc mới, % giảm mới ;
  - số giờ giá cũ được giữ ;
  - cờ nghi giảm giá ảo.
- **Trình bày:** bảng. Dòng nghi giảm giá ảo tô nền đỏ và có cờ "⚠ giảm giá ảo". Cột % đổi tô xanh / đỏ.

#### [6.7] Lịch sử thuộc tính (SCD Type 2)

- **Tác dụng:** cho biết sản phẩm có đổi tiêu đề, xuất xứ hay bảo hành không và đổi khi nào. Ví dụ: đổi tên để lên kết quả tìm kiếm.
- **Nguồn:** gold `gold_dim_product`. Bộ lọc: sản phẩm.
- **Cách tính:** mỗi phiên bản gồm tiêu đề, xuất xứ, bảo hành, thời điểm bắt đầu và kết thúc hiệu lực, có phải bản hiện tại không, số ngày hiệu lực. Bản hiện tại được tính tới thời điểm chạy truy vấn.
- **Trình bày:** timeline, mỗi phiên bản một thanh trải theo thời gian hiệu lực (bản hiện tại màu xanh, bản cũ màu xám), kèm bảng chi tiết.

#### [6.8] Sản phẩm trong các ngày sale

- **Tác dụng:** cho biết ở mỗi ngày sale, sản phẩm này có giảm thật không, giá có thấp nhất 30 ngày không, và bán gấp bao nhiêu lần ngày thường.
- **Nguồn:** M7, mỗi sự kiện một dòng. Bộ lọc: sản phẩm.
- **Cách tính:** đọc nguyên các cột của M7:
  - kết luận ;
  - giá bán thường (trung vị 30 ngày), giá thấp nhất 30 ngày, giá ngày sale ;
  - % giảm hiển thị, % giảm thật ;
  - giá gốc thường, giá gốc ngày sale, % nâng giá gốc, có phải giá thấp nhất 30 ngày không ;
  - bán TB ngày thường, bán ngày sale, bán ngày sau, uplift.
- **Trình bày:** bảng. Cột kết luận tô màu: "giảm thật" xanh lá, "giảm giá ảo" đỏ.

## Cấu trúc

```
app.py                 điều hướng 6 trang, kiểm tra kết nối, sidebar
core/db.py             client ClickHouse (cấu hình từ env / secrets / .env)
core/sql_catalog.py    parse 6 file SQL -> catalog 43 truy vấn
core/queries.py        chạy truy vấn (cache 10 phút) + truy vấn meta cho bộ lọc
core/filters.py        bộ lọc chung, drill-down / drill-through
core/theme.py          token màu light/dark, template Plotly
core/charts.py         builder biểu đồ (2 panel, bar ngang, đánh dấu ngày sale, ...)
core/components.py     card, thẻ KPI, bảng định dạng, định dạng số
views/1_..6_*.py       6 trang
smoke_test.py          kiểm tra truy vấn + render trang
.streamlit/config.toml theme + cấu hình server
```

## Giải thích thuật ngữ

Phần này giải thích các khái niệm phân tích nằm phía sau các chỉ số: chúng là gì, vì sao dashboard dùng chúng và đọc thế nào cho đúng. Công thức chính xác trên từng cột dữ liệu nằm ở [Từ điển chỉ số](#từ-điển-chỉ-số).

### Đo mức độ tập trung

#### HHI (Herfindahl–Hirschman Index)

- **Là gì:** chỉ số đo mức độ cạnh tranh của một thị trường. Lấy thị phần (%) của từng doanh nghiệp, bình phương lên rồi cộng lại. Kết quả nằm trong thang 0–10.000.
- **Vì sao bình phương:** bình phương làm doanh nghiệp lớn nặng ký hơn nhiều so với doanh nghiệp nhỏ. Thị trường có một "ông lớn" sẽ có HHI cao dù số doanh nghiệp đông.
- **Ví dụ:**

  | Thị trường | HHI | Kết luận |
  |---|---:|---|
  | 1 brand chiếm 100% | 10.000 | độc quyền |
  | 3 brand chiếm 50% / 30% / 20% | 2.500 + 900 + 400 = 3.800 | tập trung cao |
  | 4 brand, mỗi brand 25% | 4 × 625 = 2.500 | tập trung cao |
  | 10 brand, mỗi brand 10% | 10 × 100 = 1.000 | ngưỡng tập trung vừa |
  | 100 brand, mỗi brand 1% | 100 | phân tán |

- **Ngưỡng:**
  - Mốc 1.800 lấy từ hướng dẫn sáp nhập (2023) của Bộ Tư pháp và Ủy ban Thương mại Liên bang Mỹ: trên 1.800 là thị trường tập trung cao.
  - Dashboard coi dưới 1.000 là phân tán, khoảng 1.000–1.800 là tập trung vừa.
- **Tác dụng trong dashboard ([2.4]):** cho biết danh mục nào đang bị vài brand chi phối. Danh mục tập trung cao thì brand mới khó chen vào và brand dẫn đầu có quyền định giá. Danh mục phân tán thì cạnh tranh chủ yếu bằng giá và khuyến mãi.
- **Lưu ý:**
  - HHI phụ thuộc vào cách chia thị trường. Danh mục cấp 1 là một thị trường rộng: hai brand cùng "thời trang nam" có thể không cạnh tranh trực tiếp với nhau.
  - "Không rõ thương hiệu" bị loại. Nhưng "OEM" (hàng không thương hiệu) được tính là **một** brand, dù thực tế gồm nhiều nhà sản xuất, nên có thể làm HHI cao hơn thực tế.

#### Tỉ lệ tập trung top-k (CR1, CR5, CR10)

- **Là gì:** tổng thị phần của k doanh nghiệp lớn nhất. CR5 = 60% nghĩa là 5 brand đầu chiếm 60% doanh thu.
- **Tác dụng ([2.4], [3.5]):** dễ hiểu hơn HHI và trả lời thẳng câu "vài người đứng đầu nắm bao nhiêu". [3.5] dùng biến thể theo phần trăm số seller: top 1%, 5%, 10%, 20%, 50% seller.
- **Lưu ý:** CR-k không quan tâm phần còn lại chia ra sao. Hai thị trường cùng CR5 = 60% có thể rất khác nhau, nên đọc cùng HHI.

#### Hệ số Gini

- **Là gì:** chỉ số đo bất bình đẳng, thường dùng cho thu nhập của một quốc gia. Ở đây Gini đo mức chênh lệch doanh thu giữa các seller.
  - Xếp seller từ nhỏ tới lớn, rồi vẽ **đường Lorenz**: trục X là % số seller cộng dồn, trục Y là % doanh thu cộng dồn.
  - Nếu mọi seller bằng nhau, đường Lorenz là đường chéo.
  - Gini = diện tích giữa đường chéo và đường Lorenz ÷ diện tích dưới đường chéo.
- **Thang:** 0 = mọi seller doanh thu bằng nhau. Giá trị tối đa là (n − 1) / n, tiến gần 1 khi có nhiều seller, ứng với trường hợp một seller chiếm hết.
- **Ví dụ** với 4 seller:
  - doanh thu 25 / 25 / 25 / 25 → Gini = 0 ;
  - 10 / 10 / 10 / 70 → Gini = 0,45 ;
  - 0 / 0 / 0 / 100 → Gini = 0,75 (tối đa với 4 seller).
- **Tác dụng ([3.5]):** gom cả phân bố doanh thu thành một con số, để so giữa các kỳ ("thị trường đang tập trung hơn không?") hoặc giữa các danh mục.
- **Lưu ý:** seller không bán được gì vẫn được tính. Sàn có nhiều shop "chết" sẽ có Gini cao dù các shop có bán hàng chia đều doanh thu với nhau.

#### Nguyên lý Pareto (quy tắc 80/20) và đường cong Pareto

- **Là gì:** quan sát rằng phần lớn kết quả đến từ một phần nhỏ nguyên nhân. Trong thương mại điện tử: một nhóm nhỏ sản phẩm tạo ra phần lớn doanh thu, phía sau là một "đuôi dài" sản phẩm bán rất ít.
- **Đường cong Pareto ([2.8]):** xếp sản phẩm theo doanh thu giảm dần, trục X là % sản phẩm, trục Y là % doanh thu lũy kế.
  - Đường càng cong lên sớm thì doanh thu càng tập trung.
  - Đường thẳng chéo nghĩa là mọi sản phẩm đóng góp như nhau.
  - Dashboard đánh dấu sẵn hai điểm "10% sản phẩm → y% doanh thu" và "20% sản phẩm → y% doanh thu".
- **Tác dụng:** cho biết nên tập trung tồn kho, theo dõi giá và quảng cáo vào bao nhiêu sản phẩm.

#### Phân tích ABC

- **Là gì:** cách chia hàng hóa thành nhóm theo mức đóng góp, có nguồn gốc từ quản lý kho. Dashboard dùng mốc 80% / 95% doanh thu lũy kế:
  - **A:** nhóm ít sản phẩm tạo 80% doanh thu đầu tiên ;
  - **B:** 15% doanh thu tiếp theo ;
  - **C:** 5% doanh thu cuối ;
  - **N:** sản phẩm không có doanh thu.
- **Tác dụng ([2.7], [5.7], [6.1], [6.3], [6.4]):**
  - Nhóm A cần ưu tiên: luôn đủ hàng, theo dõi giá đối thủ, xử lý ngay khi bị chê. Vì vậy [6.3] chỉ xét sản phẩm nhóm A bị chê.
  - Nhóm N là ứng viên để xem xét ngừng bán.
- **Lưu ý:** nhóm ABC được xếp một lần trên toàn sàn và toàn kỳ. Khi lọc một danh mục, sản phẩm nhóm A vẫn là "A của toàn sàn".

#### Ma trận tăng trưởng × rating

- **Là gì:** cách chia đối tượng thành 4 nhóm theo hai trục, lấy cảm hứng từ ma trận BCG (tăng trưởng × thị phần). [2.5] dùng trục X là tăng trưởng doanh thu, trục Y là rating, kích thước bóng là doanh thu.
- **4 góc:**
  - **Ngôi sao:** tăng trưởng và được khách đánh giá tốt. Nên đẩy mạnh.
  - **Tăng nhưng rating thấp:** bán tốt nhưng chất lượng có vấn đề, dễ tụt khi review xấu tích lũy.
  - **Rating tốt nhưng chững lại:** sản phẩm được khen nhưng mất đà, có thể do giá hoặc ít quảng bá.
  - **Cần chú ý:** vừa giảm vừa bị chấm thấp.
- **Lưu ý:** đường chia ngang là rating TB của nhóm brand đang xem, không phải một mốc tuyệt đối. Vì vậy luôn có brand nằm dưới đường.

### Thống kê

#### Trung bình và trung vị

- **Là gì:** trung bình (mean) là tổng chia số lượng. Trung vị (median) là giá trị đứng giữa khi xếp theo thứ tự.
- **Ví dụ:** 5 seller có doanh thu 1, 1, 2, 3 và 93 triệu. Trung bình là 20 triệu, nhưng 4 trong 5 seller bán dưới 3 triệu. Trung vị 2 triệu mô tả seller "điển hình" đúng hơn.
- **Tác dụng:** dashboard dùng trung vị ở những chỗ dữ liệu bị lệch bởi vài giá trị cực lớn:
  - doanh thu trung vị / seller ([3.1]) ;
  - uplift trung vị ([4.3], [4.4]) ;
  - giá bình thường = trung vị giá 30 ngày (M7), để một ngày giá bất thường không làm lệch mốc.

#### Bình quân gia quyền và trung bình không trọng số

- **Là gì:** bình quân gia quyền tính mỗi giá trị theo "trọng số" của nó, ví dụ theo lượng bán. Trung bình không trọng số coi mọi giá trị như nhau.
- **Ví dụ:** sản phẩm A giá 100.000 ₫ bán 90 cái, sản phẩm B giá 1.000.000 ₫ bán 10 cái.
  - Giá niêm yết TB (không trọng số) = (100.000 + 1.000.000) / 2 = 550.000 ₫.
  - Giá bán bình quân (gia quyền theo lượng bán) = (9 tr + 10 tr) / 100 cái = 190.000 ₫.
- **Tác dụng:** hai con số trả lời hai câu hỏi khác nhau.
  - Giá niêm yết TB ([2.1]): danh mục đang **bày bán** hàng ở mức giá nào.
  - Giá bán bình quân ([1.1], [1.3], [2.3], [3.2]): khách **thực sự mua** ở mức giá nào.
  - Ngoài ra, rating TB của sản phẩm là trung bình không trọng số: sản phẩm có 5 lượt đánh giá và sản phẩm có 5.000 lượt được tính như nhau.

#### Trung bình trượt 7 ngày

- **Là gì:** mỗi điểm là trung bình của 7 ngày gần nhất tính tới ngày đó. Cửa sổ dịch dần theo thời gian.
- **Vì sao là 7 ngày:** 7 ngày phủ đủ một tuần, nên chênh lệch giữa ngày thường và cuối tuần tự triệt tiêu. Đường còn lại cho thấy xu hướng thật.
- **Tác dụng ([1.2]):** phân biệt "doanh thu đang tăng" với "hôm nay là ngày tốt". Ngày sale làm đường trượt nhích lên trong 7 ngày tiếp theo, nên cần đọc cùng vùng tô ngày sale.

#### Tăng trưởng theo trung bình / ngày

- **Là gì:** so sánh hai kỳ bằng trung bình mỗi ngày thay vì bằng tổng.
- **Vì sao:** kỳ trước có thể thiếu ngày, vì dữ liệu mới bắt đầu hoặc có ngày crawl không đủ bị loại. Ví dụ kỳ này 14 ngày tin cậy, doanh thu 14 tỷ ; kỳ trước chỉ có 10 ngày tin cậy, doanh thu 12 tỷ.
  - So tổng: 14 / 12 − 1 = **+16,7%**, trông như đang tăng.
  - So TB / ngày: 1 tỷ / ngày so với 1,2 tỷ / ngày = **−16,7%**. Thực tế đang giảm.
- **Tác dụng:** dùng cho mọi chỉ số "tăng trưởng so với kỳ trước" ([1.1], [1.3], [2.1], [2.3], [2.5], [3.3]).

#### Cận dưới khoảng tin cậy Wilson

- **Là gì:** khi đo một tỉ lệ từ mẫu nhỏ, tỉ lệ đo được dao động rất mạnh. Khoảng tin cậy Wilson (Edwin B. Wilson, 1927) cho biết tỉ lệ thật có thể nằm trong khoảng nào, với độ tin cậy 95%. **Cận dưới** là giá trị nhỏ nhất của khoảng đó, tức "ít nhất là bao nhiêu".
- **Vì sao dùng để xếp hạng:** xếp theo % thô thì brand chỉ có vài review dễ lên đầu do ngẫu nhiên. Xếp theo cận dưới thì brand ít review bị "phạt" nhiều hơn, vì khoảng tin cậy của nó rộng hơn. Đây là cách Evan Miller đề xuất trong bài "How Not To Sort By Average Rating".
- **Ví dụ:**

  | Brand | Review tiêu cực / review có sao | % thô | Cận dưới Wilson |
  |---|---:|---:|---:|
  | A | 30 / 120 | 25,0% | 18,1% |
  | B | 440 / 2.000 | 22,0% | 20,2% |

  A có % thô cao hơn, nhưng mẫu nhỏ nên tỉ lệ thật của A có thể chỉ khoảng 18%. B gần như chắc chắn bị chê ít nhất 20%, nên B đứng trên A.
- **Tác dụng ([5.6]):** tìm brand **chắc chắn** bị chê nhiều. Biểu đồ vẽ cả cận dưới (thanh) và % thô (vạch dọc). Hai giá trị cách xa nhau nghĩa là brand có ít review.

#### Tương quan không phải nhân quả

- **Là gì:** hai đại lượng cùng tăng (tương quan) không có nghĩa là cái này gây ra cái kia (nhân quả).
- **Ví dụ trong dashboard:**
  - [4.2]: sản phẩm giảm giá sâu bán nhiều hơn. Có thể do giảm giá thật sự kích cầu, nhưng cũng có thể do shop chọn giảm giá cho chính những sản phẩm vốn đã bán chạy.
  - [5.7]: sản phẩm sao cao bán chạy hơn. Có thể do khách tin sản phẩm được khen, nhưng cũng có thể do sản phẩm bán chạy là sản phẩm tốt, nên vừa được khen vừa được mua.
- **Tác dụng:** các biểu đồ này **mô tả** quan hệ, không dự đoán được "giảm giá thêm 10% thì bán thêm bao nhiêu". Muốn biết nhân quả cần thí nghiệm, ví dụ A/B test giá.

### Bán hàng

#### Doanh thu và số bán ước tính

- **Là gì:** Tiki không công bố doanh số theo ngày, chỉ hiện số "đã bán" lũy kế. Pipeline ước tính số bán mỗi ngày bằng phần tăng của "đã bán", rồi nhân với giá bán TB trong ngày để ra doanh thu.
- **Tác dụng:** đủ tốt để so sánh **tương đối**: danh mục nào lớn hơn, ngày nào bán nhiều hơn, brand nào tăng nhanh hơn.
- **Lưu ý:** không dùng như số kế toán, vì:
  - không trừ đơn hủy hoặc trả hàng ;
  - không biết từng đơn được bán ở giá nào trong ngày, hay ở biến thể nào ;
  - số bán của một ngày tính từ lần crawl cuối hôm trước tới lần crawl cuối hôm đó, nên không khớp đúng từ 0 giờ tới 24 giờ.

#### Thị phần

- **Là gì:** phần doanh thu của một đối tượng (danh mục, brand, seller) trong tổng doanh thu của thị trường đang xét.
- **Tác dụng:** so vị thế giữa các đối thủ. Dashboard tính thị phần theo **giá trị** (doanh thu), không theo số lượng. Thị phần theo số lượng sẽ ưu ái brand bán hàng rẻ.
- **Lưu ý:** "thị trường đang xét" (mẫu số) khác nhau giữa các biểu đồ, xem bảng mẫu số trong [Từ điển chỉ số](#từ-điển-chỉ-số).

#### Sell-through (% sản phẩm có bán)

- **Là gì:** trong bán lẻ, sell-through thường là số hàng đã bán / số hàng nhập về. Pipeline không biết tồn kho, nên dashboard dùng một bản xấp xỉ: % sản phẩm bán được ít nhất 1 đơn vị trong ngày.
- **Tác dụng ([1.5], [2.1], [4.2]):** đo độ "sống" của danh mục. Sell-through thấp nghĩa là nhiều sản phẩm bày bán mà không ai mua, tức đuôi dài lớn.

#### Uplift (hệ số tăng)

- **Là gì:** tỉ số giữa kết quả khi có tác động và kết quả bình thường. Uplift ×1 = không thay đổi, ×3 = gấp 3 lần.
- **Hai cách tính trong dashboard:**
  - **Toàn sàn ([1.5]):** doanh thu TB / ngày sale ÷ doanh thu TB / ngày thường.
  - **Từng sản phẩm ([4.3], [4.4], [6.8]):** số bán ngày sale ÷ số bán TB ngày thường của chính sản phẩm đó. Khi gộp nhiều sản phẩm thì lấy trung vị, vì sản phẩm thường ngày bán 0,1 cái / ngày mà ngày sale bán 3 cái đã có uplift ×30.
- **Lưu ý:** crawler chạy khoảng 4,2 giờ một lần, nên một phần số bán của ngày sale được ghi sang hôm sau. Vì vậy dashboard tách riêng "ngày sau sale" và có cột "Bán ngày sau".

### Khuyến mãi

#### % giảm hiển thị và % giảm thật

- **% giảm hiển thị:** con số Tiki ghi trên trang, tính so với **giá gốc** (giá gạch ngang): (giá gốc − giá bán) / giá gốc.
- **% giảm thật:** so với **giá bình thường**, là giá khách thực sự trả các ngày trước đó: (giá bình thường − giá ngày sale) / giá bình thường.
- **Ví dụ thật** (sản phẩm 1000022, ngày 11.11):
  - Giá bán bình thường là 63.000 ₫, giá gốc bình thường cũng là 63.000 ₫.
  - Ngày 11.11, giá gốc được nâng lên 95.000 ₫ và trang ghi "−30%".
  - Khách phải trả 66.000 ₫, tức **đắt hơn** ngày thường 4,8% (% giảm thật = −4,8%).
- **Tác dụng ([4.3], [4.5], [4.7], [6.8]):** khoảng cách giữa hai con số là phần "giảm trên giấy". Khoảng cách càng lớn thì quảng cáo càng phóng đại.

#### Giá tham chiếu và Chỉ thị Omnibus của EU

- **Là gì:** Chỉ thị (EU) 2019/2161, gọi tắt là "Omnibus", quy định rằng khi quảng cáo giảm giá, "giá trước khi giảm" phải là **giá thấp nhất trong 30 ngày trước đó**. Mục đích là chặn việc nâng giá trước rồi "giảm".
- **Cách dashboard áp dụng:** M7 tính hai giá tham chiếu từ 30 ngày trước ngày sale:
  - **Giá bình thường** = trung vị giá bán các ngày không sale. Dùng để tính % giảm thật.
  - **Giá thấp nhất 30 ngày** = đúng chuẩn Omnibus. Dùng để kiểm tra "ngày sale có phải giá tốt nhất tháng không" ([4.3], [6.8]).

#### Giảm giá ảo

- **Là gì:** thủ thuật nâng giá gốc (giá gạch ngang) ngay trước đợt sale để % giảm trông lớn, trong khi giá khách trả gần như không đổi. Tài liệu tiếng Anh gọi là *fake discount* hoặc *reference price inflation*.
- **Dashboard phát hiện ở hai mức:**
  - **Theo từng lần đổi giá (gold F3):** lần đổi giá mà giá gốc tăng **và** % giảm cũng tăng. Đây là cờ "nghi giảm giá ảo" trên các biểu đồ theo ngày ([4.1], [4.8], [6.6]...). Cách này nhanh nhưng thô: trên dữ liệu test đúng khoảng 97% số lần báo và bắt được khoảng 83% trường hợp.
  - **Theo từng ngày sale (M7):** kết luận "giảm giá ảo" khi giá gốc ngày sale cao hơn giá gốc thường ≥ 10% **và** giá bán thật giảm < 5%. Cách này dựa trên giá 30 ngày nên chắc chắn hơn ([4.3]–[4.7], [6.8]).
- **Tác dụng:** tìm seller và sản phẩm quảng cáo sai lệch ([4.6], [4.7], [3.6]). [4.4] còn cho thấy giảm giá ảo có thật sự giúp bán được nhiều hơn sản phẩm không giảm hay không.

#### Giảm danh nghĩa

- **Là gì:** sản phẩm ghi giảm ≥ 10% trong ngày sale, không nâng giá gốc, nhưng giá bán bằng ngày thường. Nói cách khác, mức "giảm" đó đã có từ trước và là giá thường xuyên của sản phẩm.
- **Khác giảm giá ảo:** không có hành vi nâng giá, nhưng ngày sale cũng không rẻ hơn. Với người mua, mua ngày sale hay ngày thường như nhau.

### Đánh giá khách hàng

#### Rating và sao review

- **Rating:** điểm Tiki hiện trên trang sản phẩm hoặc trang seller. Đây là con số tổng hợp mọi lượt đánh giá từ trước tới nay, và là con số người mua nhìn thấy.
- **Sao review:** trung bình số sao của các review mà crawler thu được. Vì có từng review, dashboard tách được theo mức sao, danh mục, tháng viết và thâm niên người viết.
- **Vì sao có thể lệch:** crawler không thu hết mọi review của sản phẩm, nên hai con số được tính trên hai tập lượt đánh giá khác nhau. [6.3] và [6.4] hiện cả hai con số để so.

#### Phân bố sao hình chữ J

- **Là gì:** review trên sàn thương mại điện tử thường dồn rất nhiều vào 5 sao, ít ở 2–4 sao, rồi nhích lên ở 1 sao. Biểu đồ phân bố trông giống chữ J.
- **Hệ quả:** sao TB của hầu hết sản phẩm nằm sát nhau (4,2; 4,3; 4,4), nên khó phân biệt sản phẩm tốt và xấu chỉ bằng sao TB. Vì vậy dashboard dùng thêm % tiêu cực, net sentiment và phân bố sao ([5.2]).

#### % tích cực, % tiêu cực

- **Là gì:** % review từ 4 sao trở lên và % review 1–2 sao. Review 3 sao là trung tính, không thuộc nhóm nào. Mẫu số là review có sao.
- **Tác dụng:** % tiêu cực nhạy hơn sao TB với vấn đề chất lượng. Khi % review 1–2 sao của một sản phẩm tăng từ 5% lên 15%, sao TB chỉ giảm khoảng 0,2–0,4 sao, nhưng % tiêu cực tăng gấp 3. Dashboard lấy **15%** làm ngưỡng cảnh báo ([3.3], [3.6], [6.3], [6.4]).

#### Net sentiment

- **Là gì:** % review 5 sao − % review 1–3 sao, thang −100 … 100. Ý tưởng lấy từ Net Promoter Score (NPS) của Fred Reichheld (2003). NPS chia khách thành người ủng hộ, người thụ động và người phản đối, rồi lấy % ủng hộ trừ % phản đối.
- **Cách chia ở dashboard:**
  - 5 sao là người ủng hộ ;
  - 4 sao là thụ động, không tính ;
  - 1–3 sao là chưa hài lòng.
- **Ví dụ:** 5 sao 60%, 4 sao 25%, 3 sao 7%, 2 sao 3%, 1 sao 5%. % tích cực = 85%, trông rất tốt. Nhưng net sentiment = 60 − 15 = **45**, cho thấy cứ 4 người cực kỳ hài lòng thì có 1 người chưa hài lòng.
- **Tác dụng ([5.1]):** khắt khe hơn % tích cực và phân biệt rõ hơn khi sao TB của các nhóm sát nhau.

#### Thâm niên người review

- **Là gì:** tài khoản của người viết review đã tham gia Tiki bao lâu, theo thông tin trên trang review.
- **Tác dụng ([5.5]):** kiểm tra giả thuyết "khách lâu năm khó tính hơn" và phát hiện bất thường. Ví dụ: rất nhiều review 5 sao đến từ tài khoản mới tạo có thể là dấu hiệu review mua.

### Dữ liệu

#### Ngày tin cậy

- **Là gì:** cờ chất lượng dữ liệu của từng ngày. Một ngày không tin cậy nếu crawler thu được dưới 90% số sản phẩm của ngày đông nhất, hoặc dưới 90% sản phẩm tính được số bán. Đây thường là ngày đầu hoặc cuối đợt crawl, hoặc ngày crawler bị gián đoạn.
- **Tác dụng:** mọi biểu đồ theo ngày bỏ các ngày này. Nếu giữ lại, một ngày chỉ crawl được 1% sản phẩm sẽ trông như doanh thu giảm 99%.

#### SCD Type 2 (Slowly Changing Dimension, loại 2)

- **Là gì:** cách lưu lịch sử thuộc tính trong kho dữ liệu. Khi tiêu đề, xuất xứ hoặc bảo hành của sản phẩm thay đổi, bản cũ không bị ghi đè. Gold đóng bản cũ (ghi thời điểm kết thúc) và thêm một phiên bản mới có thời điểm bắt đầu.
- **Tác dụng ([6.7]):** biết sản phẩm đã đổi gì và khi nào. Ví dụ: shop đổi tên sản phẩm để lên tìm kiếm, hoặc đổi xuất xứ. Doanh số của mỗi ngày gắn với đúng phiên bản thuộc tính tại thời điểm đó.
