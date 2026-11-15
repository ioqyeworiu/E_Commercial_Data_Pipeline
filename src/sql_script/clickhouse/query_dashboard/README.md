# Truy vấn dashboard

43 truy vấn cho 6 dashboard, đọc từ lớp data mart ([`MART_LAYER.md`](../../../../MART_LAYER.md)). Mỗi truy vấn là một biểu đồ, có comment `-- [x.y] Tên | Biểu đồ: ...` ở đầu.

| File | Dashboard | Người xem / câu hỏi chính |
|---|---|---|
| [`01_overview.sql`](01_overview.sql) | Tổng quan thị trường | Thị trường lớn cỡ nào, tăng hay giảm, danh mục nào kéo doanh thu, ngày sale có hiệu quả không |
| [`02_category_brand.sql`](02_category_brand.sql) | Danh mục & thương hiệu | Danh mục nào lớn / tăng nhanh, thị trường có bị vài brand chi phối không, phân khúc giá, Pareto sản phẩm |
| [`03_seller.sql`](03_seller.sql) | Người bán | Official so với seller thường, độ tập trung, bảng xếp hạng, seller rủi ro |
| [`04_pricing_promotion.sql`](04_pricing_promotion.sql) | Giá & khuyến mãi | Biến động giá, giảm giá có bán thêm không, ngày sale rẻ thật hay không, ai giảm giá ảo |
| [`05_review.sql`](05_review.sql) | Đánh giá khách hàng | Mức hài lòng, danh mục / brand bị chê, reviewer lâu năm có khó tính hơn không |
| [`06_product.sql`](06_product.sql) | Sản phẩm (danh sách + chi tiết) | Sản phẩm bán chạy / đang lên / bị chê ; trang chi tiết một sản phẩm |

## Dashboard Streamlit

App [`src/python/dashboard/`](../../../python/dashboard/README.md) (service `dashboard` trong `docker-compose.yml`, http://localhost:8501) đọc trực tiếp 6 file SQL này: mỗi truy vấn là một card, tiêu đề và tooltip lấy từ dòng `-- [x.y] Tên | Biểu đồ: ...` và các comment ngay sau nó. Vì vậy khi sửa file ở đây:

- giữ đúng định dạng header (phải có `| Biểu đồ:`), nếu không app sẽ không nhận ra truy vấn;
- đổi tên cột output thì sửa trang tương ứng trong `src/python/dashboard/views/` ;
- thêm / bớt truy vấn thì sửa `EXPECTED_QUERIES` trong `src/python/dashboard/core/sql_catalog.py`.

Kiểm tra cả 43 truy vấn qua app: `MSYS_NO_PATHCONV=1 docker exec -w /app streamlit_dashboard python smoke_test.py`.

## Tham số

Truy vấn dùng cú pháp **query parameter** của ClickHouse (`{tên:Kiểu}`), chạy được ngay bằng `clickhouse-client`:

| Tham số | Kiểu | Ý nghĩa | Ví dụ |
|---|---|---|---|
| `date_from`, `date_to` | Date | Khoảng ngày đang xem. Kỳ so sánh = cùng số ngày liền trước | `2026-10-27`, `2026-11-11` |
| `category_l1` | String | `''` = mọi danh mục ; tên danh mục cấp 1 để lọc / drill-down | `thời trang nam` |
| `sale_event` | String | Ngày sale cho [4.5]–[4.7] | `11.11` |
| `product_id` | Int64 | Sản phẩm cho trang chi tiết [6.4]–[6.8] | `1044005` |

```bash
# Git Bash, từ thư mục gốc dự án: chạy cả một dashboard
docker exec -i clickhouse_server clickhouse-client --user=clickhouse --password=<mật khẩu> \
  -d ecommercial_warehouse --multiquery --format PrettyCompactMonoBlock \
  --param_date_from=2026-10-27 --param_date_to=2026-11-11 --param_category_l1='' \
  --param_sale_event=11.11 --param_product_id=1044005 \
  < src/sql_script/clickhouse/query_dashboard/01_overview.sql
```

Khi đưa vào công cụ BI, đổi cú pháp tham số như sau:

| Công cụ | `{date_from:Date}` / `{date_to:Date}` | `{category_l1:String} = '' OR category_level_1 = {category_l1:String}` |
|---|---|---|
| Grafana (plugin ClickHouse) | `toDate($__fromTime)` / `toDate($__toTime)`, hoặc thay cả điều kiện bằng `$__dateFilter(full_date)` | Biến template `${category_l1}` với giá trị "All" = `''` |
| Metabase (native query) | `{{date_from}}` / `{{date_to}}` (biến kiểu Date) | Thay cả điều kiện bằng `[[AND category_level_1 = {{category_l1}}]]` |
| Superset (SQL Lab, Jinja) | `toDate(parseDateTimeBestEffort('{{ from_dttm }}'))` / tương tự với `to_dttm` | `'{{ filter_values('category_level_1')[0] if filter_values('category_level_1') else '' }}'` |

## Quy ước chung

- Mọi truy vấn theo ngày lọc `is_reliable_day`: bỏ ngày crawl chưa đủ (đầu/cuối kỳ crawl, ngày chưa tính được số bán).
- Tăng trưởng so sánh **trung bình / ngày** của kỳ này và kỳ trước, để kỳ trước thiếu ngày không phóng đại %.
- Tỉ lệ luôn tính bằng `sum(tử) / sum(mẫu)` từ thành phần cộng được của mart, không lấy trung bình của trung bình.
- Thị phần, xếp hạng brand / seller loại member "Không rõ thương hiệu / người bán".
- Doanh thu và số bán là **ước tính** từ chênh lệch "đã bán" lũy kế. Gold đã bỏ qua các lần crawl bị trống "đã bán" ([`MART_LAYER.md` mục 9](../../../../MART_LAYER.md#9-lỗi-đã-bán-ở-gold-phát-hiện-khi-dựng-mart-đã-sửa)).

## Danh mục biểu đồ

### Dashboard 1: Tổng quan thị trường

| ID | Biểu đồ | Nội dung | Mart |
|---|---|---|---|
| 1.1 | KPI cards | Doanh thu, số bán, doanh thu/ngày và % so kỳ trước, giá bán bình quân, % giảm TB, rating TB, số seller / brand hoạt động | M1, M3, M4 |
| 1.2 | Combo cột + đường | Doanh thu theo ngày, TB trượt 7 ngày, % giảm TB, đánh dấu ngày sale | M1 |
| 1.3 | Bar ngang | Doanh thu, thị phần, tăng trưởng theo danh mục cấp 1 | M2 |
| 1.4 | Treemap | Cơ cấu doanh thu cấp 1 > cấp 2 | M2 |
| 1.5 | Bar nhóm | Ngày sale / ngày sau sale / ngày thường: doanh thu/ngày, bán TB/sản phẩm, uplift | M1 |
| 1.6 | Column | Doanh thu TB theo thứ trong tuần (bỏ ngày sale) | M1 |

### Dashboard 2: Danh mục & thương hiệu

| ID | Biểu đồ | Nội dung | Mart |
|---|---|---|---|
| 2.1 | Table + data bar | Bảng hiệu suất danh mục, drill-down cấp 1 → cấp 2 bằng `category_l1` | M2 |
| 2.2 | Stacked area | Doanh thu theo ngày của 5 danh mục lớn nhất + "khác" | M2 |
| 2.3 | Bar ngang + bảng | Top 15 brand: thị phần, giá, % giảm, rating, tăng trưởng | M3 |
| 2.4 | Bar + bảng | Độ tập trung thị trường theo danh mục: HHI, top-1 / top-5 / top-10 share | M3 |
| 2.5 | Bubble | Ma trận brand: tăng trưởng × rating × doanh thu, chia 4 góc | M3 |
| 2.6 | 100% stacked bar | Cơ cấu doanh thu theo phân khúc giá trong từng danh mục | M5 |
| 2.7 | Bar kép | Phân loại ABC: % sản phẩm so với % doanh thu | M6 |
| 2.8 | Line | Đường cong Pareto: x% sản phẩm tạo y% doanh thu | M6 |

### Dashboard 3: Người bán

| ID | Biểu đồ | Nội dung | Mart |
|---|---|---|---|
| 3.1 | KPI cards | Seller hoạt động, % official, thị phần official, doanh thu trung vị / seller, rating seller TB | M4 |
| 3.2 | Bảng so sánh | Official so với seller thường: doanh thu / seller, giá, % giảm, rating, tỉ lệ nghi giảm giá ảo | M4 |
| 3.3 | Table | Top 20 seller: thị phần, tăng trưởng, rating, % review tiêu cực | M4, M6 |
| 3.4 | Multi-line | Rating và lượt đánh giá mới của 5 seller lớn nhất theo ngày | M4 |
| 3.5 | Bar | Top 1/5/10/20/50% seller chiếm bao nhiêu % doanh thu, hệ số Gini | M4 |
| 3.6 | Table có cờ | Seller cần theo dõi: điểm rủi ro 0–3 (giảm giá ảo, review tiêu cực, rating giảm) | M4, M6 |

### Dashboard 4: Giá & khuyến mãi

| ID | Biểu đồ | Nội dung | Mart |
|---|---|---|---|
| 4.1 | Stacked column + đường | Số lần tăng / giảm giá theo ngày, số lần nghi giảm giá ảo | M1 |
| 4.2 | Column + đường | Bán TB / sản phẩm / ngày theo mức giảm giá (chỉ ngày thường) | M5 |
| 4.3 | Table | Bảng điểm mỗi ngày sale: % tham gia, % giảm hiển thị so với thật, % giảm thật, % giá thấp nhất 30 ngày, % giảm giá ảo, uplift | M7 |
| 4.4 | 100% stacked bar | Phân loại khuyến mãi (giảm thật / danh nghĩa / ảo / không giảm) theo sự kiện | M7 |
| 4.5 | Dumbbell | % giảm hiển thị so với % giảm thật theo danh mục | M7 |
| 4.6 | Bar ngang | Top 15 seller có nhiều sản phẩm giảm giá ảo | M7 |
| 4.7 | Table | Danh sách sản phẩm giảm giá ảo (drill-through sang dashboard 6) | M7, M6 |
| 4.8 | Histogram | Phân bố % thay đổi giá (đọc gold F3) | gold F3 |

### Dashboard 5: Đánh giá khách hàng

| ID | Biểu đồ | Nội dung | Mart |
|---|---|---|---|
| 5.1 | KPI cards | Số review, sao TB, % tích cực / tiêu cực, net sentiment, % có ảnh | M8 |
| 5.2 | Bar ngang | Phân bố 5 → 1 sao | M8 |
| 5.3 | Combo | Số review và sao TB theo tháng | M8 |
| 5.4 | Bar ngang | Chất lượng review theo danh mục (drill-down) | M8 |
| 5.5 | Column | Thâm niên reviewer so với sao TB, % tiêu cực | M8 |
| 5.6 | Bar ngang | Top 15 brand bị chê, xếp theo cận dưới Wilson | M6 |
| 5.7 | Column | Nhóm sao TB của sản phẩm so với bán TB/ngày | M6 |

### Dashboard 6: Sản phẩm

| ID | Biểu đồ | Nội dung | Nguồn |
|---|---|---|---|
| 6.1 | Table | Top 20 sản phẩm theo doanh thu | M6 |
| 6.2 | Table | Sản phẩm tăng trưởng 7 ngày cao nhất | M6 |
| 6.3 | Table có cờ | Sản phẩm nhóm A bị chê (sao < 4 hoặc ≥ 15% review 1–2 sao) | M6 |
| 6.4 | Thẻ thông tin | Toàn bộ scorecard của `product_id` | M6 |
| 6.5 | Combo | Giá (mở / đóng / min / max / giá gốc) và số bán theo ngày | gold F2 |
| 6.6 | Table | Nhật ký đổi giá | gold F3 |
| 6.7 | Timeline | Lịch sử thuộc tính SCD2 | gold `gold_dim_product` |
| 6.8 | Table | Sản phẩm trong các ngày sale: giá tham chiếu, % giảm thật, verdict, uplift | M7 |

[6.5]–[6.7] đọc thẳng gold vì cần dữ liệu chi tiết của **một** sản phẩm. `product_id` là cột đầu của `ORDER BY` nên các truy vấn này vẫn chạy dưới 0,5 giây.

## Hiệu năng

Chạy cả 43 truy vấn trên dữ liệu test (10 triệu snapshot, container ClickHouse 2 core / 3 GB), với `category_l1 = ''` và với một danh mục: mọi truy vấn chạy dưới 2,1 giây. Chậm nhất là các truy vấn quét `mart_brand_daily` (675 nghìn dòng) và [1.1] (đếm seller / brand khác nhau trong kỳ).
