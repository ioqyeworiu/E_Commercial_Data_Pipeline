# api_snapshot_crawl: cào snapshot Tiki trong 1 ngày (18/11/2026)

Bộ này cào **104.763 URL sản phẩm**, mỗi URL đúng **1 lần trong ngày 18/11/2026 (giờ VN)**, và lấy **toàn bộ review** của từng sản phẩm. Dữ liệu được ghi ra CSV theo **đúng định dạng message Kafka** của:

| file | giống message của | topic |
|---|---|---|
| `product_data_changes.csv` | `refresh_crawler.py` (1 dòng / URL) | `product_data_changes` |
| `user_review.csv` | `comment_crawler.py` (1 dòng / review) | `user_review` |
| `product_urls.csv` | `discovery_crawler.py` (`url_id,url`, URL đã bỏ query) | Postgres `crawl_state` |

Script không dùng Selenium mà gọi JSON API công khai của Tiki (giống `api_*_crawler.py`), rồi **đổi giá trị về đúng chuỗi mà crawler HTML nhìn thấy**. Nhờ vậy job `a_raw` và các hàm chuẩn hoá ở bronze đọc được như bình thường.

> **Không chạy cào trên máy cá nhân.** Lệnh `preflight` và `run` đòi biến `TIKI_SNAPSHOT_CLOUD=1` và chỉ chạy trong Claude cloud routine. Trên máy cá nhân chỉ dùng `selftest` (offline, đọc fixture).

## Cách chạy

```bash
pip install requests python-dotenv tzdata                       # + rclone trên PATH
python snapshot_crawler.py selftest --fixtures <dir có cats.json listing.json product.json reviews.json>
TIKI_SNAPSHOT_CLOUD=1 python snapshot_crawler.py preflight --store rclone:gdrive:tiki_crawl_20261118
TIKI_SNAPSHOT_CLOUD=1 python snapshot_crawler.py run       --store rclone:gdrive:tiki_crawl_20261118
python snapshot_crawler.py finalize --store rclone:gdrive:tiki_crawl_20261118    # chỉ gộp file, không gọi Tiki
```

Mã thoát của `run`:

| mã | ý nghĩa |
|---|---|
| 0 | xong, đã có `DONE` |
| 2 | chưa xong, lần chạy sau làm tiếp |
| 3 | một lần chạy khác đang sống (heartbeat < 15 phút) |
| 4 | chưa tới 00:00 ngày 18/11 |

Một lần `run` làm các bước sau:
1. **Discover**:
   - Lấy cây danh mục bằng `api/v2/categories?parent_id=2&include=children`, khoảng 1.214 danh mục lá.
   - Lấy listing của từng danh mục, xoay vòng theo trang: lá trước, rồi nút trong, rồi các kiểu sort, cuối cùng là listing theo người bán.
   - Bỏ trùng theo product_id và lấy N URL đầu tiên. `url_id` = 2.000.001 … 2.104.763, nên không trùng id thật trong `crawl_state` hay dữ liệu test (≥ 1.000.000).
2. **Crawl** theo chunk 2.000 URL, 8 thread, mỗi thread nghỉ 0,5 s giữa 2 request:
   - Mỗi URL gọi product API, widget người bán (cache theo người bán) và **mọi trang** review.
   - Mỗi chunk xong được upload lên Drive thành `parts/*_part_NNNN.csv.gz`, rồi mới ghi `chunk_NNNN.json` (đánh dấu chunk xong).
   - Lần chạy sau bỏ qua các chunk đã xong. Chunk dở dang thì cào lại và bỏ kết quả cũ, nên **mỗi URL chỉ xuất hiện 1 lần** trong kết quả.
3. **Dừng** lúc 23:45 VN, không bắt đầu URL mới sau giờ đó.
4. **Finalize**: gộp các part thành `tiki_crawl_20261118.zip` (thư mục `crawl_20261118/` gồm 3 CSV + `failed_urls.csv` + `manifest.json`), upload lên Drive và ghi `DONE`.

Cấu hình rclone qua biến môi trường của cloud environment, scope `drive.file`:

```
RCLONE_CONFIG_GDRIVE_TYPE=drive
RCLONE_CONFIG_GDRIVE_SCOPE=drive.file
RCLONE_CONFIG_GDRIVE_TOKEN={"access_token":"...","token_type":"Bearer","refresh_token":"...","expiry":"..."}
```

## Đổi field API → chuỗi như crawler HTML

Field sản phẩm (`/api/v2/products/{id}`):

| field | lấy từ API |
|---|---|
| `urlId` | url_id trong `product_urls.csv` |
| `title` | `name` |
| `brand` | `brand.name` |
| `price` | `"{original_price}₫"`, chỉ khi có giảm giá (HTML chỉ hiện giá gốc khi giảm) |
| `discount` | `"-{discount_rate}%"` |
| `sellPrice` | `"{price}₫"` |
| `sold` | token cuối của `quantity_sold.text` (`"Đã bán 3k"` → `"3k"`); không có thì `0` |
| `productRate` / `productRateCount` | `rating_average` làm tròn 1 chữ số / `review_count`; null nếu chưa có review |
| `categoryTree` | tên các breadcrumb (bỏ phần tử cuối là sản phẩm) nối bằng `" > "` |
| `brandOrigin` / `madeIn` / `warranty` | thông số "Xuất xứ thương hiệu" / "…Made in…" / "Sản phẩm có được bảo hành không?" |
| `additionalInfo` | `["key_{tên}: value_{giá trị}", …]` cho các thông số còn lại + `warranty_info` |
| `description` | text của HTML mô tả, nối không khoảng cách như `get_text(strip=True)` |
| `seller` | `current_seller.name` |
| `sellerRate` / `sellerRateCount` | widget người bán: `"4.7"` / `"(5432 đánh giá)"`; nếu chưa có thì cả 2 là `"Chưa có đánh giá"` |
| `observed_at` | lúc product API trả về (ISO, +07:00) |

Field review (`/api/v2/reviews`, đi hết `last_page`):

| field | lấy từ API |
|---|---|
| `user_name` | `created_by.full_name` |
| `joined_duration` | `contribute_info.summary.joined_time` |
| `Đã viết` / `Đã nhận` | `"{total_review} Đánh giá"` / `"{total_thank} Lượt cảm ơn"` |
| `star_score` | `rating` |
| `status` | `title` |
| `comment` | `content` |
| `images_urls` | `['background-image: url("…");', …]` |
| `rate_time` | `"Đánh giá vào N phút/giờ/ngày/tháng/năm trước"`, tính từ `observed_at − created_at` |
| `use_time` | `timeline.content` |
| `prod_properties` | `product_attributes` |
| `observed_at` | lúc lấy trang review đầu tiên của sản phẩm |

Quy ước chung: CSV là UTF-8 không BOM, field dạng list ghi thành chuỗi JSON (`ensure_ascii=False`), null ghi thành ô rỗng. URL bị lỗi sau khi đã thử lại (404, sản phẩm bị gỡ) không có dòng sản phẩm; chúng được liệt kê trong `failed_urls.csv`.
