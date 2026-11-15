"""
Crawler review + người dùng (API v2) cho Tiki, cào TĂNG DẦN.

Message gửi vào KAFKA_TOPIC (mặc định data_crawled_v2), key = url_id,
record_type = "review". Mỗi message gồm review_summary, reviews[] (kèm comments[])
và users[] (người viết review, không trùng trong message).

Cơ chế tăng dần:
  - Lịch cào và mốc riêng trong bảng `review_crawl_state` (crawler tự tạo bảng và tự
    thêm URL mới từ `crawl_state`), nên không giành URL với crawler sản phẩm.
  - Lần đầu (chưa có mốc) -> "backfill": đi hết mọi trang review.
  - Các lần sau -> "incremental": Tiki trả review mới nhất trước (sort=id|desc thực tế
    sắp theo created_at giảm dần), dừng khi gặp review cũ hơn (mốc - REVIEW_OVERLAP_HOURS).
    Vùng lùi giúp không sót review duyệt muộn; review lặp lại thì downstream loại trùng
    bằng review_id.
  - Nhiều review -> chia nhiều message (REVIEWS_PER_MESSAGE review/message), cùng
    crawl_id, có chunk_index và is_last_chunk.
  - Mốc chỉ được dời khi Kafka đã ack TẤT CẢ message của lần cào đó.
  - crawl_interval tự giãn x2 khi không có review mới, co /2 khi có
    (trong khoảng REVIEW_MIN_INTERVAL..REVIEW_MAX_INTERVAL phút).

Cách chạy:
    python api_review_crawler.py --test-url <url> [--watermark <epoch|ISO>] --out file.json
        -> chỉ gọi API và ghi JSON ra file, KHÔNG đụng Kafka / Postgres
           (--watermark giả lập lần cào incremental)
    python api_review_crawler.py --create-topic
    python api_review_crawler.py
        -> chạy thật
"""
import argparse
import logging
import os
import signal
import threading
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from time import monotonic
from uuid import uuid4

from tiki_api_common import (
    API_PRODUCT, API_REVIEW_ANALYSIS, API_REVIEWS, MESSAGE_SOFT_LIMIT,
    POSTGRES_URL, SCHEMA_VERSION, SOURCE, WORKERS,
    ApiCrawlerBase, KafkaSink, MemorySink, MetricWriter, absolute_url, encode_json, ensure_topic,
    epoch_to_iso, now_iso, parse_ids_from_url, write_test_output, zone_info,
)

logger = logging.getLogger("api_review_crawler")

RECORD_TYPE = "review"
REVIEWS_PER_PAGE = 20     # Tiki chặn limit > 20 ("Query limitation is: 20")
PURCHASED_FROM_PREFIX = "Mua từ nhà bán "   # attributes[] của review: tên shop TẠI THỜI ĐIỂM mua
REVIEWS_PER_MESSAGE = int(os.getenv("REVIEWS_PER_MESSAGE", "1000"))   # ~1KB/review -> ~1MB/message
REVIEW_OVERLAP_HOURS = float(os.getenv("REVIEW_OVERLAP_HOURS", "72"))
REVIEW_INITIAL_INTERVAL = int(os.getenv("REVIEW_INITIAL_INTERVAL", "60"))   # phút
REVIEW_MIN_INTERVAL = int(os.getenv("REVIEW_MIN_INTERVAL", "30"))
REVIEW_MAX_INTERVAL = int(os.getenv("REVIEW_MAX_INTERVAL", "1440"))
SEED_INTERVAL_SEC = int(os.getenv("SEED_INTERVAL_SEC", "600"))   # bao lâu thêm URL mới 1 lần
DEFAULT_AVATAR_MARKER = "/assets/img/avatar"   # ảnh mặc định //tiki.vn/assets/img/avatar.png

METRIC_FILE = os.getenv("API_REVIEW_CRAWLER_METRIC_FILE", "/app/metrics/api_review_crawler_metrics.csv")
METRIC_FIELDS = [
    "url_id", "product_id", "crawl_id", "mode", "chunk_index", "is_last_chunk",
    "n_reviews", "message_bytes", "crawling_start_at", "observed_at", "produced_at",
    "kafka_received_at", "kafka_partition", "kafka_offset", "error_message",
]

# Giữ đồng bộ với src/sql_script/postgresql/ddl/review_crawl_state_ddl.sql
STATE_DDL = f"""
CREATE TABLE IF NOT EXISTS public.review_crawl_state
(
    url_id                 INTEGER PRIMARY KEY REFERENCES public.crawl_state (id) ON DELETE CASCADE,
    last_review_created_at TIMESTAMPTZ,
    last_reviews_count     INTEGER,
    crawl_interval         INTEGER   DEFAULT {REVIEW_INITIAL_INTERVAL},
    next_crawled_at        TIMESTAMP DEFAULT NOW(),
    last_crawled_at        TIMESTAMP,
    last_success_at        TIMESTAMP,
    consecutive_no_new     INTEGER   DEFAULT 0,
    consecutive_errors     INTEGER   DEFAULT 0,
    last_http_status       VARCHAR(20)
);
CREATE INDEX IF NOT EXISTS idx_review_crawl_next ON public.review_crawl_state (next_crawled_at);
"""

SEED_SQL = """
    INSERT INTO public.review_crawl_state (url_id)
    SELECT c.id FROM public.crawl_state c
    WHERE c.status = 'ACTIVE'
      AND NOT EXISTS (SELECT 1 FROM public.review_crawl_state r WHERE r.url_id = c.id)
    ON CONFLICT (url_id) DO NOTHING
    RETURNING url_id;
"""

CLAIM_SQL = """
    UPDATE public.review_crawl_state r
    SET next_crawled_at = NOW() + r.crawl_interval * INTERVAL '1 minute',
        last_crawled_at = NOW()
    FROM public.crawl_state c
    WHERE c.id = r.url_id
      AND r.url_id IN (
          SELECT r2.url_id
          FROM public.review_crawl_state r2
          JOIN public.crawl_state c2 ON c2.id = r2.url_id
          WHERE c2.status = 'ACTIVE'
            AND r2.next_crawled_at <= NOW()
          ORDER BY r2.next_crawled_at ASC
          LIMIT :batch_size
          FOR UPDATE OF r2 SKIP LOCKED
      )
    RETURNING r.url_id, c.url, CAST(EXTRACT(EPOCH FROM r.last_review_created_at) AS BIGINT);
"""

SUCCESS_SQL = """
    UPDATE public.review_crawl_state
    SET last_review_created_at = GREATEST(COALESCE(last_review_created_at, to_timestamp(0)),
                                          to_timestamp(:max_created)),
        last_reviews_count = :reviews_count,
        last_success_at    = NOW(),
        last_http_status   = '200',
        consecutive_errors = 0,
        consecutive_no_new = CASE WHEN :n_new > 0 THEN 0 ELSE consecutive_no_new + 1 END,
        crawl_interval     = CASE WHEN :n_new > 0 THEN GREATEST(:min_iv, crawl_interval / 2)
                                  ELSE LEAST(:max_iv, crawl_interval * 2) END,
        next_crawled_at    = NOW() + (CASE WHEN :n_new > 0 THEN GREATEST(:min_iv, crawl_interval / 2)
                                           ELSE LEAST(:max_iv, crawl_interval * 2) END) * INTERVAL '1 minute'
    WHERE url_id = :url_id;
"""

ERROR_SQL = """
    UPDATE public.review_crawl_state
    SET last_http_status = :http_status,
        consecutive_errors = consecutive_errors + 1
    WHERE url_id = :url_id;
"""


class ReviewFetchError(Exception):
    def __init__(self, status, page):
        super().__init__(f"reviews API page {page} returned {status}")
        self.status = status


class DeliveryTracker:
    """
    Gom kết quả giao của mọi message trong 1 lần cào. Gọi on_done(error, result) đúng
    1 lần, khi đã seal() (cào xong) VÀ mọi message đã có delivery report.
    """

    def __init__(self, on_done):
        self.on_done = on_done
        self._lock = threading.Lock()
        self._pending = 0
        self._sealed = False
        self._done = False
        self._error = None
        self._result = None

    def add(self):
        with self._lock:
            self._pending += 1

    def delivered(self, err=None):
        with self._lock:
            self._pending -= 1
            if err is not None and self._error is None:
                self._error = err
            fire = self._sealed and self._pending == 0 and not self._done
            if fire:
                self._done = True
        if fire:
            self.on_done(self._error, self._result)

    def seal(self, result, error=None):
        with self._lock:
            self._result = result
            if error is not None and self._error is None:
                self._error = error
            self._sealed = True
            fire = self._pending == 0 and not self._done
            if fire:
                self._done = True
        if fire:
            self.on_done(self._error, self._result)


@dataclass
class CrawlContext:
    url_id: int
    url: str
    watermark: int | None          # epoch giây; None = chưa cào review lần nào
    tracker: DeliveryTracker
    product_id: int | None = None
    seen_sellers: dict = field(default_factory=dict)   # seller_id -> tên, từ các review đã cào lần này
    seller_summary: list | None = None
    crawl_id: str = field(default_factory=lambda: str(uuid4()))
    crawl_started_at: str = field(default_factory=now_iso)
    observed_at: str | None = None
    review_summary: dict | None = None
    ai_summary_status: object = "skipped"
    reviews_count: int | None = None
    chunk_index: int = 0
    pages_fetched: int = 0
    n_requests: int = 0
    n_sent: int = 0
    n_new: int = 0
    max_created: int = 0

    @property
    def mode(self):
        return "backfill" if self.watermark is None else "incremental"


# --------------------------------------------------------------------------- #
# Map response API -> thực thể
# --------------------------------------------------------------------------- #
def normalize_avatar_url(url):
    """User chưa đặt ảnh -> Tiki trả ảnh mặc định chung -> None; còn lại thêm https: nếu thiếu."""
    url = absolute_url(url)
    if url is None or DEFAULT_AVATAR_MARKER in url:
        return None
    return url


def build_review_and_user(rv: dict):
    by = rv.get("created_by") or {}
    contrib = (by.get("contribute_info") or {}).get("summary") or {}
    timeline = rv.get("timeline") or {}
    seller = rv.get("seller") or {}
    user_id = by.get("id") or rv.get("customer_id")
    purchased_from = next(
        (a[len(PURCHASED_FROM_PREFIX):].strip() for a in rv.get("attributes") or []
         if isinstance(a, str) and a.startswith(PURCHASED_FROM_PREFIX)),
        None,
    )
    review = {
        "review_id": rv.get("id"),
        "product_id": rv.get("product_id"),
        "spid": rv.get("spid"),                          # mã sản phẩm của người bán + biến thể khách đã mua
        # người bán khách đã mua. Tên lưu kèm review là tên shop LÚC viết review (shop có thể đã
        # đổi tên) -> gộp người bán theo seller_id; tên hiện tại xem seller_summary / product crawler
        "seller_id": seller.get("id"),                   # đôi khi API để trống
        "seller_name": seller.get("name"),
        "purchased_from_seller_name": purchased_from,    # từ attributes, còn khi seller_id trống
        "user_id": user_id,
        "rating": rv.get("rating"),
        "title": rv.get("title"),
        "content": rv.get("content"),
        "status": rv.get("status"),
        "thank_count": rv.get("thank_count"),
        "comment_count": rv.get("comment_count"),
        "score": rv.get("score"),
        "new_score": rv.get("new_score"),
        "is_photo": rv.get("is_photo"),
        "image_urls": [i.get("full_path") for i in rv.get("images") or [] if i.get("full_path")],
        "attributes": rv.get("attributes") or [],
        "product_attributes": rv.get("product_attributes") or [],
        "purchased": by.get("purchased"),
        "purchased_at": epoch_to_iso(by.get("purchased_at")),
        "created_at": epoch_to_iso(rv.get("created_at")),
        "created_at_epoch": rv.get("created_at"),
        "delivery_date": timeline.get("delivery_date"),
        "used_duration_text": timeline.get("content"),
        "vote_attributes": rv.get("vote_attributes"),
        "delivery_rating": rv.get("delivery_rating") or [],
        "comments": [
            {
                "comment_id": c.get("id"),
                "commentator": c.get("commentator"),   # "seller" | "customer" | "tikicare"
                "commentator_id": c.get("customer_id"),
                "commentator_name": c.get("fullname"),
                "content": c.get("content"),
                "status": c.get("status"),
                "created_at": epoch_to_iso(c.get("create_at") or c.get("created_at")),
            }
            for c in rv.get("comments") or []
        ],
    }
    user = {
        "user_id": user_id,
        "name": by.get("full_name") or by.get("name"),
        "avatar_url": normalize_avatar_url(by.get("avatar_url")),
        "region": by.get("region"),
        "account_created_at": by.get("created_time"),
        "group_id": by.get("group_id"),
        "joined_text": contrib.get("joined_time"),
        "total_review": contrib.get("total_review"),
        "total_thank": contrib.get("total_thank"),
    } if user_id else None
    return review, user


# --------------------------------------------------------------------------- #
# Crawler
# --------------------------------------------------------------------------- #
class ApiReviewCrawler(ApiCrawlerBase):
    def __init__(self, *args, reviews_per_message=REVIEWS_PER_MESSAGE,
                 overlap_hours=REVIEW_OVERLAP_HOURS, **kwargs):
        super().__init__(*args, **kwargs)
        self.reviews_per_message = max(1, reviews_per_message)
        self.overlap_hours = overlap_hours
        self.overlap_sec = int(overlap_hours * 3600)
        self._last_seed_at = None

    # --- Postgres ---
    def ensure_state_table(self):
        self.execute(STATE_DDL)

    def before_batch(self):
        if self._last_seed_at is not None and monotonic() - self._last_seed_at < SEED_INTERVAL_SEC:
            return
        added = self.execute(SEED_SQL, fetch=True)
        self._last_seed_at = monotonic()
        if added:
            logger.info(f"Thêm {len(added)} URL mới vào review_crawl_state")

    def claim(self, batch_size):
        return self.execute(CLAIM_SQL, {"batch_size": batch_size}, fetch=True)

    def process(self, row):
        self.process_url(row[0], row[1], row[2])

    def _on_crawl_done(self, url_id, error, ctx: CrawlContext):
        if error is None:
            logger.info(f"URL ID {url_id}: {ctx.mode} xong | gửi {ctx.n_sent} review ({ctx.n_new} mới) | "
                        f"{ctx.chunk_index} message | {ctx.pages_fetched} trang | {ctx.n_requests} request")
        if self.db_engine is None:
            return
        try:
            if error is None:
                self.execute(SUCCESS_SQL, {
                    "url_id": url_id,
                    "max_created": ctx.max_created,
                    "reviews_count": ctx.reviews_count,
                    "n_new": ctx.n_new,
                    "min_iv": REVIEW_MIN_INTERVAL,
                    "max_iv": REVIEW_MAX_INTERVAL,
                })
            else:
                if isinstance(error, ReviewFetchError):
                    status = error.status
                elif isinstance(error, Exception):
                    status = "ERR"
                else:                      # KafkaError từ delivery report
                    status = "KAFKA_ERR"
                self.execute(ERROR_SQL, {"url_id": url_id, "http_status": str(status)[:20]})
        except Exception:
            logger.exception(f"URL ID {url_id}: cập nhật review_crawl_state thất bại")

    # --- cào ---
    def _product_info(self, ctx: CrawlContext):
        """
        Người bán chính (spid, seller_id - API tóm tắt AI bắt buộc có) và danh sách người bán
        đang bán sản phẩm (chính + other_sellers). Cache theo product_id.
        """
        key = ("pdp_info", ctx.product_id)
        hit = self.cache.get(key)
        if hit is not None:
            return hit
        status, p = self.api().get(API_PRODUCT.format(product_id=ctx.product_id),
                                   {"platform": "web", "version": 3})
        ctx.n_requests += 1
        cs = (p or {}).get("current_seller") or {}
        if not cs.get("product_id") or not cs.get("id"):
            return None
        sellers = [{"seller_id": cs["id"], "name": cs.get("name")}]
        for o in (p or {}).get("other_sellers") or []:
            if o.get("id") and o["id"] != cs["id"]:
                sellers.append({"seller_id": o["id"], "name": o.get("name")})
        info = {"spid": int(cs["product_id"]), "seller_id": cs["id"], "sellers": sellers}
        self.cache.put(key, info)
        return info

    def _seller_summary(self, ctx: CrawlContext) -> list:
        """
        Thống kê review theo từng người bán, bằng API review lọc seller_id (đúng con số trang
        web hiển thị cho người bán đó). Phạm vi: người bán đang bán sản phẩm + mọi người bán
        xuất hiện trong review của lần cào này (backfill -> đủ cả người bán cũ đã ngừng bán).
        """
        info = self._product_info(ctx) or {}
        selling = {s["seller_id"]: s["name"] for s in info.get("sellers") or []}
        seller_ids = list(selling) + [sid for sid in ctx.seen_sellers if sid not in selling]
        out = []
        for sid in seller_ids:
            status, r = self.api().get(API_REVIEWS, {
                "limit": 1, "page": 1, "sort": "id|desc",
                "product_id": ctx.product_id, "seller_id": sid,
            })
            ctx.n_requests += 1
            r = r or {}
            out.append({
                "seller_id": sid,
                "seller_name": selling.get(sid) or ctx.seen_sellers.get(sid),
                "is_current_seller": sid == info.get("seller_id"),
                "is_selling_now": sid in selling,
                "reviews_count": r.get("reviews_count"),
                "reviews_listed": (r.get("paging") or {}).get("total"),
                "rating_average": r.get("rating_average"),
                "stars": r.get("stars"),
                "fetch_status": status,
            })
        return out

    def _build_summary(self, head: dict, ctx: CrawlContext, fetch_ai: bool) -> dict:
        ai = None
        if fetch_ai:        # tóm tắt AI chỉ đổi khi có review mới -> không gọi lại vô ích
            info = self._product_info(ctx)
            ids = (info["spid"], info["seller_id"]) if info else None
            if ids:
                status, a = self.api().get(API_REVIEW_ANALYSIS,
                                           {"product_id": ctx.product_id, "spid": ids[0], "seller_id": ids[1]})
                ctx.n_requests += 1
                ctx.ai_summary_status = status
                ai = (a or {}).get("data")
            else:
                ctx.ai_summary_status = "no_product_ids"
        return {
            "rating_average": head.get("rating_average"),
            "reviews_count": head.get("reviews_count"),
            "stars": head.get("stars"),
            "review_photo": head.get("review_photo"),
            "attribute_vote_summary": head.get("attribute_vote_summary"),
            "ai_summary": ai,
        }

    def _crawl(self, ctx: CrawlContext):
        api = self.api()
        ctx.product_id, _ = parse_ids_from_url(ctx.url)
        cutoff = None if ctx.watermark is None else ctx.watermark - self.overlap_sec
        buffer = []      # list[(review, user)] chưa gửi
        page = 1
        while True:
            status, r = api.get(API_REVIEWS, {
                "limit": REVIEWS_PER_PAGE,
                "include": "comments,contribute_info,attribute_vote_summary",
                "sort": "id|desc",
                "page": page,
                "product_id": ctx.product_id,
            })
            ctx.n_requests += 1
            if r is None:
                raise ReviewFetchError(status, page)
            ctx.pages_fetched += 1
            data = r.get("data") or []

            if page == 1:
                ctx.observed_at = now_iso()
                ctx.reviews_count = r.get("reviews_count")
                has_new = any(ctx.watermark is None or (rv.get("created_at") or 0) > ctx.watermark
                              for rv in data)
                ctx.review_summary = self._build_summary(r, ctx, fetch_ai=has_new)

            reached_old = False
            for rv in data:
                created = rv.get("created_at") or 0
                if cutoff is not None and created < cutoff:
                    reached_old = True       # review sắp giảm dần theo created_at -> phần sau đều cũ hơn
                    break
                if ctx.watermark is None or created > ctx.watermark:
                    ctx.n_new += 1
                ctx.max_created = max(ctx.max_created, created)
                seller = rv.get("seller") or {}
                if seller.get("id"):
                    ctx.seen_sellers.setdefault(seller["id"], seller.get("name"))
                buffer.append(build_review_and_user(rv))

            last_page = (r.get("paging") or {}).get("last_page") or 0
            has_more = bool(data) and not reached_old and page < last_page
            while has_more and len(buffer) >= self.reviews_per_message:
                self._send_chunk(ctx, buffer[: self.reviews_per_message], is_last=False)
                buffer = buffer[self.reviews_per_message:]
            if not has_more:
                break
            if self.stop_event.is_set():
                raise InterruptedError("crawler đang dừng giữa chừng, không dời mốc")
            page += 1

        # thống kê theo người bán chỉ đổi khi có review mới -> không có thì khỏi gọi lại
        if ctx.n_new > 0:
            ctx.seller_summary = self._seller_summary(ctx)
        self._send_chunk(ctx, buffer, is_last=True)

    def _send_chunk(self, ctx: CrawlContext, pairs: list, is_last: bool):
        reviews = [rv for rv, _ in pairs]
        users = {}
        for _, user in pairs:
            if user:
                users[user["user_id"]] = user
        ctx.chunk_index += 1
        message = {
            "schema_version": SCHEMA_VERSION,
            "record_type": RECORD_TYPE,
            "source": SOURCE,
            "message_id": str(uuid4()),
            "crawl_id": ctx.crawl_id,
            "url_id": ctx.url_id,
            "url": ctx.url,
            "product_id": ctx.product_id,
            "observed_at": ctx.observed_at,
            "crawl_started_at": ctx.crawl_started_at,
            "emitted_at": now_iso(),
            "mode": ctx.mode,
            "watermark_before": epoch_to_iso(ctx.watermark),
            "chunk_index": ctx.chunk_index,
            "is_last_chunk": is_last,
            "review_summary": ctx.review_summary,          # gộp MỌI người bán
            # theo từng người bán; chỉ có ở message cuối và khi có review mới (null = không đổi)
            "seller_summary": ctx.seller_summary if is_last else None,
            "reviews": reviews,
            "users": list(users.values()),
            "crawl_meta": {
                "review_order": "created_at desc (sort=id|desc)",
                "overlap_hours": self.overlap_hours,
                "reviews_per_message": self.reviews_per_message,
                "pages_fetched": ctx.pages_fetched,
                "n_requests": ctx.n_requests,
                "ai_summary_status": ctx.ai_summary_status,
                "reviews_in_message": len(reviews),
                "reviews_sent_total": ctx.n_sent + len(reviews),
                # 2 trường dưới chỉ chốt được ở message cuối
                "new_reviews_total": ctx.n_new if is_last else None,
                "watermark_after": epoch_to_iso(max(ctx.watermark or 0, ctx.max_created)) if is_last else None,
            },
        }
        value = encode_json(message)
        if len(value) > MESSAGE_SOFT_LIMIT and len(pairs) > 1:
            ctx.chunk_index -= 1
            half = len(pairs) // 2
            self._send_chunk(ctx, pairs[:half], is_last=False)
            self._send_chunk(ctx, pairs[half:], is_last=is_last)
            return

        ctx.n_sent += len(reviews)
        ctx.tracker.add()
        metric_row = {
            "url_id": ctx.url_id, "product_id": ctx.product_id, "crawl_id": ctx.crawl_id,
            "mode": ctx.mode, "chunk_index": ctx.chunk_index, "is_last_chunk": is_last,
            "n_reviews": len(reviews), "message_bytes": len(value),
            "crawling_start_at": ctx.crawl_started_at, "observed_at": ctx.observed_at,
            "produced_at": now_iso(),
        }
        self.sink.send(
            ctx.url_id, value,
            {"schema_version": SCHEMA_VERSION, "record_type": RECORD_TYPE, "source": SOURCE},
            partial(self._on_delivery, ctx, metric_row),
        )

    def _on_delivery(self, ctx: CrawlContext, metric_row, err, partition, offset):
        metric_row["kafka_received_at"] = now_iso()
        if err is not None:
            logger.error(f"URL ID {ctx.url_id}: delivery failed (chunk {metric_row['chunk_index']}): {err}")
            metric_row["error_message"] = str(err)
        else:
            metric_row["kafka_partition"] = partition
            metric_row["kafka_offset"] = offset
        self.write_metric(metric_row)
        ctx.tracker.delivered(err)

    def process_url(self, url_id, url, watermark=None) -> CrawlContext:
        tracker = DeliveryTracker(partial(self._on_crawl_done, url_id))
        ctx = CrawlContext(url_id=url_id, url=url, watermark=watermark, tracker=tracker)
        error = None
        try:
            self._crawl(ctx)
        except Exception as e:
            error = e
            if isinstance(e, (ReviewFetchError, InterruptedError)):
                logger.warning(f"URL ID {url_id}: {e}")
            else:
                logger.exception(f"URL ID {url_id}: lỗi khi cào review {url}")
            self.write_metric({
                "url_id": url_id, "product_id": ctx.product_id, "crawl_id": ctx.crawl_id,
                "mode": ctx.mode, "crawling_start_at": ctx.crawl_started_at,
                "error_message": repr(e)[:500],
            })
        finally:
            tracker.seal(ctx, error)
        return ctx


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def parse_watermark(value):
    if value is None:
        return None
    if value.isdigit():
        return int(value)
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=zone_info)
    return int(dt.timestamp())


def run_test(urls, out_path, watermark, reviews_per_message):
    sink = MemorySink()
    crawler = ApiReviewCrawler(sink=sink, reviews_per_message=reviews_per_message)
    for i, url in enumerate(urls, start=1):
        crawler.process_url(url_id=-i, url=url, watermark=watermark)   # url_id âm = bản thử
    write_test_output(sink.messages, out_path)
    logger.info(f"Đã ghi {len(sink.messages)} message -> {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Tiki API review crawler (v2, incremental) -> Kafka")
    parser.add_argument("--test-url", action="append", help="chạy thử với URL này, ghi ra file, không gửi Kafka")
    parser.add_argument("--out", default="api_review_record_sample.json", help="file JSON đầu ra cho --test-url")
    parser.add_argument("--watermark", help="giả lập mốc của lần cào trước (epoch giây hoặc ISO), chỉ dùng với --test-url")
    parser.add_argument("--reviews-per-message", type=int, default=REVIEWS_PER_MESSAGE)
    parser.add_argument("--create-topic", action="store_true", help="chỉ tạo topic nếu chưa có rồi thoát")
    args = parser.parse_args()

    if args.test_url:
        run_test(args.test_url, args.out, parse_watermark(args.watermark), args.reviews_per_message)
        return
    if args.create_topic:
        ensure_topic()
        return

    from sqlalchemy import create_engine

    crawler = ApiReviewCrawler(
        sink=KafkaSink("api-review-crawler"),
        db_engine=create_engine(POSTGRES_URL, pool_pre_ping=True, pool_size=WORKERS + 2),
        metrics=MetricWriter(METRIC_FILE, METRIC_FIELDS),
        reviews_per_message=args.reviews_per_message,
    )
    crawler.ensure_state_table()
    signal.signal(signal.SIGTERM, crawler.stop)
    signal.signal(signal.SIGINT, crawler.stop)
    logger.info(f"Review crawler start: workers={crawler.workers}, batch={crawler.batch_size}, "
                f"reviews_per_message={crawler.reviews_per_message}, overlap={crawler.overlap_hours}h")
    crawler.run()


if __name__ == "__main__":
    main()
