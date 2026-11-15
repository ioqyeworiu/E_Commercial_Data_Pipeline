"""
API crawler (v2) cho Tiki.

Thay vì mở trình duyệt rồi parse HTML như refresh/comment crawler, crawler này gọi
thẳng các JSON API mà trang sản phẩm Tiki tự gọi (tìm ra bằng Selenium performance
log + CDP). Mỗi URL trong `crawl_state` -> 1 message JSON gom theo thực thể
(product, seller, brand, categories, variants, promotions, review_summary,
reviews, users) -> Kafka topic `data_crawled_v2`, key = url_id.

Cách chạy:
    python api_crawler.py --test-url <url> [--test-url <url> ...] --out sample.json
        -> chỉ gọi API và ghi JSON ra file, KHÔNG đụng Kafka / Postgres
    python api_crawler.py --create-topic
        -> tạo topic nếu chưa có (KAFKA_TOPIC_PARTITIONS partition), rồi thoát
    python api_crawler.py
        -> chạy thật: claim URL từ Postgres, cào, gửi Kafka (tự tạo topic nếu thiếu)

Lưu ý: dùng chung hàng đợi `crawl_state` với refresh/comment crawler. Nếu chạy song
song, các crawler sẽ chia nhau URL (mỗi URL chỉ 1 crawler lấy trong 1 chu kỳ).
"""
import argparse
import csv
import json
import logging
import os
import re
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from time import monotonic, sleep
from uuid import uuid4
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
)
logger = logging.getLogger("api_crawler")

zone_info = ZoneInfo("Asia/Ho_Chi_Minh")

SCHEMA_VERSION = "v2"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "data_crawled_v2")
KAFKA_TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "6"))
KAFKA_TOPIC_REPLICATION = int(os.getenv("KAFKA_TOPIC_REPLICATION", "1"))
MAX_MESSAGE_BYTES = 10_485_760     # khớp KAFKA_MESSAGE_MAX_BYTES của broker
MESSAGE_SOFT_LIMIT = 9_000_000     # vượt mức này thì cắt bớt reviews

POSTGRES_URL = os.getenv("POSTGRES_URL")

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
)
WORKERS = int(os.getenv("WORKERS", "2"))                     # số thread cào song song
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))             # số URL claim mỗi lần
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.5"))     # giây nghỉ giữa 2 request / thread
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15"))
MAX_REVIEW_PAGES = int(os.getenv("MAX_REVIEW_PAGES", "5"))   # 20 review/trang, mới nhất trước
ENTITY_CACHE_TTL_SEC = int(os.getenv("ENTITY_CACHE_TTL_SEC", "3600"))  # cache seller/store/brand
EMPTY_BATCH_SLEEP = 3
POLL_INTERVAL = 0.1

METRIC_FILE = os.getenv("API_CRAWLER_METRIC_FILE", "/app/metrics/api_crawler_metrics.csv")
METRIC_FIELDS = [
    "url_id", "product_id", "crawling_start_at", "observed_at", "crawling_end_at",
    "produced_at", "kafka_received_at", "kafka_partition", "kafka_offset",
    "message_bytes", "n_requests", "n_reviews", "http_status", "error_message",
]

# --------------------------------------------------------------------------- #
# Endpoint (bắt được từ network log của trang sản phẩm)
# --------------------------------------------------------------------------- #
API_PRODUCT = "https://tiki.vn/api/v2/products/{product_id}"
API_REVIEWS = "https://tiki.vn/api/v2/reviews"
API_SELLER_WIDGET = "https://api.tiki.vn/product-detail/v2/widgets/seller"
API_STORE = "https://tiki.vn/api/v2/seller/stores/{store_slug}"
API_BRAND = "https://tiki.vn/api/v2/brands/{brand_id}"
API_DELIVERY = "https://tiki.vn/api/v2/products/widget/delivery_info/{spid}"
API_COUPON = "https://api.tiki.vn/raiden/v2/asa/seller/coupon"
API_REVIEW_ANALYSIS = "https://api.tiki.vn/falcon/ext/v1/review-analysis"

REVIEWS_PER_PAGE = 20   # Tiki chặn limit > 20 ("Query limitation is: 20")

PRODUCT_ID_RE = re.compile(r"-p(\d+)\.html")
SPID_RE = re.compile(r"[?&]spid=(\d+)")
HTML_TAG_RE = re.compile(r"<[^>]+>")


class ProductUnavailable(Exception):
    def __init__(self, status):
        super().__init__(f"product detail API returned {status}")
        self.status = status


def now_iso() -> str:
    return datetime.now(tz=zone_info).isoformat()


def epoch_to_iso(value):
    if not value:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=zone_info).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def strip_html(value):
    if not isinstance(value, str):
        return value
    return HTML_TAG_RE.sub(" ", value).replace("&nbsp;", " ").strip()


def parse_ids_from_url(url: str):
    m = PRODUCT_ID_RE.search(url)
    if not m:
        raise ValueError(f"không tìm thấy product_id trong URL: {url}")
    s = SPID_RE.search(url)
    return int(m.group(1)), (int(s.group(1)) if s else None)


# --------------------------------------------------------------------------- #
# HTTP client: 1 instance / thread (requests.Session không thread-safe)
# --------------------------------------------------------------------------- #
class TikiApi:
    def __init__(self, user_agent=USER_AGENT, delay=REQUEST_DELAY, timeout=REQUEST_TIMEOUT):
        self.delay = delay
        self.timeout = timeout
        self._last_request_at = 0.0
        self.session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=4))
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "vi-VN,vi;q=0.9",
            "Referer": "https://tiki.vn/",
        })

    def get(self, url, params=None):
        """Trả về (status, json | None). Không raise: lỗi mạng -> status = 'ERR:<loại lỗi>'."""
        wait = self.delay - (monotonic() - self._last_request_at)
        if wait > 0:
            sleep(wait)
        try:
            resp = self.session.get(url, params=params, timeout=self.timeout)
        except requests.RequestException as e:
            return f"ERR:{type(e).__name__}", None
        finally:
            self._last_request_at = monotonic()
        if resp.status_code != 200:
            return resp.status_code, None
        try:
            return 200, resp.json()
        except ValueError:
            return "ERR:invalid_json", None


class TTLCache:
    """Cache dùng chung giữa các thread cho thực thể ít đổi (seller, store, brand)."""

    def __init__(self, ttl_sec):
        self.ttl = ttl_sec
        self._data = {}
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            item = self._data.get(key)
            if item and monotonic() - item[0] < self.ttl:
                return item[1]
        return None

    def put(self, key, value):
        with self._lock:
            self._data[key] = (monotonic(), value)


# --------------------------------------------------------------------------- #
# Map response API -> thực thể
# --------------------------------------------------------------------------- #
def build_product(p: dict, spid, seller_id, leaf_category_id) -> dict:
    stock = p.get("stock_item") or {}
    inventory = p.get("inventory") or {}
    quantity_sold = p.get("quantity_sold") or {}
    specs = []
    for group in p.get("specifications") or []:
        for attr in group.get("attributes") or []:
            specs.append({
                "group": group.get("name"),
                "code": attr.get("code"),
                "name": attr.get("name"),
                "value": attr.get("value"),
            })
    return {
        "product_id": p.get("id"),
        "master_id": p.get("master_id"),
        "spid": spid,
        "sku": p.get("sku"),
        "name": p.get("name"),
        "type": p.get("type"),
        "url": f"https://tiki.vn/{p['url_path']}" if p.get("url_path") else None,
        "short_url": p.get("short_url"),
        "brand_id": (p.get("brand") or {}).get("id"),
        "seller_id": seller_id,
        "leaf_category_id": leaf_category_id,
        "price": p.get("price"),
        "list_price": p.get("list_price"),
        "original_price": p.get("original_price"),
        "discount": p.get("discount"),
        "discount_rate": p.get("discount_rate"),
        "rating_average": p.get("rating_average"),
        "review_count": p.get("review_count"),
        "favourite_count": p.get("favourite_count"),
        "all_time_quantity_sold": p.get("all_time_quantity_sold"),
        "quantity_sold": quantity_sold.get("value"),
        "inventory_status": p.get("inventory_status"),
        "inventory_type": p.get("inventory_type"),
        "fulfillment_type": inventory.get("fulfillment_type"),
        "stock_qty": stock.get("qty"),
        "min_sale_qty": stock.get("min_sale_qty"),
        "max_sale_qty": stock.get("max_sale_qty"),
        "day_ago_created": p.get("day_ago_created"),
        "data_version": p.get("data_version"),
        "short_description": p.get("short_description"),
        "description_html": p.get("description"),
        "thumbnail_url": p.get("thumbnail_url"),
        "image_urls": [img.get("base_url") for img in p.get("images") or [] if img.get("base_url")],
        "specifications": specs,
        "warranty_info": [
            {"name": w.get("name"), "value": w.get("value")} for w in p.get("warranty_info") or []
        ],
        "return_and_exchange_policy": strip_html(p.get("return_and_exchange_policy")),
        "benefits": [strip_html(b.get("text")) for b in p.get("benefits") or []],
        "badges": [b.get("code") for b in p.get("badges_new") or [] if b.get("code")],
        "flags": ((p.get("tracking_info") or {}).get("amplitude")) or {},
    }


def build_categories(p: dict) -> list:
    out, level = [], 0
    for crumb in p.get("breadcrumbs") or []:
        cid = crumb.get("category_id")
        if not cid:          # phần tử cuối của breadcrumbs là chính sản phẩm (category_id = 0)
            continue
        level += 1
        parent = out[-1]["category_id"] if out else None
        out.append({
            "category_id": cid,
            "name": crumb.get("name"),
            "url": f"https://tiki.vn{crumb['url']}" if (crumb.get("url") or "").startswith("/") else crumb.get("url"),
            "level": level,
            "parent_id": parent,
        })
    if out:
        out[-1]["is_leaf"] = True
    return out


def build_variants(p: dict) -> dict:
    return {
        "options": [
            {"code": o.get("code"), "name": o.get("name"),
             "values": [v.get("label") for v in o.get("values") or []]}
            for o in p.get("configurable_options") or []
        ],
        "items": [
            {
                "spid": c.get("id"),
                "child_id": c.get("child_id"),
                "sku": c.get("sku"),
                "name": c.get("name"),
                "option_values": {k: v for k, v in c.items() if k.startswith("option")},
                "price": c.get("price"),
                "original_price": c.get("original_price"),
                "discount_rate": c.get("discount_rate"),
                "inventory_status": c.get("inventory_status"),
                "seller_id": (c.get("seller") or {}).get("id"),
                "selected": c.get("selected"),
            }
            for c in p.get("configurable_products") or []
        ],
    }


def build_brand(p: dict, brand_page: dict | None, fetched_at) -> dict | None:
    brand = p.get("brand") or {}
    if not brand.get("id"):
        return None
    specs = {a.get("code"): a.get("value")
             for g in p.get("specifications") or [] for a in g.get("attributes") or []}
    brand_page = brand_page or {}
    return {
        "brand_id": brand.get("id"),
        "name": brand.get("name"),
        "slug": brand.get("slug"),
        "country": specs.get("brand_country"),
        "brand_page_id": brand_page.get("id"),
        "brand_page_url": f"https://tiki.vn/{brand_page['slug']}" if brand_page.get("slug") else None,
        "thumbnail_url": brand_page.get("thumbnail_url"),
        "description": brand_page.get("description") or None,
        "fetched_at": fetched_at,
    }


def build_seller(current_seller: dict, widget: dict | None, store: dict | None, fetched_at) -> dict:
    widget = widget or {}
    store = store or {}
    link = current_seller.get("link") or widget.get("url")
    return {
        "seller_id": current_seller.get("id") or widget.get("id"),
        "name": current_seller.get("name") or widget.get("name"),
        "store_id": current_seller.get("store_id") or widget.get("store_id"),
        "store_slug": link.rstrip("/").rsplit("/", 1)[-1] if link else None,
        "url": link,
        "logo_url": current_seller.get("logo") or widget.get("icon"),
        "is_official": widget.get("is_official"),
        "is_best_store": current_seller.get("is_best_store"),
        "store_level": widget.get("store_level"),
        "avg_rating_point": widget.get("avg_rating_point"),
        "review_count": widget.get("review_count"),
        "total_follower": widget.get("total_follower"),
        "days_since_joined": widget.get("days_since_joined"),
        "store_live_at": store.get("live_at"),
        "store_description": store.get("description"),
        "fetched_at": fetched_at,
    }


def to_int(value):
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def build_offers(p: dict, current_seller: dict) -> list:
    """Các người bán đang bán sản phẩm này (người bán chính + other_sellers)."""
    offers = [{
        "seller_id": current_seller.get("id"),
        "seller_name": current_seller.get("name"),
        "spid": to_int(current_seller.get("product_id")),   # API trả string
        "sku": current_seller.get("sku"),
        "price": current_seller.get("price"),
        "store_id": current_seller.get("store_id"),
        "is_current": True,
    }] if current_seller else []
    for s in p.get("other_sellers") or []:
        offers.append({
            "seller_id": s.get("id"),
            "seller_name": s.get("name"),
            "spid": to_int(s.get("product_id")),
            "sku": s.get("sku"),
            "price": s.get("price"),
            "store_id": s.get("store_id"),
            "is_current": False,
        })
    return offers


def build_promotions(p: dict, coupon: dict | None, delivery: dict | None) -> dict:
    coupon = coupon or {}
    delivery = delivery or {}
    normal = coupon.get("coupon_normal") or {}
    asa = ((coupon.get("coupon_asa") or {}).get("coupons") or {}).get("items") or []
    return {
        "coupon_text": normal.get("text"),
        "coupons": normal.get("data") or [],
        "asa_coupons": asa,
        "installment_offers": [
            {"title": i.get("title"), "summary": i.get("summary"), "type": i.get("type")}
            for i in p.get("installment_info_v3") or []
        ],
        "delivery": {
            "type": delivery.get("type"),
            "title": delivery.get("title"),
            "seller_name": delivery.get("seller_name"),
            "is_shopping_delivery": delivery.get("is_shopping_delivery"),
            "only_ship_to": delivery.get("only_ship_to"),
            "delivery_options": delivery.get("delivery_options"),
        } if delivery else None,
    }


def build_review_and_user(rv: dict):
    by = rv.get("created_by") or {}
    contrib = (by.get("contribute_info") or {}).get("summary") or {}
    timeline = rv.get("timeline") or {}
    user_id = by.get("id") or rv.get("customer_id")
    review = {
        "review_id": rv.get("id"),
        "product_id": rv.get("product_id"),
        "spid": rv.get("spid"),
        "seller_id": (rv.get("seller") or {}).get("id"),
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
        "delivery_date": timeline.get("delivery_date"),
        "used_duration_text": timeline.get("content"),
        "vote_attributes": rv.get("vote_attributes"),
        "delivery_rating": rv.get("delivery_rating") or [],
        "comments": [
            {
                "comment_id": c.get("id"),
                "commentator": c.get("commentator"),   # "seller" | "customer"
                "commentator_id": c.get("customer_id"),
                "commentator_name": c.get("fullname"),
                "content": c.get("content"),
                "created_at": epoch_to_iso(c.get("create_at") or c.get("created_at")),
            }
            for c in rv.get("comments") or []
        ],
    }
    user = {
        "user_id": user_id,
        "name": by.get("full_name") or by.get("name"),
        "region": by.get("region"),
        "account_created_at": by.get("created_time"),
        "group_id": by.get("group_id"),
        "joined_text": contrib.get("joined_time"),
        "total_review": contrib.get("total_review"),
        "total_thank": contrib.get("total_thank"),
    } if user_id else None
    return review, user


# --------------------------------------------------------------------------- #
# Cào 1 sản phẩm -> 1 record JSON
# --------------------------------------------------------------------------- #
class ProductFetcher:
    def __init__(self, api: TikiApi, cache: TTLCache, max_review_pages=MAX_REVIEW_PAGES):
        self.api = api
        self.cache = cache
        self.max_review_pages = max_review_pages

    def _cached(self, key, fetch_status, name, url, params=None):
        hit = self.cache.get(key)
        if hit is not None:
            fetch_status[name] = "cache"
            return hit
        status, data = self.api.get(url, params)
        fetch_status[name] = status
        if data is None:
            return None
        value = {"data": data, "fetched_at": now_iso()}
        self.cache.put(key, value)
        return value

    def fetch(self, url_id, url) -> dict:
        crawl_started_at = now_iso()
        product_id, spid_hint = parse_ids_from_url(url)
        fetch_status = {}

        params = {"platform": "web", "version": 3}
        if spid_hint:
            params["spid"] = spid_hint
        status, p = self.api.get(API_PRODUCT.format(product_id=product_id), params)
        observed_at = now_iso()      # thời điểm "nhìn thấy" sản phẩm -> time semantics downstream
        fetch_status["product"] = status
        if p is None or not p.get("id"):
            raise ProductUnavailable(status)

        current_seller = p.get("current_seller") or {}
        spid = int(current_seller.get("product_id") or spid_hint or p["id"])
        seller_id = current_seller.get("id")
        brand_id = (p.get("brand") or {}).get("id")
        categories = build_categories(p)
        leaf_category_id = categories[-1]["category_id"] if categories else None

        # --- seller (widget + store), cache theo seller_id ---
        widget = store = None
        if seller_id:
            widget = self._cached(("seller_widget", seller_id), fetch_status, "seller_widget",
                                  API_SELLER_WIDGET,
                                  {"seller_id": seller_id, "mpid": p["id"], "spid": spid,
                                   "platform": "desktop", "version": 3})
            slug = (current_seller.get("link") or "").rstrip("/").rsplit("/", 1)[-1]
            if slug:
                store = self._cached(("store", slug), fetch_status, "store",
                                     API_STORE.format(store_slug=slug))
        widget_resp = (widget or {}).get("data") or {}
        seller_widget_data = (widget_resp.get("data") or {}).get("seller")
        seller = build_seller(
            current_seller, seller_widget_data, (store or {}).get("data"),
            (widget or {}).get("fetched_at"),
        ) if (current_seller or seller_widget_data) else None

        # --- brand, cache theo brand_id ---
        brand_page = None
        if brand_id:
            brand_page = self._cached(("brand", brand_id), fetch_status, "brand",
                                      API_BRAND.format(brand_id=brand_id))
        brand = build_brand(p, (brand_page or {}).get("data"), (brand_page or {}).get("fetched_at"))

        # --- giao hàng + coupon (thay đổi theo thời gian, không cache) ---
        status, delivery = self.api.get(API_DELIVERY.format(spid=spid), {"platform": "web", "pdp": "v2"})
        fetch_status["delivery"] = status
        coupon = None
        if seller_id:
            status, coupon = self.api.get(API_COUPON, {"platform": "desktop", "pid": spid, "seller_id": seller_id})
            fetch_status["coupon"] = status

        # --- reviews, mới nhất trước ---
        reviews, users, review_head = [], {}, None
        pages_fetched = 0
        for page in range(1, self.max_review_pages + 1):
            status, r = self.api.get(API_REVIEWS, {
                "limit": REVIEWS_PER_PAGE,
                "include": "comments,contribute_info,attribute_vote_summary",
                "sort": "id|desc",
                "page": page,
                "spid": spid,
                "product_id": p["id"],
                "seller_id": seller_id or "",
            })
            fetch_status[f"reviews_p{page}"] = status
            if r is None:
                break
            pages_fetched += 1
            if review_head is None:
                review_head = r
            for rv in r.get("data") or []:
                review, user = build_review_and_user(rv)
                reviews.append(review)
                if user:
                    users[user["user_id"]] = user
            paging = r.get("paging") or {}
            if not r.get("data") or page >= (paging.get("last_page") or 0):
                break

        status, analysis = self.api.get(API_REVIEW_ANALYSIS,
                                        {"product_id": p["id"], "spid": spid, "seller_id": seller_id or ""})
        fetch_status["review_analysis"] = status

        review_head = review_head or {}
        review_summary = {
            "rating_average": review_head.get("rating_average", p.get("rating_average")),
            "reviews_count": review_head.get("reviews_count", p.get("review_count")),
            "stars": review_head.get("stars"),
            "review_photo": review_head.get("review_photo"),
            "attribute_vote_summary": review_head.get("attribute_vote_summary"),
            "ai_summary": (analysis or {}).get("data"),
        }

        n_requests = sum(1 for v in fetch_status.values() if v != "cache")
        return {
            "schema_version": SCHEMA_VERSION,
            "source": "tiki_api",
            "message_id": str(uuid4()),
            "url_id": url_id,
            "url": url,
            "observed_at": observed_at,
            "crawl_started_at": crawl_started_at,
            "crawl_finished_at": now_iso(),
            "ids": {
                "product_id": p["id"],
                "spid": spid,
                "seller_id": seller_id,
                "brand_id": brand_id,
                "leaf_category_id": leaf_category_id,
            },
            "product": build_product(p, spid, seller_id, leaf_category_id),
            "seller": seller,
            "brand": brand,
            "categories": categories,
            "variants": build_variants(p),
            "offers": build_offers(p, current_seller),
            "promotions": build_promotions(p, coupon, delivery),
            "review_summary": review_summary,
            "reviews": reviews,
            "users": list(users.values()),
            "crawl_meta": {
                "review_sort": "id|desc",
                "review_pages_fetched": pages_fetched,
                "max_review_pages": self.max_review_pages,
                "reviews_truncated": False,
                "n_requests": n_requests,
                "fetch_status": fetch_status,
            },
        }


def encode_record(record: dict) -> bytes:
    """Serialize; nếu quá lớn thì cắt bớt reviews (giữ review mới nhất)."""
    message = json.dumps(record, ensure_ascii=False).encode("utf-8")
    while len(message) > MESSAGE_SOFT_LIMIT and record["reviews"]:
        record["reviews"] = record["reviews"][: len(record["reviews"]) // 2]
        kept = {r["user_id"] for r in record["reviews"]}
        record["users"] = [u for u in record["users"] if u["user_id"] in kept]
        record["crawl_meta"]["reviews_truncated"] = True
        message = json.dumps(record, ensure_ascii=False).encode("utf-8")
    return message


# --------------------------------------------------------------------------- #
# Kafka + Postgres
# --------------------------------------------------------------------------- #
def ensure_topic(broker=KAFKA_BROKER, topic=KAFKA_TOPIC,
                 partitions=KAFKA_TOPIC_PARTITIONS, replication=KAFKA_TOPIC_REPLICATION) -> bool:
    """Tạo topic nếu chưa có. Trả về True nếu vừa tạo."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": broker})
    existing = admin.list_topics(timeout=15).topics
    if topic in existing:
        logger.info(f"Topic '{topic}' đã tồn tại ({len(existing[topic].partitions)} partition)")
        return False
    futures = admin.create_topics([NewTopic(
        topic,
        num_partitions=partitions,
        replication_factor=replication,
        config={"max.message.bytes": str(MAX_MESSAGE_BYTES)},
    )])
    futures[topic].result(timeout=30)
    logger.info(f"Đã tạo topic '{topic}' với {partitions} partition, replication {replication}")
    return True


def create_kafka_producer():
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": f"api-crawler-{os.getpid()}-{uuid4().hex[:8]}",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "snappy",
        "message.max.bytes": MAX_MESSAGE_BYTES,
        "queue.buffering.max.kbytes": 1_048_576,
    })


CLAIM_SQL = """
    UPDATE public.crawl_state
    SET next_crawled_at = NOW() + crawl_interval*INTERVAL '1 minute',
    last_crawled_at = NOW()
    WHERE id IN (
        SELECT id FROM public.crawl_state
        WHERE status = 'ACTIVE'
        AND next_crawled_at <= NOW()
        ORDER BY next_crawled_at ASC
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, url;
"""

STATUS_SQL = """
    UPDATE public.crawl_state
    SET last_http_status = :http_status,
        consecutive_errors = CASE WHEN :ok THEN 0 ELSE consecutive_errors + 1 END
    WHERE id = :id;
"""


class MetricWriter:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    def write(self, row: dict):
        with self.lock:
            exists = os.path.exists(self.path)
            with open(self.path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=METRIC_FIELDS, extrasaction="ignore")
                if not exists:
                    writer.writeheader()
                writer.writerow(row)


class ApiCrawler:
    def __init__(self):
        from sqlalchemy import create_engine, text

        self._text = text
        self.db_engine = create_engine(POSTGRES_URL, pool_pre_ping=True, pool_size=WORKERS + 1)
        self.producer = create_kafka_producer()
        self.metrics = MetricWriter(METRIC_FILE)
        self.cache = TTLCache(ENTITY_CACHE_TTL_SEC)
        self.local = threading.local()
        self.stop_event = threading.Event()

    def _fetcher(self) -> ProductFetcher:
        if not hasattr(self.local, "fetcher"):
            self.local.fetcher = ProductFetcher(TikiApi(), self.cache)
        return self.local.fetcher

    def _claim(self, batch_size):
        with self.db_engine.begin() as conn:
            return conn.execute(self._text(CLAIM_SQL), {"batch_size": batch_size}).fetchall()

    def _update_status(self, url_id, http_status, ok):
        try:
            with self.db_engine.begin() as conn:
                conn.execute(self._text(STATUS_SQL),
                             {"id": url_id, "http_status": str(http_status)[:20], "ok": ok})
        except Exception:
            logger.exception(f"URL ID {url_id}: cập nhật crawl_state thất bại")

    def _poll_loop(self):
        while not self.stop_event.is_set():
            try:
                self.producer.poll(POLL_INTERVAL)
            except Exception as e:
                logger.error(f"Error occurred while polling Kafka: {e}")

    def _delivery_report(self, err, msg, metric_row):
        metric_row["kafka_received_at"] = now_iso()
        if err is not None:
            logger.error(f"URL ID {metric_row['url_id']}: delivery failed: {err}")
            metric_row["error_message"] = str(err)
        else:
            metric_row["kafka_partition"] = msg.partition()
            metric_row["kafka_offset"] = msg.offset()
            logger.info(f"URL ID {metric_row['url_id']} -> [{msg.topic()}] "
                        f"partition {msg.partition()} offset {msg.offset()}")
        self.metrics.write(metric_row)

    def _produce(self, url_id, message: bytes, metric_row: dict):
        metric_row["produced_at"] = now_iso()
        kwargs = dict(
            topic=KAFKA_TOPIC,
            key=str(url_id).encode("utf-8"),
            value=message,
            headers={"schema_version": SCHEMA_VERSION, "source": "tiki_api"},
            callback=partial(self._delivery_report, metric_row=metric_row),
        )
        try:
            self.producer.produce(**kwargs)
        except BufferError:
            logger.warning(f"Producer queue đầy, chờ (URL ID {url_id})")
            self.producer.poll(1)
            self.producer.produce(**kwargs)

    def process(self, row):
        url_id, url = row[0], row[1]
        if self.stop_event.is_set():
            return
        metric_row = {"url_id": url_id, "crawling_start_at": now_iso()}
        try:
            record = self._fetcher().fetch(url_id, url)
        except ProductUnavailable as e:
            metric_row.update(crawling_end_at=now_iso(), http_status=e.status, error_message=str(e))
            self.metrics.write(metric_row)
            self._update_status(url_id, e.status, ok=False)
            logger.warning(f"URL ID {url_id}: {e}")
            return
        except Exception as e:
            metric_row.update(crawling_end_at=now_iso(), http_status="ERR", error_message=repr(e)[:500])
            self.metrics.write(metric_row)
            self._update_status(url_id, "ERR", ok=False)
            logger.exception(f"URL ID {url_id}: lỗi khi cào {url}")
            return

        message = encode_record(record)
        metric_row.update(
            product_id=record["ids"]["product_id"],
            observed_at=record["observed_at"],
            crawling_end_at=record["crawl_finished_at"],
            message_bytes=len(message),
            n_requests=record["crawl_meta"]["n_requests"],
            n_reviews=len(record["reviews"]),
            http_status=200,
        )
        self._produce(url_id, message, metric_row)
        self._update_status(url_id, 200, ok=True)

    def run(self):
        ensure_topic()
        poll_thread = threading.Thread(target=self._poll_loop, name="kafka-poll", daemon=True)
        poll_thread.start()
        logger.info(f"API crawler start: workers={WORKERS}, batch={BATCH_SIZE}, "
                    f"delay={REQUEST_DELAY}s, max_review_pages={MAX_REVIEW_PAGES}, topic={KAFKA_TOPIC}")
        try:
            with ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix="worker") as pool:
                while not self.stop_event.is_set():
                    rows = self._claim(BATCH_SIZE)
                    if not rows:
                        logger.info(f"Không còn URL đến hạn, nghỉ {EMPTY_BATCH_SLEEP}s")
                        self.stop_event.wait(EMPTY_BATCH_SLEEP)
                        continue
                    list(pool.map(self.process, rows))   # chờ hết batch rồi mới claim tiếp
        finally:
            logger.info("Flushing producer queue...")
            self.stop_event.set()
            poll_thread.join(timeout=5)
            remaining = self.producer.flush(timeout=30)
            if remaining > 0:
                logger.warning(f"Còn {remaining} message chưa gửi được")

    def stop(self, *_):
        logger.info("Nhận tín hiệu dừng, hoàn tất batch hiện tại rồi thoát...")
        self.stop_event.set()


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def run_test(urls, out_path, max_review_pages):
    fetcher = ProductFetcher(TikiApi(), TTLCache(ENTITY_CACHE_TTL_SEC), max_review_pages)
    records = []
    for i, url in enumerate(urls, start=1):
        try:
            record = fetcher.fetch(url_id=-i, url=url)    # url_id âm = bản thử, không có trong crawl_state
        except Exception as e:
            logger.error(f"{url}: {e}")
            continue
        size = len(encode_record(record))
        meta = record["crawl_meta"]
        logger.info(
            f"OK product_id={record['ids']['product_id']} | {size / 1024:.1f} KB | "
            f"{meta['n_requests']} request | {len(record['reviews'])} review | "
            f"{len(record['users'])} user | fetch_status={meta['fetch_status']}"
        )
        records.append(record)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    logger.info(f"Đã ghi {len(records)} record -> {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Tiki API crawler (v2) -> Kafka")
    parser.add_argument("--test-url", action="append", help="chạy thử với URL này, ghi ra file, không gửi Kafka")
    parser.add_argument("--out", default="api_crawler_sample.json", help="file JSON đầu ra cho --test-url")
    parser.add_argument("--max-review-pages", type=int, default=MAX_REVIEW_PAGES)
    parser.add_argument("--create-topic", action="store_true", help="chỉ tạo topic nếu chưa có rồi thoát")
    args = parser.parse_args()

    if args.test_url:
        run_test(args.test_url, args.out, args.max_review_pages)
        return
    if args.create_topic:
        ensure_topic()
        return

    crawler = ApiCrawler()
    signal.signal(signal.SIGTERM, crawler.stop)
    signal.signal(signal.SIGINT, crawler.stop)
    crawler.run()


if __name__ == "__main__":
    main()
