"""
Cào snapshot 1 lần trong 1 ngày (18/11/2026, giờ VN) qua JSON API của Tiki, nhưng GHI RA
ĐÚNG định dạng message Kafka của 2 crawler Selenium:
    refresh_crawler.py  -> product_data_changes.csv   (1 dòng / URL)
    comment_crawler.py  -> user_review.csv            (1 dòng / review, lấy HẾT review)

CHỈ chạy trong Claude cloud routine (không bao giờ chạy cào trên máy người dùng):
mọi lệnh có gọi mạng đòi biến môi trường TIKI_SNAPSHOT_CLOUD=1.

Lệnh:
    selftest  --fixtures DIR      offline: map product.json / reviews.json / listing.json / cats.json
    preflight --store S           ~8 request kiểm tra endpoint + ghi preflight.txt lên store
    run       --store S           discover (nếu chưa có) -> crawl theo chunk -> finalize; chạy lại được
    finalize  --store S           gộp các part trên store -> tiki_crawl_20261118.zip

Store: rclone:gdrive:tiki_crawl_20261118 (Google Drive) hoặc local:<dir>.
Bố cục store:
    product_urls.csv.gz, discovery_meta.json, heartbeat.json, DONE, preflight.txt
    parts/{products,reviews,failed}_part_NNNN.csv.gz, parts/chunk_NNNN.json (ghi CUỐI = chunk xong)
    tiki_crawl_20261118.zip
"""
import argparse
import csv
import gzip
import io
import json
import logging
import math
import os
import random
import sys
import threading
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from html.parser import HTMLParser

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))     # src/python/crawler -> tiki_api_common

from tiki_api_common import (  # noqa: E402
    API_PRODUCT, API_PRODUCT_LISTING, API_REVIEWS, API_SELLER_WIDGET,
    TikiApi, parse_ids_from_url, zone_info,
)
from drive_sync import make_store  # noqa: E402

logger = logging.getLogger("snapshot_crawler")

# --------------------------------------------------------------------------- #
# Tham số
# --------------------------------------------------------------------------- #
DAY = os.getenv("SNAPSHOT_DAY", "2026-11-18")            # ngày cào (giờ VN)
N_URLS = int(os.getenv("SNAPSHOT_N_URLS", "104763"))     # số URL sản phẩm (lẻ, không tròn)
URL_ID_START = int(os.getenv("SNAPSHOT_URL_ID_START", "2000001"))   # tránh id thật của crawl_state + dữ liệu test (>=1.000.000)
CHUNK_SIZE = int(os.getenv("SNAPSHOT_CHUNK_SIZE", "2000"))
WORKERS = int(os.getenv("SNAPSHOT_WORKERS", "8"))
REQUEST_DELAY = float(os.getenv("SNAPSHOT_REQUEST_DELAY", "0.5"))   # giây / thread -> <= 16 req/s
DEADLINE_HHMM = os.getenv("SNAPSHOT_DEADLINE", "23:45")  # sau giờ này không bắt đầu URL mới
HEARTBEAT_SEC = 300
HEARTBEAT_STALE_SEC = 900
MAX_LISTING_PAGES = 50
ZIP_NAME = "tiki_crawl_20261118.zip"
ZIP_DIR = "crawl_20261118"

API_CATEGORIES = "https://tiki.vn/api/v2/categories"
API_LISTING = "https://tiki.vn/api/personalish/v1/blocks/listings"
ROOT_CATEGORY_ID = 2

PRODUCT_FIELDS = [   # refresh_crawler.py:342-362 (thứ tự key của record gửi Kafka)
    "urlId", "title", "brand", "price", "discount", "sellPrice", "sold", "productRate",
    "productRateCount", "categoryTree", "brandOrigin", "madeIn", "warranty", "description",
    "seller", "sellerRate", "sellerRateCount", "additionalInfo", "observed_at",
]
REVIEW_FIELDS = [    # comment_crawler.py:238-327
    "url_id", "user_name", "joined_duration", "Đã viết", "Đã nhận", "star_score", "status",
    "comment", "images_urls", "rate_time", "use_time", "prod_properties", "observed_at",
]
URL_FIELDS = ["url_id", "url"]
FAILED_FIELDS = ["url_id", "url", "stage", "http_status", "detail", "at"]
NO_SELLER_RATING = "Chưa có đánh giá"


def now_vn() -> datetime:
    return datetime.now(tz=zone_info)


def day_bounds(day: str):
    start = datetime.fromisoformat(day).replace(tzinfo=zone_info)
    hh, mm = (int(x) for x in DEADLINE_HHMM.split(":"))
    return start, start.replace(hour=hh, minute=mm), start + timedelta(days=1)


def require_cloud():
    if os.getenv("TIKI_SNAPSHOT_CLOUD") != "1":
        sys.exit("Từ chối: lệnh này gọi mạng tới Tiki, chỉ được chạy trong Claude cloud routine "
                 "(đặt TIKI_SNAPSHOT_CLOUD=1 ở đó). Trên máy người dùng chỉ chạy `selftest`.")


# --------------------------------------------------------------------------- #
# Chuyển giá trị API -> chuỗi y như crawler HTML thấy
# --------------------------------------------------------------------------- #
class _TextJoiner(HTMLParser):
    """Giống BeautifulSoup get_text(strip=True): strip từng mẩu text rồi nối KHÔNG khoảng cách."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip += 1

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self._skip:
            self._skip -= 1

    def handle_data(self, data):
        if not self._skip:
            s = data.strip()
            if s:
                self.parts.append(s)


def html_text(value):
    if value is None:
        return None
    if not isinstance(value, str):
        return str(value)
    p = _TextJoiner()
    p.feed(value)
    p.close()
    return "".join(p.parts)


def vnd(value):
    """37000 -> '37000₫' (HTML '37.000₫' sau khi crawler bỏ dấu '.')"""
    try:
        return f"{int(round(float(value)))}₫"
    except (TypeError, ValueError):
        return None


def relative_vi(seconds) -> str:
    s = max(0, int(seconds))
    if s < 3600:
        return f"{max(1, s // 60)} phút trước"
    if s < 86400:
        return f"{s // 3600} giờ trước"
    d = s // 86400
    if d < 30:
        return f"{d} ngày trước"
    if d < 365:
        return f"{d // 30} tháng trước"
    return f"{d // 365} năm trước"


def map_product(url_id, p: dict, seller_rating, observed_at: str) -> dict:
    price, original = p.get("price"), p.get("original_price") or p.get("list_price")
    rate = p.get("discount_rate") or 0
    discounted = rate > 0 and original and price is not None and original > price

    qs = p.get("quantity_sold") or {}
    tokens = (qs.get("text") or "").split()
    sold = (tokens[-1] if tokens else None) if qs else 0

    review_count = p.get("review_count") or 0
    product_rate = round(float(p.get("rating_average") or 0), 1) if review_count > 0 else None

    crumbs = [c.get("name") for c in p.get("breadcrumbs") or [] if c.get("category_id")]
    category_tree = " > ".join(n.strip() for n in crumbs if n) or None

    brand_origin = made_in = warranty = ""
    extra = []
    for group in p.get("specifications") or []:
        for attr in group.get("attributes") or []:
            name = (attr.get("name") or "").strip()
            value = html_text(attr.get("value"))
            if "Xuất xứ thương hiệu" in name:
                brand_origin = value or ""
            elif "Made in" in name:
                made_in = value or ""
            elif "Sản phẩm có được bảo hành không?" in name:
                warranty = value or ""
            else:
                extra.append(f"key_{name}: value_{value}")
    for w in p.get("warranty_info") or []:
        extra.append(f"key_{(w.get('name') or '').strip()}: value_{html_text(w.get('value'))}")

    seller = (p.get("current_seller") or {}).get("name")
    seller_rate, seller_rate_count = seller_rating or (NO_SELLER_RATING, NO_SELLER_RATING)
    brand = (p.get("brand") or {}).get("name")

    return {
        "urlId": url_id,
        "title": (p.get("name") or "").strip(),
        "brand": brand.strip() if brand else None,
        "price": vnd(original) if discounted else None,
        "discount": f"-{int(round(rate))}%" if discounted else None,
        "sellPrice": vnd(price) if price is not None else 0.0,
        "sold": sold,
        "productRate": product_rate,
        "productRateCount": int(review_count) if review_count > 0 else None,
        "categoryTree": category_tree,
        "brandOrigin": brand_origin,
        "madeIn": made_in,
        "warranty": warranty,
        "description": html_text(p.get("description")),
        "seller": seller.strip() if seller else None,
        "sellerRate": seller_rate,
        "sellerRateCount": seller_rate_count,
        "additionalInfo": extra or None,
        "observed_at": observed_at,
    }


def seller_rating_from_widget(widget: dict | None):
    """API widget người bán -> ('4.7', '(5432 đánh giá)') hoặc None = chưa có đánh giá."""
    s = ((widget or {}).get("data") or {}).get("seller") or {}
    point, count = s.get("avg_rating_point"), s.get("review_count")
    try:
        point = float(point)
    except (TypeError, ValueError):
        return None
    if not point or not count:
        return None
    count_text = f"{int(count)}" if isinstance(count, (int, float)) else str(count).strip()
    return f"{round(point, 1)}", f"({count_text} đánh giá)"


def _prod_property(a):
    if isinstance(a, str):
        return a.strip()
    if isinstance(a, dict):
        name = a.get("name") or a.get("label") or a.get("code")
        value = a.get("value") or a.get("values")
        if name is not None and value is not None:
            return f"{name}: {value}"
    return json.dumps(a, ensure_ascii=False)


def map_review(url_id, rv: dict, observed_dt: datetime) -> dict:
    by = rv.get("created_by") or {}
    summary = (by.get("contribute_info") or {}).get("summary") or {}
    timeline = rv.get("timeline") or {}
    created = rv.get("created_at")
    written, thanked = summary.get("total_review"), summary.get("total_thank")
    return {
        "url_id": url_id,
        "user_name": (by.get("full_name") or by.get("name") or "").strip(),
        "joined_duration": (summary.get("joined_time") or "").strip(),
        "Đã viết": f"{written} Đánh giá" if written is not None else "",
        "Đã nhận": f"{thanked} Lượt cảm ơn" if thanked is not None else "",
        "star_score": rv.get("rating") if rv.get("rating") is not None else "",
        "status": (rv.get("title") or "").strip(),
        "comment": (rv.get("content") or "").strip(),
        "images_urls": [f'background-image: url("{i["full_path"]}");'
                        for i in rv.get("images") or [] if i.get("full_path")],
        "rate_time": (f"Đánh giá vào {relative_vi(observed_dt.timestamp() - int(created))}"
                      if created else ""),
        "use_time": (timeline.get("content") or "").strip(),
        "prod_properties": [_prod_property(a) for a in rv.get("product_attributes") or []],
        "observed_at": observed_dt.isoformat(),
    }


def listing_item_url(item: dict):
    """Item listing -> (product_id, URL chuẩn hoá như standalize_url) hoặc None."""
    path = (item.get("url_path") or "").split("?")[0].strip("/")
    if not path and item.get("url_key"):
        path = f"{item['url_key']}.html"
    if not path:
        return None
    url = f"https://tiki.vn/{path}"
    try:
        pid, _ = parse_ids_from_url(url)
    except ValueError:
        return None
    return pid, url


def csv_cell(v):
    if v is None:
        return ""
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    return v


def write_csv_gz(path, fields, rows):
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(fields)
        for r in rows:
            w.writerow([csv_cell(r.get(k)) for k in fields])


# --------------------------------------------------------------------------- #
# HTTP: TikiApi (retry + giãn cách) + backoff dài + tạm dừng toàn cục khi bị 429/403
# --------------------------------------------------------------------------- #
PERMANENT = {400, 404, 410}


class Fetcher:
    def __init__(self, api_factory, max_attempts=6):
        self.api_factory = api_factory
        self.max_attempts = max_attempts
        self.local = threading.local()
        self.lock = threading.Lock()
        self.pause_until = 0.0
        self.stats = {}

    def _api(self):
        if not hasattr(self.local, "api"):
            self.local.api = self.api_factory()
        return self.local.api

    def _count(self, status):
        with self.lock:
            key = str(status)
            self.stats[key] = self.stats.get(key, 0) + 1

    def snapshot(self):
        with self.lock:
            return dict(self.stats)

    def get(self, url, params=None):
        """Trả (status, json|None). 404... -> trả ngay; 429/403/5xx/lỗi mạng -> chờ rồi thử lại."""
        status = None
        for attempt in range(self.max_attempts):
            wait = self.pause_until - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            status, data = self._api().get(url, params)
            self._count(status)
            if status == 200 and data is not None:
                return status, data
            if status in PERMANENT:
                return status, None
            backoff = min(180, 5 * 2 ** attempt) + random.uniform(0, 2)
            if status in (429, 403):
                with self.lock:
                    self.pause_until = max(self.pause_until, time.monotonic() + backoff)
                logger.warning(f"HTTP {status} -> tạm dừng mọi thread {backoff:.0f}s ({url})")
            else:
                time.sleep(backoff)
        return status, None


class SellerCache:
    def __init__(self, fetch: Fetcher):
        self.fetch = fetch
        self.data = {}
        self.lock = threading.Lock()

    def rating(self, seller_id):
        if not seller_id:
            return None
        with self.lock:
            if seller_id in self.data:
                return self.data[seller_id]
        status, w = self.fetch.get(API_SELLER_WIDGET,
                                   {"seller_id": seller_id, "platform": "desktop", "version": 3})
        value = seller_rating_from_widget(w)
        if w is not None:
            with self.lock:
                self.data[seller_id] = value
        return value


# --------------------------------------------------------------------------- #
# Discovery: cây danh mục -> listing từng danh mục (round-robin theo trang) -> N URL
# --------------------------------------------------------------------------- #
class Source:
    def __init__(self, kind, key, url_key=None, sort=None):
        self.kind, self.key, self.url_key, self.sort = kind, key, url_key, sort

    def fetch(self, fetch: Fetcher, page):
        if self.kind == "category":
            params = {"limit": 40, "category": self.key, "page": page}
            if self.url_key:
                params["urlKey"] = self.url_key
            if self.sort:
                params["sort"] = self.sort
            status, r = fetch.get(API_LISTING, params)
        else:   # seller
            status, r = fetch.get(API_PRODUCT_LISTING, {"limit": 40, "page": page, "seller": self.key})
        r = r or {}
        paging = r.get("paging") or {}
        return r.get("data") or [], paging.get("last_page") or 0


def category_nodes(tree: dict):
    leaves, inner = [], []

    def walk(node):
        children = node.get("children") or []
        (inner if children else leaves).append((node["id"], node.get("url_key")))
        for c in children:
            walk(c)

    for root in tree.get("data") or []:
        walk(root)
    return leaves, inner


def discover(fetch: Fetcher, pool: ThreadPoolExecutor, n: int):
    status, tree = fetch.get(API_CATEGORIES, {"parent_id": ROOT_CATEGORY_ID, "include": "children"})
    if not tree:
        raise RuntimeError(f"không lấy được cây danh mục (HTTP {status})")
    leaves, inner = category_nodes(tree)
    logger.info(f"Cây danh mục: {len(leaves)} lá, {len(inner)} nút trong")

    found = {}            # product_id -> url, giữ thứ tự phát hiện
    sellers = {}          # seller_id -> None (thứ tự gặp)
    stages = [
        ("leaf", [Source("category", c, k) for c, k in leaves]),
        ("inner", [Source("category", c, k) for c, k in inner]),
    ] + [
        (f"leaf_sort_{s}", [Source("category", c, k, s) for c, k in leaves])
        for s in ("top_seller", "newest", "price,asc", "price,desc")
    ] + [("seller", None)]
    stage_stats = []

    for name, sources in stages:
        if len(found) >= n:
            break
        if sources is None:   # mở rộng theo người bán đã gặp
            sources = [Source("seller", sid) for sid in sellers]
        before = len(found)
        active, page = list(sources), 1
        while active and len(found) < n and page <= MAX_LISTING_PAGES:
            results = list(pool.map(lambda s: s.fetch(fetch, page), active))
            next_active = []
            for src, (items, last_page) in zip(active, results):
                for it in items:
                    if it.get("advertisement"):
                        continue
                    if it.get("seller_id"):
                        sellers.setdefault(it["seller_id"], None)
                    parsed = listing_item_url(it)
                    if parsed and parsed[0] not in found and len(found) < n:
                        found[parsed[0]] = parsed[1]
                if items and page < last_page:
                    next_active.append(src)
            logger.info(f"discover[{name}] trang {page}: {len(active)} nguồn -> tổng {len(found)} URL")
            active, page = next_active, page + 1
        stage_stats.append({"stage": name, "sources": len(sources), "new_urls": len(found) - before,
                            "last_page_reached": page - 1})

    urls = [{"url_id": URL_ID_START + i, "url": u} for i, u in enumerate(found.values())]
    meta = {
        "generated_at": now_vn().isoformat(), "target": n, "found": len(urls),
        "leaf_categories": len(leaves), "inner_categories": len(inner),
        "sellers_seen": len(sellers), "stages": stage_stats, "http": fetch.snapshot(),
        "url_id_range": [URL_ID_START, URL_ID_START + len(urls) - 1] if urls else None,
    }
    if len(urls) < n:
        logger.warning(f"Chỉ tìm được {len(urls)}/{n} URL")
    return urls, meta


# --------------------------------------------------------------------------- #
# Crawl 1 URL: sản phẩm + toàn bộ review
# --------------------------------------------------------------------------- #
def crawl_one(fetch: Fetcher, sellers: SellerCache, url_id, url, stop_at: datetime | None):
    """Trả (product_row | None, review_rows, failures, skipped)."""
    if stop_at is not None and now_vn() >= stop_at:
        return None, [], [], True
    failures = []

    def fail(stage, status, detail=""):
        failures.append({"url_id": url_id, "url": url, "stage": stage, "http_status": status,
                         "detail": detail, "at": now_vn().isoformat()})

    try:
        product_id, spid = parse_ids_from_url(url)
    except ValueError as e:
        fail("parse_url", "", str(e))
        return None, [], failures, False

    params = {"platform": "web", "version": 3}
    if spid:
        params["spid"] = spid
    status, p = fetch.get(API_PRODUCT.format(product_id=product_id), params)
    observed_at = now_vn().isoformat()
    if not p or not p.get("id"):
        fail("product", status)
        return None, [], failures, False

    rating = sellers.rating((p.get("current_seller") or {}).get("id"))
    product_row = map_product(url_id, p, rating, observed_at)

    reviews = []
    if (p.get("review_count") or 0) > 0:
        page, observed_dt = 1, None
        while True:
            status, r = fetch.get(API_REVIEWS, {
                "limit": 20, "include": "comments,contribute_info,attribute_vote_summary",
                "sort": "id|desc", "page": page, "product_id": product_id,
            })
            if r is None:
                fail("reviews", status, f"page {page}")
                break
            if page == 1:
                observed_dt = now_vn()
            data = r.get("data") or []
            reviews.extend(map_review(url_id, rv, observed_dt) for rv in data)
            last_page = (r.get("paging") or {}).get("last_page") or 0
            if not data or page >= last_page:
                break
            page += 1
    return product_row, reviews, failures, False


# --------------------------------------------------------------------------- #
# Heartbeat / khoá giữa các lần chạy routine
# --------------------------------------------------------------------------- #
class Heartbeat:
    def __init__(self, store, run_id):
        self.store, self.run_id = store, run_id
        self.state = {"stage": "start"}
        self._stop = threading.Event()
        self._thread = None

    def other_alive(self):
        text = self.store.get_text("heartbeat.json")
        if not text:
            return False
        try:
            hb = json.loads(text)
            age = (now_vn() - datetime.fromisoformat(hb["at"])).total_seconds()
        except (ValueError, KeyError):
            return False
        alive = hb.get("run_id") != self.run_id and hb.get("status") != "stopped" and age < HEARTBEAT_STALE_SEC
        if alive:
            logger.info(f"Lần chạy khác ({hb.get('run_id')}) còn sống, heartbeat {age:.0f}s trước")
        return alive

    def beat(self, status="running"):
        body = {"run_id": self.run_id, "at": now_vn().isoformat(), "status": status, **self.state}
        try:
            self.store.put_text("heartbeat.json", json.dumps(body, ensure_ascii=False, indent=1))
        except Exception:
            logger.exception("ghi heartbeat lỗi")

    def update(self, **kw):
        self.state.update(kw)

    def start(self):
        self.beat()

        def loop():
            while not self._stop.wait(HEARTBEAT_SEC):
                self.beat()

        self._thread = threading.Thread(target=loop, name="heartbeat", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        self.beat("stopped")


# --------------------------------------------------------------------------- #
# Các bước
# --------------------------------------------------------------------------- #
def load_or_discover(store, fetch, pool, workdir, n):
    local = os.path.join(workdir, "product_urls.csv.gz")
    if store.get("product_urls.csv.gz", local):
        with gzip.open(local, "rt", encoding="utf-8", newline="") as f:
            urls = [{"url_id": int(r["url_id"]), "url": r["url"]} for r in csv.DictReader(f)]
        logger.info(f"Đã có product_urls.csv.gz trên store: {len(urls)} URL")
        return urls
    urls, meta = discover(fetch, pool, n)
    write_csv_gz(local, URL_FIELDS, urls)
    store.put_text("discovery_meta.json", json.dumps(meta, ensure_ascii=False, indent=1))
    store.put(local, "product_urls.csv.gz")
    logger.info(f"Discovery xong: {len(urls)} URL -> store")
    return urls


def chunk_metas(store, workdir):
    d = os.path.join(workdir, "metas")
    store.get_dir("parts", d, include="chunk_*.json")
    metas = {}
    for name in os.listdir(d) if os.path.isdir(d) else []:
        with open(os.path.join(d, name), encoding="utf-8") as f:
            m = json.load(f)
        metas[m["chunk"]] = m
    return metas


def crawl(store, fetch, pool, urls, workdir, stop_at, hb, chunk_size=CHUNK_SIZE):
    sellers = SellerCache(fetch)
    metas = chunk_metas(store, workdir)
    chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]
    logger.info(f"{len(chunks)} chunk, đã xong {len(metas)}")
    for idx, chunk in enumerate(chunks):
        if idx in metas:
            continue
        if stop_at is not None and now_vn() >= stop_at:
            logger.warning("Tới hạn chót, dừng crawl")
            break
        started = now_vn().isoformat()
        http_before = fetch.snapshot()
        hb.update(stage="crawl", chunk=idx, chunks_total=len(chunks), chunks_done=len(metas))
        out = list(pool.map(lambda u: crawl_one(fetch, sellers, u["url_id"], u["url"], stop_at), chunk))
        products, reviews, failed, done_ids, skipped = [], [], [], [], 0
        for u, (prow, rrows, fails, was_skipped) in zip(chunk, out):
            if was_skipped:
                skipped += 1
                continue
            done_ids.append(u["url_id"])
            if prow:
                products.append(prow)
            reviews.extend(rrows)
            failed.extend(fails)

        tag = f"{idx:04d}"
        files = {
            f"products_part_{tag}.csv.gz": (PRODUCT_FIELDS, products),
            f"reviews_part_{tag}.csv.gz": (REVIEW_FIELDS, reviews),
            f"failed_part_{tag}.csv.gz": (FAILED_FIELDS, failed),
        }
        for name, (fields, rows) in files.items():
            path = os.path.join(workdir, name)
            write_csv_gz(path, fields, rows)
            store.put(path, f"parts/{name}")
            os.unlink(path)
        http_after = fetch.snapshot()
        meta = {
            "chunk": idx, "run_id": hb.run_id, "partial": skipped > 0,
            "first_url_id": chunk[0]["url_id"], "last_url_id": chunk[-1]["url_id"],
            "n_urls": len(chunk), "n_done": len(done_ids), "n_skipped": skipped,
            "done_url_ids": done_ids if skipped else None,
            "n_products": len(products), "n_reviews": len(reviews), "n_failed": len(failed),
            "started_at": started, "finished_at": now_vn().isoformat(),
            "http": {k: http_after.get(k, 0) - http_before.get(k, 0) for k in http_after},
        }
        store.put_text(f"parts/chunk_{tag}.json", json.dumps(meta, ensure_ascii=False, indent=1))
        metas[idx] = meta
        hb.update(chunks_done=len(metas), last_chunk=meta)
        logger.info(f"Chunk {tag}: {len(products)} sản phẩm, {len(reviews)} review, "
                    f"{len(failed)} lỗi, bỏ qua {skipped}")
        if skipped:
            break
    return len(metas), len(chunks)


def finalize(store, workdir, chunk_size=CHUNK_SIZE):
    parts = os.path.join(workdir, "final_parts")
    store.get_dir("parts", parts)
    urls_gz = os.path.join(workdir, "product_urls.csv.gz")
    if not store.get("product_urls.csv.gz", urls_gz):
        raise RuntimeError("store chưa có product_urls.csv.gz")
    disc = json.loads(store.get_text("discovery_meta.json") or "{}")

    metas = {}
    for name in sorted(os.listdir(parts)):
        if name.startswith("chunk_") and name.endswith(".json"):
            with open(os.path.join(parts, name), encoding="utf-8") as f:
                m = json.load(f)
            metas[m["chunk"]] = m

    zip_path = os.path.join(workdir, ZIP_NAME)
    stats = {}
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        def merge(arcname, sources, fields, observed_col=None, id_col=None):
            n, lo, hi, ids = 0, None, None, set()
            with zf.open(f"{ZIP_DIR}/{arcname}", "w", force_zip64=True) as raw:
                out = io.TextIOWrapper(raw, encoding="utf-8", newline="")
                w = csv.writer(out)
                w.writerow(fields)
                for src in sources:
                    if not os.path.exists(src):
                        continue
                    with gzip.open(src, "rt", encoding="utf-8", newline="") as f:
                        r = csv.reader(f)
                        header = next(r, None)
                        oi = header.index(observed_col) if observed_col and header else None
                        ii = header.index(id_col) if id_col and header else None
                        for row in r:
                            w.writerow(row)
                            n += 1
                            if oi is not None and row[oi]:
                                lo = row[oi] if lo is None or row[oi] < lo else lo
                                hi = row[oi] if hi is None or row[oi] > hi else hi
                            if ii is not None:
                                ids.add(row[ii])
                out.flush()
                out.detach()
            stats[arcname] = {"rows": n, "observed_at_min": lo, "observed_at_max": hi,
                              "distinct_url_id": len(ids) if id_col else None}

        merge("product_urls.csv", [urls_gz], URL_FIELDS, id_col="url_id")
        order = sorted(metas)
        merge("product_data_changes.csv", [os.path.join(parts, f"products_part_{i:04d}.csv.gz") for i in order],
              PRODUCT_FIELDS, "observed_at", "urlId")
        merge("user_review.csv", [os.path.join(parts, f"reviews_part_{i:04d}.csv.gz") for i in order],
              REVIEW_FIELDS, "observed_at", "url_id")
        merge("failed_urls.csv", [os.path.join(parts, f"failed_part_{i:04d}.csv.gz") for i in order],
              FAILED_FIELDS, id_col="url_id")

        n_urls = stats["product_urls.csv"]["rows"]
        expected = math.ceil(n_urls / chunk_size) if n_urls else 0
        http_total = {}
        for m in metas.values():
            for k, v in (m.get("http") or {}).items():
                http_total[k] = http_total.get(k, 0) + v
        manifest = {
            "generated_at": now_vn().isoformat(),
            "crawl_day": DAY,
            "source": "Tiki JSON API, mapped to the Kafka message format of refresh_crawler.py / comment_crawler.py",
            "files": stats,
            "chunks": {"expected": expected, "done": len(metas),
                       "partial": [i for i in order if metas[i].get("partial")],
                       "missing": [i for i in range(expected) if i not in metas]},
            "urls_not_crawled": sum(m.get("n_skipped", 0) for m in metas.values())
                                + sum(min(chunk_size, n_urls - i * chunk_size) for i in range(expected) if i not in metas),
            "http_status_counts": http_total,
            "discovery": disc,
            "chunk_meta": [metas[i] for i in order],
        }
        zf.writestr(f"{ZIP_DIR}/manifest.json", json.dumps(manifest, ensure_ascii=False, indent=1))
    store.put(zip_path, ZIP_NAME)
    logger.info(f"Đã upload {ZIP_NAME} ({os.path.getsize(zip_path) / 1e6:.1f} MB): "
                f"{json.dumps(stats, ensure_ascii=False)}")
    return manifest


# --------------------------------------------------------------------------- #
# Lệnh
# --------------------------------------------------------------------------- #
def make_fetcher(delay=REQUEST_DELAY):
    return Fetcher(lambda: TikiApi(delay=delay))


def cmd_run(args):
    require_cloud()
    store = make_store(args.store)
    os.makedirs(args.workdir, exist_ok=True)
    if store.exists("DONE"):
        logger.info("Đã có DONE trên store, không làm gì")
        return 0
    run_id = f"{now_vn():%H%M%S}-{uuid.uuid4().hex[:6]}"
    hb = Heartbeat(store, run_id)
    if hb.other_alive():
        return 3

    start, stop_at, _ = day_bounds(DAY)
    now = now_vn()
    if now < start:
        wait = (start - now).total_seconds()
        if wait > 900:
            logger.info(f"Chưa tới {start.isoformat()} (còn {wait / 60:.0f} phút), thoát")
            return 4
        logger.info(f"Chờ {wait:.0f}s tới {start.isoformat()}")
        time.sleep(wait)

    hb.start()
    try:
        fetch = make_fetcher()
        with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="w") as pool:
            if now_vn() < stop_at:
                hb.update(stage="discover")
                urls = load_or_discover(store, fetch, pool, args.workdir, args.n)
                done, total = crawl(store, fetch, pool, urls, args.workdir, stop_at, hb)
            else:
                done, total = len(chunk_metas(store, args.workdir)), None
        if total is not None and done < total and now_vn() < stop_at:
            logger.warning(f"Mới xong {done}/{total} chunk; lần chạy sau sẽ tiếp tục")
            return 2
        hb.update(stage="finalize")
        manifest = finalize(store, args.workdir)
        store.put_text("DONE", json.dumps({"at": now_vn().isoformat(), "run_id": run_id,
                                           "files": manifest["files"], "chunks": manifest["chunks"]},
                                          ensure_ascii=False, indent=1))
        return 0
    finally:
        hb.stop()


def cmd_finalize(args):
    store = make_store(args.store)
    os.makedirs(args.workdir, exist_ok=True)
    m = finalize(store, args.workdir)
    print(json.dumps({"files": m["files"], "chunks": m["chunks"]}, ensure_ascii=False, indent=1))
    return 0


def _shape(x, depth=0):
    if isinstance(x, dict):
        return {k: _shape(v, depth + 1) for k, v in list(x.items())[:40]} if depth < 2 else f"dict[{len(x)}]"
    if isinstance(x, list):
        return [f"list[{len(x)}]", _shape(x[0], depth + 1)] if x else "list[0]"
    return type(x).__name__


def cmd_preflight(args):
    require_cloud()
    fetch = make_fetcher()
    report = {"at": now_vn().isoformat(), "checks": []}

    def check(name, url, params=None, extract=None):
        status, data = fetch.get(url, params)
        item = {"name": name, "status": status}
        if data is not None and extract:
            try:
                item.update(extract(data))
            except Exception as e:     # noqa: BLE001
                item["extract_error"] = repr(e)
        report["checks"].append(item)
        return data

    tree = check("categories_tree", API_CATEGORIES, {"parent_id": ROOT_CATEGORY_ID, "include": "children"},
                 lambda d: dict(zip(("leaves", "inner"), map(len, category_nodes(d)))))
    leaf = category_nodes(tree)[0][0] if tree else (8295, "khu-mui-sap-thom")
    paging = lambda d: {"paging": d.get("paging"), "n_items": len(d.get("data") or []),
                        "item_keys": sorted((d.get("data") or [{}])[0].keys())[:60]}
    check("listing_leaf_urlKey", API_LISTING, {"limit": 40, "category": leaf[0], "page": 1, "urlKey": leaf[1]}, paging)
    check("listing_leaf_no_urlKey", API_LISTING, {"limit": 40, "category": leaf[0], "page": 1}, paging)
    check("listing_root_1883_top_seller", API_LISTING, {"limit": 40, "category": 1883, "page": 1,
                                                        "urlKey": "nha-cua-doi-song", "sort": "top_seller"}, paging)
    check("v2_products_seller_1", API_PRODUCT_LISTING, {"limit": 40, "page": 1, "seller": 1}, paging)
    p = check("product_74245802", API_PRODUCT.format(product_id=74245802), {"platform": "web", "version": 3},
              lambda d: {"name": d.get("name"), "review_count": d.get("review_count")})
    w = check("seller_widget_72479", API_SELLER_WIDGET, {"seller_id": 72479, "platform": "desktop", "version": 3},
              lambda d: {"shape": _shape(((d.get("data") or {}).get("seller")) or d),
                         "mapped": seller_rating_from_widget(d)})
    r = check("reviews_page1", API_REVIEWS, {"limit": 20, "include": "comments,contribute_info,attribute_vote_summary",
                                             "sort": "id|desc", "page": 1, "product_id": 74245802},
              lambda d: {"paging": d.get("paging")})
    if p:
        report["sample_product_row"] = map_product(URL_ID_START, p, seller_rating_from_widget(w), now_vn().isoformat())
        report["sample_product_row"]["description"] = (report["sample_product_row"]["description"] or "")[:200]
    if r and r.get("data"):
        report["sample_review_row"] = map_review(URL_ID_START, r["data"][0], now_vn())
    report["http"] = fetch.snapshot()
    text = json.dumps(report, ensure_ascii=False, indent=1, default=str)
    print(text)
    if args.store:
        store = make_store(args.store)
        store.put_text("preflight.txt", text)
        print(f"preflight.txt -> {store.label}: {'OK' if store.exists('preflight.txt') else 'KHÔNG THẤY'}")
    return 0


class FakeApi:
    """API giả cho selftest: trả fixture theo URL, không gọi mạng."""

    def __init__(self, fixtures):
        def load(name):
            with open(os.path.join(fixtures, name), encoding="utf-8") as f:
                return json.load(f)
        self.cats, self.listing = load("cats.json"), load("listing.json")
        self.product, self.reviews = load("product.json"), load("reviews.json")
        self.calls = 0

    def get(self, url, params=None):
        self.calls += 1
        params = params or {}
        if url == API_CATEGORIES:
            return 200, self.cats
        if url in (API_LISTING, API_PRODUCT_LISTING):
            data = self.listing if int(params.get("page", 1)) <= 2 else {"data": [], "paging": {"last_page": 2}}
            return 200, data
        if url == API_SELLER_WIDGET:
            return 200, {"data": {"seller": {"avg_rating_point": 4.66, "review_count": 5432}}}
        if url == API_REVIEWS:
            page = int(params.get("page", 1))
            if page > 3:
                return 200, {"data": []}
            return 200, dict(self.reviews, paging=dict(self.reviews["paging"], last_page=3))
        if url.startswith("https://tiki.vn/api/v2/products/"):
            pid = int(url.rsplit("/", 1)[-1])
            if pid % 9 == 0:
                return 404, None          # thử nhánh URL lỗi
            return 200, dict(self.product, id=pid)
        return 404, None


def cmd_selftest(args):
    import tempfile
    work = tempfile.mkdtemp(prefix="snapshot_selftest_")
    store = make_store(f"local:{os.path.join(work, 'store')}")
    fetch = Fetcher(lambda: FakeApi(args.fixtures), max_attempts=1)
    n, chunk = 37, 10
    with ThreadPoolExecutor(max_workers=4) as pool:
        urls, meta = discover(fetch, pool, n)
        write_csv_gz(os.path.join(work, "product_urls.csv.gz"), URL_FIELDS, urls)
        store.put(os.path.join(work, "product_urls.csv.gz"), "product_urls.csv.gz")
        store.put_text("discovery_meta.json", json.dumps(meta, ensure_ascii=False))
        hb = Heartbeat(store, "selftest")
        # lần 1 dừng sau 2 chunk (giả lập session chết), lần 2 chạy tiếp
        crawl(store, fetch, pool, urls[:20], work, None, hb, chunk_size=chunk)
        done, total = crawl(store, fetch, pool, urls, work, None, hb, chunk_size=chunk)
    manifest = finalize(store, work, chunk_size=chunk)

    with zipfile.ZipFile(os.path.join(work, ZIP_NAME)) as zf:
        names = zf.namelist()
        with zf.open(f"{ZIP_DIR}/product_data_changes.csv") as f:
            prod = list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")))
        with zf.open(f"{ZIP_DIR}/user_review.csv") as f:
            rev = list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")))
        with zf.open(f"{ZIP_DIR}/product_urls.csv") as f:
            url_rows = list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")))

    assert list(prod[0].keys()) == PRODUCT_FIELDS, prod[0].keys()
    assert list(rev[0].keys()) == REVIEW_FIELDS, rev[0].keys()
    assert len(url_rows) == n and len({r["url"] for r in url_rows}) == n
    assert [int(r["url_id"]) for r in url_rows] == list(range(URL_ID_START, URL_ID_START + n))
    n_404 = sum(1 for r in url_rows if parse_ids_from_url(r["url"])[0] % 9 == 0)
    assert len(prod) == n - n_404, (len(prod), n, n_404)
    assert manifest["files"]["failed_urls.csv"]["rows"] == n_404
    assert len(rev) == len(prod) * 3 * len(FakeApi(args.fixtures).reviews["data"])
    assert len({r["urlId"] for r in prod}) == len(prod)
    assert manifest["chunks"]["missing"] == [] and manifest["chunks"]["done"] == total == math.ceil(n / chunk)

    print("Zip:", names)
    print("Product row:", json.dumps({k: (v[:120] if isinstance(v, str) else v) for k, v in prod[0].items()},
                                     ensure_ascii=False, indent=1))
    print("Review row:", json.dumps(rev[0], ensure_ascii=False, indent=1))
    print(f"OK: {len(url_rows)} URL, {len(prod)} product, {len(rev)} review, {n_404} lỗi 404, "
          f"{manifest['chunks']['done']} chunk -> {work}")
    return 0


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
                        force=True)
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("run", "finalize", "preflight"):
        sp = sub.add_parser(name)
        sp.add_argument("--store", required=(name != "preflight"))
        sp.add_argument("--workdir", default=os.path.join(os.getenv("TMPDIR", "/tmp"), "tiki_snapshot"))
        sp.add_argument("--workers", type=int, default=WORKERS)
        sp.add_argument("--n", type=int, default=N_URLS)
    st = sub.add_parser("selftest")
    st.add_argument("--fixtures", required=True)
    args = ap.parse_args()
    handler = {"run": cmd_run, "finalize": cmd_finalize, "preflight": cmd_preflight, "selftest": cmd_selftest}
    sys.exit(handler[args.cmd](args))


if __name__ == "__main__":
    main()
