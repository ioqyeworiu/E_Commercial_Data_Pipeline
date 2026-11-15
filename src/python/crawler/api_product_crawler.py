"""
Crawler sản phẩm (API v2) cho Tiki.

Mỗi URL đến hạn trong `crawl_state` -> 1 message JSON gom theo thực thể:
product, sellers (người bán chính + mọi người bán khác), brand, categories,
variants, offers, promotions. Review và người dùng do api_review_crawler.py cào riêng.

Message gửi vào KAFKA_TOPIC (mặc định data_crawled_v2), key = url_id,
record_type = "product".

Cách chạy:
    python api_product_crawler.py --test-url <url> [--test-url <url> ...] --out file.json
        -> chỉ gọi API và ghi JSON ra file, KHÔNG đụng Kafka / Postgres
    python api_product_crawler.py --create-topic
        -> tạo topic nếu chưa có rồi thoát
    python api_product_crawler.py
        -> chạy thật

Lưu ý: dùng chung hàng đợi `crawl_state` với refresh/comment crawler và api_crawler.py.
Chạy song song thì các crawler sẽ chia nhau URL.
"""
import argparse
import logging
import os
import signal
from functools import partial
from uuid import uuid4

from tiki_api_common import (
    API_BRAND, API_BUNDLE, API_CATEGORY, API_COUPON, API_DELIVERY, API_PRODUCT,
    API_PRODUCT_LISTING, API_SELLER_CATEGORIES, API_SELLER_COUPONS, API_SELLER_FOLLOWING,
    API_SELLER_WIDGET, API_STORE, POSTGRES_URL, SCHEMA_VERSION, SOURCE, WORKERS,
    ApiCrawlerBase, KafkaSink, MemorySink, MetricWriter, encode_json, ensure_topic,
    now_iso, parse_ids_from_url, strip_html, to_int, write_test_output,
)

logger = logging.getLogger("api_product_crawler")

RECORD_TYPE = "product"
SELLER_LOGO_BASE = "https://vcdn.tikicdn.com/ts/seller/"
METRIC_FILE = os.getenv("API_PRODUCT_CRAWLER_METRIC_FILE", "/app/metrics/api_product_crawler_metrics.csv")
METRIC_FIELDS = [
    "url_id", "product_id", "crawling_start_at", "observed_at", "crawling_end_at",
    "produced_at", "kafka_received_at", "kafka_partition", "kafka_offset",
    "message_bytes", "n_requests", "http_status", "error_message",
]

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


class ProductUnavailable(Exception):
    def __init__(self, status):
        super().__init__(f"product detail API returned {status}")
        self.status = status


LISTING_TOTAL_CAP = 2000       # /api/v2/products?seller=|brand= trả paging.total tối đa 2000
CATEGORY_COUNT_CAP = 10000     # /api/v2/categories/{id} trả product_count tối đa 10000


def listing_total(listing: dict | None):
    """
    Tổng số sản phẩm từ API listing. paging.total bị chặn ở 2000, nên ưu tiên total_text
    ("592", "10000+"). Trả (số, text gốc, có_phải_cận_dưới).
    """
    paging = (listing or {}).get("paging") or {}
    text = str(paging.get("total_text") or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    total = int(digits) if digits else paging.get("total")
    is_lower_bound = text.endswith("+") or (not text and total == LISTING_TOTAL_CAP)
    return total, (text or None), is_lower_bound


class FetchLog:
    """Gọi API + ghi status từng endpoint của 1 lần cào (để biết thiếu dữ liệu vì đâu)."""

    def __init__(self, api):
        self.api = api
        self.status = {}
        self.n_requests = 0

    def get(self, name, url, params=None):
        status, data = self.api.get(url, params)
        self.n_requests += 1
        self.status[name] = status
        return data

    def cached(self, name):
        self.status[name] = "cache"


# --------------------------------------------------------------------------- #
# Map response API -> thực thể
# --------------------------------------------------------------------------- #
def build_product(p: dict, spid, seller_id, leaf_category_id) -> dict:
    stock = p.get("stock_item") or {}
    inventory = p.get("inventory") or {}
    quantity_sold = p.get("quantity_sold") or {}
    primary_category = p.get("categories") or {}
    return_policy = p.get("return_policy") or {}
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
        "url_key": p.get("url_key"),
        "url": f"https://tiki.vn/{p['url_path']}" if p.get("url_path") else None,
        "short_url": p.get("short_url"),
        "brand_id": (p.get("brand") or {}).get("id"),
        "seller_id": seller_id,
        "leaf_category_id": leaf_category_id,
        "primary_category": {
            "category_id": primary_category.get("id"),
            "name": primary_category.get("name"),
            "is_leaf": primary_category.get("is_leaf"),
        } if primary_category else None,
        "authors": [
            {"author_id": a.get("id"), "name": a.get("name"), "slug": a.get("slug")}
            for a in p.get("authors") or []
        ],
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
        "product_virtual_type": inventory.get("product_virtual_type"),
        "stock_qty": stock.get("qty"),
        "min_sale_qty": stock.get("min_sale_qty"),
        "max_sale_qty": stock.get("max_sale_qty"),
        "preorder_date": stock.get("preorder_date"),
        "day_ago_created": p.get("day_ago_created"),
        "data_version": p.get("data_version"),
        "short_description": p.get("short_description"),
        "highlights": (p.get("highlight") or {}).get("items") or [],
        "description_html": p.get("description"),
        "thumbnail_url": p.get("thumbnail_url"),
        "image_urls": [img.get("base_url") for img in p.get("images") or [] if img.get("base_url")],
        "video_url": p.get("video_url"),
        "specifications": specs,
        "warranty_info": [
            {"name": w.get("name"), "value": w.get("value")} for w in p.get("warranty_info") or []
        ],
        "return_and_exchange_policy": strip_html(p.get("return_and_exchange_policy")),
        "return_policy": {
            "title": return_policy.get("title"),
            "items": [c for b in return_policy.get("body") or [] for c in b.get("content") or []],
            "link": (return_policy.get("cta") or {}).get("link"),
        } if return_policy else None,
        "benefits": [strip_html(b.get("text")) for b in p.get("benefits") or []],
        "price_comparison": p.get("price_comparison"),   # vd "So sánh 7 nhà bán khác", "Giá từ 160.000 ₫"
        "deal_specs": p.get("deal_specs"),
        "badges": [b.get("code") for b in p.get("badges_new") or [] if b.get("code")],
        "flags": ((p.get("tracking_info") or {}).get("amplitude")) or {},
    }


def build_category(crumb: dict, info: dict | None, position: int) -> dict:
    """Gộp breadcrumb (thứ tự hiển thị) với API danh mục (parent, level, số sản phẩm...)."""
    info = info or {}
    url = crumb.get("url") or ""
    return {
        "category_id": crumb.get("category_id"),
        "name": info.get("name") or crumb.get("name"),
        "url": f"https://tiki.vn{url}" if url.startswith("/") else (url or None),
        "url_key": info.get("url_key"),
        "breadcrumb_position": position,        # 1 = gốc hiển thị trên breadcrumb
        "level": info.get("level"),             # level theo Tiki (Root = 1)
        "parent_id": info.get("parent_id"),
        "is_leaf": info.get("is_leaf"),
        "product_count": info.get("product_count"),
        "product_count_is_lower_bound": (info.get("product_count") or 0) >= CATEGORY_COUNT_CAP,
        "status": info.get("status"),
        "include_in_menu": info.get("include_in_menu"),
        "thumbnail_url": info.get("thumbnail_url"),
    }


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
                "thumbnail_url": c.get("thumbnail_url"),
            }
            for c in p.get("configurable_products") or []
        ],
    }


def build_brand(p: dict, brand_page: dict | None, total, fetched_at) -> dict | None:
    """total = kết quả listing_total(...)."""
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
        "total_products": total[0],
        "total_products_text": total[1],
        "total_products_is_lower_bound": total[2],
        "brand_page_id": brand_page.get("id"),
        "brand_page_url": f"https://tiki.vn/{brand_page['slug']}" if brand_page.get("slug") else None,
        "thumbnail_url": brand_page.get("thumbnail_url") or None,
        "description": brand_page.get("description") or None,
        "fetched_at": fetched_at,
    }


def build_coupon(c: dict) -> dict:
    return {
        "coupon_id": c.get("coupon_id"),
        "coupon_code": c.get("coupon_code"),
        "rule_id": c.get("rule_id"),
        "label": c.get("label"),
        "short_title": c.get("short_title"),
        "short_description": c.get("short_description"),
        "long_description": c.get("long_description"),
        "coupon_type": c.get("coupon_type"),
        "simple_action": c.get("simple_action"),
        "discount_amount": c.get("discount_amount"),
        "discount_max": c.get("discount_max"),
        "min_amount": c.get("min_amount"),
        "period": c.get("period"),
        "expired_at": c.get("expired_at"),
        "status": c.get("status"),
        "out_of_stock": c.get("out_of_stock"),
        "tags": c.get("tags") or [],
        "seller_id": c.get("seller_id"),
    }


def seller_slug(link):
    return link.rstrip("/").rsplit("/", 1)[-1] if link else None


def build_seller(seller_id, base: dict, widget: dict | None, store: dict | None, total,
                 categories: list | None, coupons: list | None, following: dict | None, fetched_at) -> dict:
    """base = current_seller hoặc 1 phần tử other_sellers của API chi tiết sản phẩm;
    total = kết quả listing_total(...)."""
    widget = widget or {}
    store = store or {}
    link = base.get("link") or widget.get("url")
    logo = widget.get("icon") or base.get("logo")
    if logo and not logo.startswith("http"):
        logo = SELLER_LOGO_BASE + logo.lstrip("/")     # other_sellers trả logo dạng đường dẫn tương đối
    total_follower = (following or {}).get("total_follower")
    return {
        "seller_id": seller_id,
        "name": base.get("name") or widget.get("name"),
        "store_id": base.get("store_id") or widget.get("store_id") or store.get("id"),
        "store_slug": seller_slug(link),
        "url": link,
        "logo_url": logo or None,
        "is_official": widget.get("is_official"),
        "store_level": widget.get("store_level"),
        "avg_rating_point": widget.get("avg_rating_point"),
        "review_count": widget.get("review_count"),
        "total_follower": total_follower if total_follower is not None else widget.get("total_follower"),
        "days_since_joined": widget.get("days_since_joined"),
        "total_products": total[0],
        "total_products_text": total[1],
        "total_products_is_lower_bound": total[2],
        "store_live_at": store.get("live_at"),
        "store_name": store.get("name"),
        "store_description": store.get("description"),
        "store_rating_summary": store.get("rating_summary"),
        "categories": [
            {"category_id": c.get("cate_id"), "name": c.get("name"), "parent_id": c.get("parent_id"),
             "level": c.get("level"), "path": c.get("path"), "active": c.get("active")}
            for c in categories or []
        ],
        "coupons": [build_coupon(c) for c in coupons or []],
        "fetched_at": fetched_at,
    }


def build_offers(p: dict, current_seller: dict) -> list:
    """Giá bán của từng người bán cho sản phẩm này (người bán chính + other_sellers)."""
    offers = [{
        "seller_id": current_seller.get("id"),
        "seller_name": current_seller.get("name"),
        "spid": to_int(current_seller.get("product_id")),   # API trả string
        "sku": current_seller.get("sku"),
        "price": current_seller.get("price"),
        "store_id": current_seller.get("store_id"),
        "is_best_store": current_seller.get("is_best_store"),
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
            "is_best_store": s.get("is_best_store"),
            "is_current": False,
        })
    return offers


def build_promotions(p: dict, coupon: dict | None, delivery: dict | None, bundle: dict | None) -> dict:
    coupon = coupon or {}
    normal = coupon.get("coupon_normal") or {}
    asa = ((coupon.get("coupon_asa") or {}).get("coupons") or {}).get("items") or []
    return {
        "coupon_text": normal.get("text"),
        "coupons": [build_coupon(c) for c in normal.get("data") or []],
        "asa_coupons": [build_coupon(c) for c in asa],
        "installment_offers": [
            {"title": i.get("title"), "summary": i.get("summary"), "type": i.get("type")}
            for i in p.get("installment_info_v3") or []
        ],
        "bundle_items": [   # "mua kèm": sản phẩm Tiki gợi ý mua cùng
            {
                "product_id": b.get("id"),
                "name": b.get("name"),
                "price": b.get("price"),
                "list_price": b.get("list_price"),
                "original_price": b.get("original_price"),
                "bundle_price": b.get("bundle_price"),
                "discount_rate": b.get("discount_rate"),
                "url": f"https://tiki.vn/{b['url_path']}" if b.get("url_path") else None,
            }
            for b in (bundle or {}).get("data") or []
        ],
        # Tiki chỉ trả ước tính giao hàng khi biết địa chỉ người xem; không có địa chỉ -> type "ask_location"
        "delivery": {
            "type": delivery.get("type"),
            "title": delivery.get("title"),
            "seller_name": delivery.get("seller_name"),
            "is_shopping_delivery": delivery.get("is_shopping_delivery"),
            "only_ship_to": delivery.get("only_ship_to"),
            "delivery_options": delivery.get("delivery_options"),
        } if delivery else None,
    }


# --------------------------------------------------------------------------- #
# Crawler
# --------------------------------------------------------------------------- #
class ApiProductCrawler(ApiCrawlerBase):

    def _seller(self, log: FetchLog, seller_id, base: dict) -> dict:
        """Thông tin đầy đủ 1 người bán, ghép từ 6 API; cache theo seller_id."""
        key = ("seller", seller_id)
        hit = self.cache.get(key)
        if hit is not None:
            log.cached(f"seller:{seller_id}")
            return hit
        prefix = f"seller:{seller_id}:"
        widget = ((log.get(prefix + "widget", API_SELLER_WIDGET,
                           {"seller_id": seller_id, "platform": "desktop", "version": 3})
                   or {}).get("data") or {}).get("seller")
        slug = seller_slug(base.get("link") or (widget or {}).get("url"))
        store = log.get(prefix + "store", API_STORE.format(store_slug=slug)) if slug else None
        listing = log.get(prefix + "product_count", API_PRODUCT_LISTING,
                          {"seller": seller_id, "limit": 1, "page": 1})
        categories = (log.get(prefix + "categories", API_SELLER_CATEGORIES, {"seller_id": seller_id})
                      or {}).get("data")
        coupons = (log.get(prefix + "coupons", API_SELLER_COUPONS.format(seller_id=seller_id)) or {}).get("data")
        following = ((log.get(prefix + "following", API_SELLER_FOLLOWING, {"tiki_seller_id": seller_id})
                      or {}).get("data") or {}).get("following")
        entity = build_seller(
            seller_id, base, widget, store, listing_total(listing),
            categories, coupons, following, now_iso(),
        )
        if widget or store:          # chỉ cache khi lấy được phần lõi
            self.cache.put(key, entity)
        return entity

    def _brand(self, log: FetchLog, p: dict):
        brand_id = (p.get("brand") or {}).get("id")
        if not brand_id:
            return None
        key = ("brand", brand_id)
        hit = self.cache.get(key)
        if hit is None:
            page = log.get("brand", API_BRAND.format(brand_id=brand_id))
            listing = log.get("brand_product_count", API_PRODUCT_LISTING, {"brand": brand_id, "limit": 1, "page": 1})
            hit = {"page": page, "total": listing_total(listing), "fetched_at": now_iso()}
            if page is not None:
                self.cache.put(key, hit)
        else:
            log.cached("brand")
        return build_brand(p, hit["page"], hit["total"], hit["fetched_at"])

    def _categories(self, log: FetchLog, p: dict) -> list:
        crumbs = [c for c in p.get("breadcrumbs") or [] if c.get("category_id")]   # phần tử cuối = chính sản phẩm
        out = []
        for position, crumb in enumerate(crumbs, start=1):
            cid = crumb["category_id"]
            key = ("category", cid)
            info = self.cache.get(key)
            if info is None:
                info = log.get(f"category:{cid}", API_CATEGORY.format(category_id=cid))
                if info is not None:
                    self.cache.put(key, info)
            else:
                log.cached(f"category:{cid}")
            out.append(build_category(crumb, info, position))
        return out

    def fetch(self, url_id, url) -> dict:
        log = FetchLog(self.api())
        crawl_started_at = now_iso()
        product_id, spid_hint = parse_ids_from_url(url)

        params = {"platform": "web", "version": 3}
        if spid_hint:
            params["spid"] = spid_hint
        p = log.get("product", API_PRODUCT.format(product_id=product_id), params)
        observed_at = now_iso()      # thời điểm "nhìn thấy" sản phẩm -> time semantics downstream
        if p is None or not p.get("id"):
            raise ProductUnavailable(log.status["product"])

        current_seller = p.get("current_seller") or {}
        spid = int(current_seller.get("product_id") or spid_hint or p["id"])
        seller_id = current_seller.get("id")
        categories = self._categories(log, p)
        leaf_category_id = categories[-1]["category_id"] if categories else None

        # --- mọi người bán của sản phẩm (chính + other_sellers) ---
        sellers = []
        bases = ([current_seller] if seller_id else []) + list(p.get("other_sellers") or [])
        seen = set()
        for base in bases:
            sid = base.get("id")
            if not sid or sid in seen:
                continue
            seen.add(sid)
            entity = dict(self._seller(log, sid, base))
            entity["is_current_seller"] = sid == seller_id
            sellers.append(entity)

        brand = self._brand(log, p)

        # --- giao hàng, coupon, mua kèm (đổi theo thời gian, không cache) ---
        delivery = log.get("delivery", API_DELIVERY.format(spid=spid), {"platform": "web", "pdp": "v2"})
        coupon = None
        if seller_id:
            coupon = log.get("coupon", API_COUPON, {"platform": "desktop", "pid": spid, "seller_id": seller_id})
        bundle = log.get("bundle", API_BUNDLE.format(product_id=p["id"]), {"seller_id": seller_id or ""})

        return {
            "schema_version": SCHEMA_VERSION,
            "record_type": RECORD_TYPE,
            "source": SOURCE,
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
                "brand_id": (p.get("brand") or {}).get("id"),
                "leaf_category_id": leaf_category_id,
            },
            "product": build_product(p, spid, seller_id, leaf_category_id),
            "sellers": sellers,
            "brand": brand,
            "categories": categories,
            "variants": build_variants(p),
            "offers": build_offers(p, current_seller),
            "promotions": build_promotions(p, coupon, delivery, bundle),
            "crawl_meta": {
                "n_requests": log.n_requests,
                "fetch_status": log.status,
            },
        }

    # --- Postgres ---
    def claim(self, batch_size):
        return self.execute(CLAIM_SQL, {"batch_size": batch_size}, fetch=True)

    def _update_status(self, url_id, http_status, ok):
        if self.db_engine is None:
            return
        try:
            self.execute(STATUS_SQL, {"id": url_id, "http_status": str(http_status)[:20], "ok": ok})
        except Exception:
            logger.exception(f"URL ID {url_id}: cập nhật crawl_state thất bại")

    # --- xử lý 1 URL ---
    def process(self, row):
        self.process_url(row[0], row[1])

    def process_url(self, url_id, url):
        metric_row = {"url_id": url_id, "crawling_start_at": now_iso()}
        try:
            record = self.fetch(url_id, url)
        except ProductUnavailable as e:
            metric_row.update(crawling_end_at=now_iso(), http_status=e.status, error_message=str(e))
            self.write_metric(metric_row)
            self._update_status(url_id, e.status, ok=False)
            logger.warning(f"URL ID {url_id}: {e}")
            return None
        except Exception as e:
            metric_row.update(crawling_end_at=now_iso(), http_status="ERR", error_message=repr(e)[:500])
            self.write_metric(metric_row)
            self._update_status(url_id, "ERR", ok=False)
            logger.exception(f"URL ID {url_id}: lỗi khi cào {url}")
            return None

        message = encode_json(record)
        metric_row.update(
            product_id=record["ids"]["product_id"],
            observed_at=record["observed_at"],
            crawling_end_at=record["crawl_finished_at"],
            message_bytes=len(message),
            n_requests=record["crawl_meta"]["n_requests"],
            http_status=200,
            produced_at=now_iso(),
        )
        self.sink.send(
            url_id, message,
            {"schema_version": SCHEMA_VERSION, "record_type": RECORD_TYPE, "source": SOURCE},
            partial(self._on_delivery, metric_row),
        )
        return record

    def _on_delivery(self, metric_row, err, partition, offset):
        url_id = metric_row["url_id"]
        metric_row["kafka_received_at"] = now_iso()
        if err is not None:
            logger.error(f"URL ID {url_id}: delivery failed: {err}")
            metric_row["error_message"] = str(err)
            self._update_status(url_id, "KAFKA_ERR", ok=False)
        else:
            metric_row["kafka_partition"] = partition
            metric_row["kafka_offset"] = offset
            self._update_status(url_id, 200, ok=True)
            if partition is not None:
                logger.info(f"URL ID {url_id} -> partition {partition} offset {offset}")
        self.write_metric(metric_row)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def run_test(urls, out_path):
    sink = MemorySink()
    crawler = ApiProductCrawler(sink=sink)
    for i, url in enumerate(urls, start=1):
        record = crawler.process_url(url_id=-i, url=url)   # url_id âm = bản thử, không có trong crawl_state
        if record:
            meta = record["crawl_meta"]
            failed = {k: v for k, v in meta["fetch_status"].items() if v not in (200, "cache")}
            logger.info(f"OK product_id={record['ids']['product_id']} | "
                        f"{len(encode_json(record)) / 1024:.1f} KB | {meta['n_requests']} request | "
                        f"{len(record['sellers'])} seller | endpoint lỗi: {failed or 'không'}")
    write_test_output(sink.messages, out_path)
    logger.info(f"Đã ghi {len(sink.messages)} message -> {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Tiki API product crawler (v2) -> Kafka")
    parser.add_argument("--test-url", action="append", help="chạy thử với URL này, ghi ra file, không gửi Kafka")
    parser.add_argument("--out", default="api_product_record_sample.json", help="file JSON đầu ra cho --test-url")
    parser.add_argument("--create-topic", action="store_true", help="chỉ tạo topic nếu chưa có rồi thoát")
    args = parser.parse_args()

    if args.test_url:
        run_test(args.test_url, args.out)
        return
    if args.create_topic:
        ensure_topic()
        return

    from sqlalchemy import create_engine

    crawler = ApiProductCrawler(
        sink=KafkaSink("api-product-crawler"),
        db_engine=create_engine(POSTGRES_URL, pool_pre_ping=True, pool_size=WORKERS + 2),
        metrics=MetricWriter(METRIC_FILE, METRIC_FIELDS),
    )
    signal.signal(signal.SIGTERM, crawler.stop)
    signal.signal(signal.SIGINT, crawler.stop)
    logger.info(f"Product crawler start: workers={crawler.workers}, batch={crawler.batch_size}")
    crawler.run()


if __name__ == "__main__":
    main()
