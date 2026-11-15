"""
Phần dùng chung cho 2 crawler API v2 của Tiki:
    api_product_crawler.py : sản phẩm, người bán, thương hiệu, danh mục, biến thể, khuyến mãi
    api_review_crawler.py  : review + người dùng (cào tăng dần)

Gồm: HTTP client gọi JSON API (retry, giãn cách request), cache thực thể, Kafka sink
(tự tạo topic nếu thiếu), sink trong RAM cho chế độ --test-url, ghi metrics CSV và
vòng lặp claim -> cào -> gửi dùng chung.
"""
import csv
import json
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
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
logger = logging.getLogger("tiki_api_common")

zone_info = ZoneInfo("Asia/Ho_Chi_Minh")

SCHEMA_VERSION = "v2"
SOURCE = "tiki_api"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "data_crawled_v2")
KAFKA_TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "6"))
KAFKA_TOPIC_REPLICATION = int(os.getenv("KAFKA_TOPIC_REPLICATION", "1"))
MAX_MESSAGE_BYTES = 10_485_760     # khớp KAFKA_MESSAGE_MAX_BYTES của broker
MESSAGE_SOFT_LIMIT = 9_000_000     # vượt mức này thì crawler phải tự chia nhỏ message

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
ENTITY_CACHE_TTL_SEC = int(os.getenv("ENTITY_CACHE_TTL_SEC", "3600"))
EMPTY_BATCH_SLEEP = 3
POLL_INTERVAL = 0.1

# --------------------------------------------------------------------------- #
# Endpoint (bắt được từ network log của trang sản phẩm)
# --------------------------------------------------------------------------- #
# trang sản phẩm
API_PRODUCT = "https://tiki.vn/api/v2/products/{product_id}"
API_REVIEWS = "https://tiki.vn/api/v2/reviews"
API_SELLER_WIDGET = "https://api.tiki.vn/product-detail/v2/widgets/seller"   # chỉ cần seller_id
API_BRAND = "https://tiki.vn/api/v2/brands/{brand_id}"
API_DELIVERY = "https://tiki.vn/api/v2/products/widget/delivery_info/{spid}"
API_COUPON = "https://api.tiki.vn/raiden/v2/asa/seller/coupon"
API_BUNDLE = "https://tiki.vn/api/v2/products/widget/bundle/{product_id}"
API_REVIEW_ANALYSIS = "https://api.tiki.vn/falcon/ext/v1/review-analysis"
API_CATEGORY = "https://tiki.vn/api/v2/categories/{category_id}"
# trang cửa hàng (seller) / trang thương hiệu
API_STORE = "https://tiki.vn/api/v2/seller/stores/{store_slug}"
API_SELLER_CATEGORIES = "https://api.tiki.vn/seller-store-v2/categories"
API_SELLER_COUPONS = "https://api.tiki.vn/tequila/v1/consumer/sellers/{seller_id}/coupons"
API_SELLER_FOLLOWING = "https://api.tiki.vn/social/openapi/interaction/following"
API_PRODUCT_LISTING = "https://tiki.vn/api/v2/products"   # ?seller= / ?brand= & limit=1 -> paging.total

PRODUCT_ID_RE = re.compile(r"-p(\d+)\.html")
SPID_RE = re.compile(r"[?&]spid=(\d+)")
HTML_TAG_RE = re.compile(r"<[^>]+>")


# --------------------------------------------------------------------------- #
# Tiện ích
# --------------------------------------------------------------------------- #
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


def absolute_url(value):
    """'//host/...' (thiếu giao thức) -> 'https://host/...'; rỗng -> None."""
    if not isinstance(value, str) or not value.strip():
        return None
    value = value.strip()
    return "https:" + value if value.startswith("//") else value


def to_int(value):
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def parse_ids_from_url(url: str):
    """Trả về (product_id, spid | None) từ URL dạng ...-p276177979.html[?spid=...]."""
    m = PRODUCT_ID_RE.search(url)
    if not m:
        raise ValueError(f"không tìm thấy product_id trong URL: {url}")
    s = SPID_RE.search(url)
    return int(m.group(1)), (int(s.group(1)) if s else None)


def encode_json(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def write_test_output(messages: list, out_path: str):
    """1 message -> ghi đúng 1 object (giống value trên Kafka); nhiều message -> ghi list."""
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(messages[0] if len(messages) == 1 else messages, f, ensure_ascii=False, indent=2)


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
    """Cache dùng chung giữa các thread cho dữ liệu ít đổi (seller, store, brand, id sản phẩm)."""

    def __init__(self, ttl_sec=ENTITY_CACHE_TTL_SEC):
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
# Sink: nơi crawler gửi message
#   send(key, value, headers, on_delivery) ; on_delivery(err, partition, offset)
# --------------------------------------------------------------------------- #
def ensure_topic(topic=KAFKA_TOPIC, broker=KAFKA_BROKER,
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


class KafkaSink:
    def __init__(self, client_prefix, topic=KAFKA_TOPIC):
        from confluent_kafka import Producer

        ensure_topic(topic)
        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BROKER,
            "client.id": f"{client_prefix}-{os.getpid()}-{uuid4().hex[:8]}",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "compression.type": "snappy",
            "message.max.bytes": MAX_MESSAGE_BYTES,
            "queue.buffering.max.kbytes": 1_048_576,
        })
        self._stop = threading.Event()
        self._poll_thread = threading.Thread(target=self._poll_loop, name="kafka-poll", daemon=True)
        self._poll_thread.start()

    def _poll_loop(self):
        # poll liên tục để callback được xử lý ngay khi broker ack
        while not self._stop.is_set():
            try:
                self.producer.poll(POLL_INTERVAL)
            except Exception as e:
                logger.error(f"Error occurred while polling Kafka: {e}")

    def send(self, key, value: bytes, headers: dict, on_delivery):
        def callback(err, msg):
            if err is not None:
                on_delivery(err, None, None)
            else:
                on_delivery(None, msg.partition(), msg.offset())

        kwargs = dict(topic=self.topic, key=str(key).encode("utf-8"), value=value,
                      headers=headers, callback=callback)
        try:
            self.producer.produce(**kwargs)
        except BufferError:
            logger.warning(f"Producer queue đầy, chờ (key {key})")
            self.producer.poll(1)
            self.producer.produce(**kwargs)

    def close(self):
        logger.info("Flushing producer queue...")
        self._stop.set()
        self._poll_thread.join(timeout=5)
        remaining = self.producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(f"Còn {remaining} message chưa gửi được")


class MemorySink:
    """Cho --test-url: giữ message trong RAM, coi như giao thành công ngay lập tức."""

    def __init__(self):
        self.messages = []
        self._lock = threading.Lock()

    def send(self, key, value: bytes, headers: dict, on_delivery):
        with self._lock:
            self.messages.append(json.loads(value))
            offset = len(self.messages) - 1
        on_delivery(None, None, offset)

    def close(self):
        pass


class MetricWriter:
    def __init__(self, path, fields):
        self.path = path
        self.fields = fields
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    def write(self, row: dict):
        with self.lock:
            exists = os.path.exists(self.path)
            with open(self.path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.fields, extrasaction="ignore")
                if not exists:
                    writer.writeheader()
                writer.writerow(row)


# --------------------------------------------------------------------------- #
# Khung crawler: claim URL từ Postgres -> cào song song -> gửi sink
# --------------------------------------------------------------------------- #
class ApiCrawlerBase:
    def __init__(self, sink, db_engine=None, metrics: MetricWriter | None = None,
                 workers=WORKERS, batch_size=BATCH_SIZE):
        self.sink = sink
        self.db_engine = db_engine          # None ở chế độ --test-url
        self.metrics = metrics
        self.workers = workers
        self.batch_size = batch_size
        self.cache = TTLCache(ENTITY_CACHE_TTL_SEC)
        self.local = threading.local()
        self.stop_event = threading.Event()

    # --- được lớp con override ---
    def claim(self, batch_size) -> list:
        raise NotImplementedError

    def process(self, row) -> None:
        raise NotImplementedError

    def before_batch(self) -> None:
        pass

    # --- tiện ích ---
    def api(self) -> TikiApi:
        if not hasattr(self.local, "api"):
            self.local.api = TikiApi()
        return self.local.api

    def execute(self, sql, params=None, fetch=False):
        from sqlalchemy import text

        with self.db_engine.begin() as conn:
            result = conn.execute(text(sql), params or {})
            return result.fetchall() if fetch else None

    def write_metric(self, row):
        if self.metrics is not None:
            self.metrics.write(row)

    def _safe_process(self, row):
        if self.stop_event.is_set():
            return
        try:
            self.process(row)
        except Exception:
            logger.exception(f"Lỗi không mong muốn với row {row}")

    def run(self):
        try:
            with ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="worker") as pool:
                while not self.stop_event.is_set():
                    self.before_batch()
                    rows = self.claim(self.batch_size)
                    if not rows:
                        logger.info(f"Không còn URL đến hạn, nghỉ {EMPTY_BATCH_SLEEP}s")
                        self.stop_event.wait(EMPTY_BATCH_SLEEP)
                        continue
                    list(pool.map(self._safe_process, rows))   # chờ hết batch rồi mới claim tiếp
        finally:
            self.sink.close()

    def stop(self, *_):
        logger.info("Nhận tín hiệu dừng, hoàn tất batch hiện tại rồi thoát...")
        self.stop_event.set()
