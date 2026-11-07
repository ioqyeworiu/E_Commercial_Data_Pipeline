from confluent_kafka import Producer
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException
from base_crawler import BaseCrawler, logger
from sqlalchemy import create_engine, text
from functools import partial
from threading import Thread
from multiprocessing import Pool, Process, Lock
import threading
import psutil
import json
import os
from dotenv import load_dotenv
from time import sleep, monotonic
from datetime import datetime
from zoneinfo import ZoneInfo
from uuid import uuid4
import csv

time_zone = ZoneInfo("Asia/Ho_Chi_Minh")

load_dotenv(override=True)

KAFKA_BROKER = os.getenv("KAFKA_BROKER") #kafka-1:9092
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")  #product_data_changes

POSTGRES_URL = os.getenv("POSTGRES_URL")

DRIVER_PATH = os.getenv("DRIVER_PATH")
USER_PROFILE_DIR = os.getenv("USER_PROFILE_DIR")
USER_AGENT = os.getenv("USER_AGENT") 

CACHE_CLEAR_INTERVAL = 25         # cứ mỗi 25 URL thì dọn cache (nếu chưa tới lúc restart)
RESTART_INTERVAL = 200             # cứ mỗi 50 URL thì restart driver, bất kể RAM
MAX_DRIVER_MEMORY_MB = 1200       # hoặc RAM vượt ngưỡng này thì restart sớm hơn
DEFAULT_BATCH_SIZE = 500
EMPTY_BATCH_SLEEP = 3             # nghỉ khi không còn URL nào cần crawl
POLL_INTERVAL = 0.1               # timeout cho producer.poll() trong poll thread

MAX_RESIZE_ITERATIONS = 6      # tránh resize vô hạn nếu trang lỗi/vô cực
RESIZE_BUFFER_PX = 300         # dư ra để chắc chắn phần cuối trang cũng "nhìn thấy"
DEFAULT_WINDOW_HEIGHT = 1080   # kích thước mặc định, quay lại sau khi crawl xong

COMMENT_CRAWLER_METRIC_FILE = os.getenv("COMMENT_CRAWLER_METRIC_FILE")
METRIC_FIELDS = ["url_id", "crawling_start_at", "observed_at", "crawling_end_at", "produced_at", "kafka_received_at", "kafka_partition", "kafka_offset", "error_message"]

def create_kafka_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": f"comment-crawler-{os.getpid()}-{uuid4().hex[:8]}",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 10,
        "compression.type": "snappy",
        "queue.buffering.max.messages": 500_000,
        "queue.buffering.max.kbytes": 1_048_576,
    })

def get_url(db_engine, batch_size):
    with db_engine.begin() as conn:
        result = conn.execute(
            text("""
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
            """),
            {"batch_size": batch_size},
        )
        rows = result.fetchall()
    return rows

def get_driver_memory_mb(driver) -> float:
    """
    Tính tổng RSS (MB) của cả cây tiến trình driver: msedgedriver.exe
    (driver.service.process) + toàn bộ msedge.exe con của nó. Đây là con số
    phản ánh đúng RAM mà browser đang thực sự chiếm, không phải RAM của
    tiến trình Python chính.
    """
    if driver is None:
        return 0.0
    try:
        driver_pid = driver.service.process.pid
    except Exception:
        return 0.0

    try:
        parent = psutil.Process(driver_pid)
    except psutil.NoSuchProcess:
        return 0.0

    total = 0
    try:
        total += parent.memory_info().rss
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    try:
        for child in parent.children(recursive=True):
            try:
                total += child.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except psutil.NoSuchProcess:
        pass

    return total / (1024 * 1024)

def clear_driver_cache(driver) -> None:
    """
    Dọn cache, cookies, local/session storage của driver hiện tại thông qua
    Chrome DevTools Protocol (CDP) - hoạt động với cả Edge (Chromium-based).
    Nhẹ hơn nhiều so với restart hẳn driver.

    Lưu ý: chỉ dọn được HTTP/disk cache + cookies (toàn cục) và
    localStorage/sessionStorage của ORIGIN đang mở (không xoá được storage
    của mọi domain đã từng ghé qua trong session). RAM do tab/DOM/JS heap
    chiếm giữ thì CDP không có cách giải phóng trực tiếp - phần đó chỉ có
    restart driver mới xử lý được triệt để.
    """
    if driver is None:
        return

    try:
        driver.execute_cdp_cmd("Network.clearBrowserCache", {})
        driver.execute_cdp_cmd("Network.clearBrowserCookies", {})
        logger.info("Đã dọn browser cache + cookies (CDP)")
    except Exception:
        logger.exception("error when clearing cache/cookies via CDP")

    try:
        driver.execute_script("window.localStorage.clear(); window.sessionStorage.clear();")
    except Exception:
        logger.exception("error when clearing localStorage/sessionStorage")

def maintain_driver_health(crawler, urls_since_restart: int) -> int:
    """
    Gọi sau mỗi URL đã crawl. Trả về giá trị counter mới (0 nếu vừa restart
    thành công, giữ nguyên nếu chưa tới mốc kiểm tra hoặc restart thất bại).

    Chỉ thực sự kiểm tra/hành động tại các mốc bội số của
    CACHE_CLEAR_INTERVAL (25), để tránh overhead quét tiến trình (psutil)
    sau mỗi URL:
        - Mốc 25, 75, 125...            -> dọn cache (nếu RAM chưa vượt ngưỡng)
        - Mốc 50, 100, 150... hoặc bất
          kỳ mốc 25 nào mà RAM đã vượt   -> restart hẳn driver
    """
    if urls_since_restart % CACHE_CLEAR_INTERVAL != 0:
        return urls_since_restart

    mem_mb = get_driver_memory_mb(crawler.driver)
    logger.info(
        f"Health check: crawled {urls_since_restart} URL with restart before "
        f"Current RAM: {mem_mb:.1f} MB"
    )

    need_restart = urls_since_restart >= RESTART_INTERVAL or mem_mb > MAX_DRIVER_MEMORY_MB

    if need_restart:
        reason = (
            f"Reached {urls_since_restart} URL with restart before"
            if urls_since_restart >= RESTART_INTERVAL
            else f"RAM exceeded ({mem_mb:.1f} MB > {MAX_DRIVER_MEMORY_MB} MB)"
        )
        logger.warning(f"Restart driver do {reason}")
        try:
            crawler.restart_driver()
        except Exception:
            logger.exception("Restart driver failed, retry later")
            return urls_since_restart  # giữ nguyên counter, thử lại ở checkpoint kế tiếp
        return 0  # reset counter sau khi restart thành công

    logger.info(f"Clear cache (after {urls_since_restart} URL)")
    clear_driver_cache(crawler.driver)
    return urls_since_restart

# --------------------------------------------------------------------------- #
# Crawler chính
# --------------------------------------------------------------------------- #
class CommentCrawler(BaseCrawler):
    def __init__(self, kafka_producer, db_engine, metric_writer_lock, **kwargs):
        super().__init__(**kwargs)
        self.producer = kafka_producer
        self.db_engine = db_engine
        self.metric_writer_lock = metric_writer_lock
        
    def _write_metric(self, row: dict):
            with self.metric_writer_lock:
                file_exists = os.path.exists(COMMENT_CRAWLER_METRIC_FILE)
                with open(COMMENT_CRAWLER_METRIC_FILE, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=METRIC_FIELDS)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(row)

    def _poll_loop(self, stop_event):
        # Chạy liên tục trong suốt vòng đời crawler, không chờ queue đầy
        # mới poll -> callback được xử lý gần như ngay khi broker ack.
        while not stop_event.is_set():
            try:
                self.producer.poll(POLL_INTERVAL)
            except Exception as e:
                logger.error(f"Error occurred while polling Kafka: {e}")

    def _crawl_one(self, url_id, url) -> str | None:
        try:
            crawling_start_at = datetime.now(time_zone) ###################
            self.driver.get(url)
            logger.info(f"Crawling review on URL {url_id}: {url}")
            observed_at = datetime.now(time_zone)   ###################
            while True:
                # Cuộn xuống từng đoạn ngắn 700 pixel
                sleep(0.15) # Dừng lại để chờ Tiki gọi API và render HTML
                self.driver.execute_script("window.scrollBy(0, 700);")
                
                # Kiểm tra tọa độ hiện tại so với chiều cao tối đa của trang
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                current_position = self.driver.execute_script("return window.pageYOffset + window.innerHeight")
                
                # Dừng lại khi cuộn chạm đáy
                if current_position >= new_height:
                    break
            try:
                records = []
                has_next_page = True
                while has_next_page:
                    sleep(0.2)
                    try:
                        customer_reviews = self.driver.find_elements(By.XPATH, "//div[@class='sc-4b1322bd-5 hcsJQG review-comment']")
                        for customer_review in customer_reviews:
                            record = {}
                            try:
                                record["url_id"] = url_id
                                record["user_name"] = customer_review.find_element(By.XPATH, ".//div[@class='review-comment__user-name']").text
                                record["joined_duration"] = customer_review.find_element(By.XPATH, ".//div[@class='review-comment__user-date']").text
                                # print(record["user_name"], record["joined_duration"])
                            except NoSuchElementException as e:
                                # print(f"{e} + loi o user name or joined duration")
                                # print("No user name or joined duration")
                                record["user_name"] = ""
                                record["joined_duration"] = ""
                            except StaleElementReferenceException as e:
                                # WebDriverWait(driver, 10).until(EC.staleness_of(customer_reviews))
                                break
                            try:
                                for user_info in customer_review.find_elements(By.XPATH, ".//div[@class='review-comment__user-info']"):
                                    record.update({f"{user_info.find_element(By.XPATH, ".//div").text}":user_info.find_element(By.XPATH, ".//span").text})
                                    # print(user_info.find_element(By.XPATH, ".//div").text, user_info.find_element(By.XPATH, ".//span").text)
                                # print(record["Đã viết"], record["Đã nhận"])
                            except NoSuchElementException as e:
                                # print(f"{e} + loi o user info")
                                # print("No user info")
                                record["Đã viết"] = ""
                                record["Đã nhận"] = ""  
                            except StaleElementReferenceException as e:
                                # WebDriverWait(driver, 10).until(EC.staleness_of(customer_reviews))
                                break  
                            try:
                                star_score = customer_review.find_element(By.XPATH, ".//div[@class='sc-a236768f-0 fFhahK review-comment__rating']")
                                star_score = [star.get_attribute("src") for star in star_score.find_elements(By.XPATH, ".//img")[:5]].count("https://salt.tikicdn.com/ts/upload/e3/f0/86/efd76e1d41c00ad8ebb7287c66b559fd.png")
                                record["star_score"] = star_score
                                record["status"] = customer_review.find_element(By.XPATH, ".//div[@class='review-comment__title']").text
                            except NoSuchElementException as e:
                                # print(f"{e} + loi o star score or status")
                                # print("No star score or status")
                                record["star_score"] = ""
                                record["status"] = ""
                            except StaleElementReferenceException as e:
                                # WebDriverWait(driver, 10).until(EC.staleness_of(customer_reviews))
                                break
                            # print(record["star_score"], record["status"])
                            try:
                                comment = customer_review.find_element(By.XPATH, ".//div[@class='review-comment__content']")
                                more_button = comment.find_element(By.XPATH, ".//span[@class='show-more-content']")
                                more_button.click()
                                comment = comment.text
                                record["comment"] = comment
                            except NoSuchElementException as e:
                                # print(f"{e} + loi o show more button on comment")
                                # print("No show_more button on comment")
                                record["comment"] = comment.text
                            except StaleElementReferenceException as e:
                                # WebDriverWait(driver, 10).until(EC.staleness_of(customer_reviews))
                                break
                            # print(record["comment"])
                            try:
                                images_urls = customer_review.find_element(By.XPATH, ".//div[@class='review-comment__images']")
                                images_urls = [img.get_attribute("style") for img in images_urls.find_elements(By.XPATH, ".//div")]
                                record["images_urls"] = images_urls
                            # print(record["images_urls"])
                            except NoSuchElementException as e:
                                # print(f"{e} + loi o images_urls")
                                # print("No images_urls")
                                record["images_urls"] = []
                            except StaleElementReferenceException as e:
                                # WebDriverWait(driver, 10).until(EC.staleness_of(customer_reviews))
                                break
                            try:
                                additional_info = customer_review.find_element(By.XPATH, ".//div[@class='review-comment__created-date']")
                                additional_info = additional_info.find_elements(By.XPATH, ".//span")
                                rate_time = additional_info[-2].text
                                record["rate_time"] = rate_time
                                # print(record["rate_time"])
                                use_time = additional_info[-1].text
                                record["use_time"] = use_time
                                # print(record["use_time"])
                                prod_properties = additional_info[:-2]
                                record["prod_properties"] = [prop.text for prop in prod_properties]
                                # print(record["prod_properties"])
                            except NoSuchElementException as e:
                                # print(f"{e} + loi o additional_info")
                                # print("No additional_info")
                                record["rate_time"] = ""
                                record["use_time"] = ""
                                record["prod_properties"] = []
                            except StaleElementReferenceException as e:
                                # WebDriverWait(driver, 10).until(EC.staleness_of(customer_reviews))
                                break
                            record["observed_at"] = observed_at.isoformat()
                            records.append(record)
                        try:
                            review_pagination = self.driver.find_element(By.XPATH, ".//div[@class='customer-reviews__pagination']")
                            next_button = review_pagination.find_element(By.XPATH, ".//a[@class='btn next']")
                            svg_element = next_button.find_element(By.CSS_SELECTOR, "svg")
                            # next_page_button = next_page_button.find_elements(By.XPATH, ".//li")[-1]
                            next_button_color = svg_element.get_attribute("color")
                            if next_button_color == "#C4C4CF":
                                print("No next page button")
                                has_next_page = False
                            else:
                                self.driver.execute_script("arguments[0].scrollIntoView();", next_button)
                                next_button.click()
                        except NoSuchElementException as e:
                            # print("No next page button")
                            # print(f"{e} + loi o next page button")
                            has_next_page = False
                        except StaleElementReferenceException as e:
                            review_pagination = self.driver.find_element(By.XPATH, ".//div[@class='customer-reviews__pagination']")
                            next_button = review_pagination.find_element(By.XPATH, ".//a[@class='btn next']")
                            svg_element = next_button.find_element(By.CSS_SELECTOR, "svg")
                            next_button_color = svg_element.get_attribute("color")
                            if next_button_color == "#C4C4CF":
                                # print("No next page button")
                                has_next_page = False
                            else:
                                self.driver.execute_script("arguments[0].scrollIntoView();", next_button)
                                next_button.click()
                            # WebDriverWait(driver, 10).until(EC.staleness_of(customer_reviews))
                    except NoSuchElementException as e:
                        # print("No customer reviews")
                        # print(f"{e} + loi o customer reviews")
                        break
                    except StaleElementReferenceException as e:
                        #WebDriverWait(driver, 10).until(EC.staleness_of(customer_reviews))
                        break                
            except Exception as e:
                print(url)
            finally:
                crawling_end_at = datetime.now(time_zone)
                metric_row = {
                    "url_id": url_id,
                    "crawling_start_at": crawling_start_at.isoformat(),
                    "observed_at": observed_at.isoformat(),
                    "crawling_end_at": crawling_end_at.isoformat(),
                }
                return records, metric_row
        except Exception as e:
            logger.error(f"Error occurred while fetching URL {url} url_id{url_id}: {e}")
            return None, None
        
    def delivery_report(self, err, msg, url_id, metric_row):
        if err is not None:
            logger.error(f"URL ID {url_id}: delivery failed: {err}")
            metric_row["kafka_partition"] = None
            metric_row["kafka_offset"] = None
            metric_row["error_message"] = str(err)
        else:
            metric_row["kafka_partition"] = msg.partition()
            metric_row["kafka_offset"] = msg.offset()
            metric_row["error_message"] = None 
            logger.info(f"Data on URL {url_id} to topic [{msg.topic()}] | partition [{msg.partition()}] | offset [{msg.offset()}]")
        self._write_metric(metric_row)
    
    def _send_to_kafka(self, url_id, records: list, metric_row: dict) -> None:
        cb = partial(self.delivery_report, url_id=url_id, metric_row=metric_row)
        for record in records:
            metric_row["produced_at"] = datetime.now(time_zone).isoformat()
            message = json.dumps(record, ensure_ascii=False).encode("utf-8")
            try:
                self.producer.produce(
                    topic=KAFKA_TOPIC,
                    key=str(url_id).encode('utf-8'),
                    value=message,
                    callback=cb,
                )
            except BufferError:
                logger.warning(f"Local producer queue is full, waiting for space (URL ID {url_id})")
                self.producer.poll(0.5)
                self.producer.produce(
                    topic=KAFKA_TOPIC,
                    key=str(url_id).encode('utf-8'),
                    value=message,
                    callback=cb,
                )

    def run(self, *args) -> None:
        stop_event = threading.Event()
        poll_thread = Thread(target=self._poll_loop, args=(stop_event,), daemon=True)
        poll_thread.start()

        urls_since_restart = 0  # biến cục bộ, chỉ sống trong vòng đời run()

        try:
            while True:
                rows = get_url(self.db_engine, batch_size=args[0] if args else DEFAULT_BATCH_SIZE)
                if not rows:
                    logger.info("No product to refresh, sleeping for 3 seconds...")
                    sleep(EMPTY_BATCH_SLEEP)
                    continue

                for row in rows:
                    url_id, url = row[0], row[1]

                    records, metric_row = self._crawl_one(url_id, url)

                    urls_since_restart += 1
                    urls_since_restart = maintain_driver_health(self, urls_since_restart)

                    if records is None:
                        continue

                    self._send_to_kafka(url_id, records, metric_row)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received, stopping crawler...")
        finally:
            logger.info("Flushing producer queue and stopping crawler...")
            stop_event.set()
            poll_thread.join(timeout=5)
            remaining = self.producer.flush(timeout=30)
            if remaining > 0:
                logger.warning("remaining %d not sent", remaining)

def run_comment_crawler(batch_size, profile_name, metric_writer_lock):
    db_engine = create_engine((POSTGRES_URL), pool_pre_ping=True)
    kafka_producer = create_kafka_producer()
    crawler = CommentCrawler(
        metric_writer_lock=metric_writer_lock,
        kafka_producer=kafka_producer,
        db_engine=db_engine,
        headless=True,
        driver_path=DRIVER_PATH,
        user_data_dir=USER_PROFILE_DIR,
        cleanup_user_data_dir=True,
        profile_name=profile_name,
        user_agent=USER_AGENT,
    )
    crawler.start()
    try:
        crawler.run(batch_size)
    finally:
        crawler.stop()

if __name__ == "__main__":
    metric_writer_lock = Lock()
    
    p1 = Process(target=run_comment_crawler, args=(DEFAULT_BATCH_SIZE, "comment1", metric_writer_lock))
    p2 = Process(target=run_comment_crawler, args=(DEFAULT_BATCH_SIZE, "comment2", metric_writer_lock))
    # p3 = Process(target=run_comment_crawler, args=(DEFAULT_BATCH_SIZE, "comment3", metric_writer_lock))
    # p4 = Process(target=run_comment_crawler, args=(DEFAULT_BATCH_SIZE, "comment4", metric_writer_lock))

    p1.start()
    p2.start()
    # p3.start()
    # p4.start()
    try:
        p1.join()
        p2.join()
        # p3.join()
        # p4.join()
    except KeyboardInterrupt:
        logger.info("Main process interrupted, waiting for children to shut down...")
        p1.join()
        p2.join()
        # p3.join()
        # p4.join()