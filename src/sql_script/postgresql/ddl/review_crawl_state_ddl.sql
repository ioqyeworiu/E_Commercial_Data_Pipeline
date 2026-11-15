-- Trạng thái cào review tăng dần của api_review_crawler.py (1 dòng / URL trong crawl_state).
-- Crawler tự chạy câu lệnh này lúc khởi động (CREATE ... IF NOT EXISTS) và tự thêm URL mới;
-- giữ đồng bộ với STATE_DDL trong src/python/crawler/api_review_crawler.py.
CREATE TABLE IF NOT EXISTS public.review_crawl_state
(
    url_id                 INTEGER PRIMARY KEY REFERENCES public.crawl_state (id) ON DELETE CASCADE,
    last_review_created_at TIMESTAMPTZ,              -- mốc: created_at lớn nhất đã được Kafka ack; NULL = chưa backfill
    last_reviews_count     INTEGER,                  -- reviews_count Tiki báo ở lần cào thành công gần nhất
    crawl_interval         INTEGER   DEFAULT 60,     -- phút; x2 khi không có review mới, /2 khi có (30..1440)
    next_crawled_at        TIMESTAMP DEFAULT NOW(),
    last_crawled_at        TIMESTAMP,
    last_success_at        TIMESTAMP,
    consecutive_no_new     INTEGER   DEFAULT 0,
    consecutive_errors     INTEGER   DEFAULT 0,
    last_http_status       VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_review_crawl_next ON public.review_crawl_state (next_crawled_at);
