CREATE DATABASE crawler;

CREATE SCHEMA public;

CREATE TABLE public.crawl_state
(
    id                    INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url                   TEXT UNIQUE NOT NULL,
    status                VARCHAR(20) DEFAULT 'ACTIVE',
    last_crawled_at       TIMESTAMP   DEFAULT NOW(),
    next_crawled_at       TIMESTAMP   DEFAULT (NOW() + 1 * INTERVAL '2 minute'),
    crawl_interval        INTEGER     DEFAULT 2,
    consecutive_no_change INTEGER     DEFAULT 0,
    consecutive_errors    INTEGER     DEFAULT 0,
    last_http_status      VARCHAR(20) DEFAULT '200'
);

CREATE INDEX idx_status_crawl ON public.crawl_state (status, next_crawled_at);
CREATE INDEX idx_next_crawl ON public.crawl_state (next_crawled_at);

