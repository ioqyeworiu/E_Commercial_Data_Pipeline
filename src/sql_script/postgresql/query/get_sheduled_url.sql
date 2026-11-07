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
