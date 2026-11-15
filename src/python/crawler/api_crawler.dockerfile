FROM python:3.11-slim

# Không cần Edge/msedgedriver: crawler gọi thẳng JSON API của Tiki
ENV PYTHONUNBUFFERED=1 \
    TZ=Asia/Ho_Chi_Minh

WORKDIR /app

COPY api_crawler.requirements.txt .
RUN pip install --no-cache-dir -r api_crawler.requirements.txt

COPY api_crawler.py ./

ENV KAFKA_BROKER=kafka-1:9092 \
    KAFKA_TOPIC=data_crawled_v2 \
    KAFKA_TOPIC_PARTITIONS=6 \
    API_CRAWLER_METRIC_FILE=/app/metrics/api_crawler_metrics.csv

RUN mkdir -p /app/metrics /app/out

CMD ["python", "api_crawler.py"]
