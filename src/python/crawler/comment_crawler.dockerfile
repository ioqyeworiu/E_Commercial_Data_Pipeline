# Python >= 3.12 
FROM --platform=linux/amd64 python:3.12-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Ho_Chi_Minh

# 1) Microsoft Edge (stable) + tini (dọn zombie process của msedge) + tzdata (cho ZoneInfo)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg unzip tini tzdata fonts-liberation \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-edge.gpg \
 && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-edge.gpg] https://packages.microsoft.com/repos/edge stable main" \
      > /etc/apt/sources.list.d/microsoft-edge.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends microsoft-edge-stable \
 && rm -rf /var/lib/apt/lists/*

# 2) msedgedriver đúng phiên bản với Edge vừa cài
RUN EDGE_VERSION="$(microsoft-edge-stable --version | grep -oP '\d+\.\d+\.\d+\.\d+')" \
 && echo "Edge version: ${EDGE_VERSION}" \
 && curl -fsSL -o /tmp/edgedriver.zip \
      "https://msedgedriver.microsoft.com/${EDGE_VERSION}/edgedriver_linux64.zip" \
 && unzip -j /tmp/edgedriver.zip msedgedriver -d /usr/local/bin \
 && chmod +x /usr/local/bin/msedgedriver \
 && rm /tmp/edgedriver.zip \
 && msedgedriver --version

WORKDIR /app

# 3) Python deps (copy requirements trước để tận dụng layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4) Source
COPY base_crawler.py comment_crawler.py ./

ENV DRIVER_PATH=/usr/local/bin/msedgedriver \
    USER_PROFILE_DIR=/tmp/selenium_user_data \
    USER_AGENT="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0" \
    POSTGRES_URL=postgresql+psycopg2://postgres:postgres@postgres:5432/postgres \
    KAFKA_BROKER=kafka-1:9092 \
    KAFKA_TOPIC=user_review \
    COMMENT_CRAWLER_METRIC_FILE=/app/metrics/comment_crawler_metrics.csv

RUN mkdir -p /app/metrics ${USER_PROFILE_DIR}

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "comment_crawler.py"]