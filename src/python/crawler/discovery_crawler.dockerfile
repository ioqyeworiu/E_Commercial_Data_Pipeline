FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Ho_Chi_Minh

# 1. Cài Microsoft Edge (stable) + công cụ cần thiết
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl gnupg ca-certificates unzip fonts-liberation fonts-noto-core tzdata \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/edge stable main" \
        > /etc/apt/sources.list.d/microsoft-edge.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends microsoft-edge-stable \
    && rm -rf /var/lib/apt/lists/*

# 2. Cài msedgedriver ĐÚNG phiên bản với Edge vừa cài
RUN EDGE_VERSION=$(microsoft-edge --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+') \
    && echo "Edge version: ${EDGE_VERSION}" \
    && curl -fsSL -o /tmp/edgedriver.zip \
        "https://msedgedriver.microsoft.com/${EDGE_VERSION}/edgedriver_linux64.zip" \
    && unzip /tmp/edgedriver.zip msedgedriver -d /usr/local/bin \
    && chmod +x /usr/local/bin/msedgedriver \
    && rm /tmp/edgedriver.zip

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY base_crawler.py discovery_crawler.py ./

ENV DRIVER_PATH=/usr/local/bin/msedgedriver \
    USER_PROFILE_DIR=/tmp/selenium_user_data \
    PROFILE_NAME=discovery \
    HEADLESS=true \
    CLEANUP_USER_DATA_DIR=true \
    USER_AGENT="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"

RUN mkdir -p ${USER_PROFILE_DIR}

CMD ["python", "discovery_crawler.py"]