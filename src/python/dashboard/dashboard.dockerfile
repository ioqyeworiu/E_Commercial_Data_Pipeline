# Dashboard Streamlit đọc data mart ClickHouse.
# Build context = src/python/dashboard (nhỏ, không kéo docker_mount/ vào).
# Code (/app) và SQL (/sql) được mount read-only từ docker-compose.yml, nên sửa code / SQL không cần rebuild ;
# COPY bên dưới chỉ để image vẫn chạy được khi không mount.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
