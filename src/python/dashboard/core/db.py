"""Kết nối ClickHouse (HTTP, clickhouse-connect).

Cấu hình theo thứ tự ưu tiên:
1. Biến môi trường CLICKHOUSE_HOST / CLICKHOUSE_PORT / CLICKHOUSE_DB
2. User / mật khẩu: CLICKHOUSE_USER_FILE / CLICKHOUSE_PASSWORD_FILE (docker secrets, khi chạy trong container)
   -> CLICKHOUSE_USER / CLICKHOUSE_PASSWORD (biến môi trường hoặc file .env ở gốc dự án, khi chạy trên host)
   -> docker_secrets/clickhouse_admin_*.txt ở gốc dự án
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import clickhouse_connect
import streamlit as st

_HERE = Path(__file__).resolve()
# src/python/dashboard/core -> gốc dự án (khi chạy trên host). Trong container (/app/core) không có gốc dự án.
PROJECT_ROOT = _HERE.parents[4] if len(_HERE.parents) > 4 else _HERE.parent


def _load_dotenv() -> None:
    env_file = PROJECT_ROOT / ".env"
    if env_file.is_file():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file, override=False)   # biến môi trường của container luôn thắng
        except ImportError:
            pass


def _secret(file_var: str, env_var: str, fallback_file: str, default: str = "") -> str:
    path = os.getenv(file_var)
    if path and Path(path).is_file():
        return Path(path).read_text(encoding="utf-8").strip()
    if os.getenv(env_var):
        return os.environ[env_var].strip()
    local = PROJECT_ROOT / "docker_secrets" / fallback_file
    if local.is_file():
        return local.read_text(encoding="utf-8").strip()
    return default


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    database: str
    user: str
    password: str


def settings() -> Settings:
    _load_dotenv()
    return Settings(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        database=os.getenv("CLICKHOUSE_DB", "ecommercial_warehouse"),
        user=_secret("CLICKHOUSE_USER_FILE", "CLICKHOUSE_USER", "clickhouse_admin_username.txt", "default"),
        password=_secret("CLICKHOUSE_PASSWORD_FILE", "CLICKHOUSE_PASSWORD", "clickhouse_admin_password.txt"),
    )


def make_client():
    s = settings()
    return clickhouse_connect.get_client(
        host=s.host,
        port=s.port,
        database=s.database,
        username=s.user,
        password=s.password,
        # Nhiều phiên Streamlit dùng chung 1 client -> không gắn session_id, để truy vấn song song không bị
        # "session is locked"
        autogenerate_session_id=False,
        connect_timeout=5,
        send_receive_timeout=120,
        settings={"max_execution_time": 60},
    )


@st.cache_resource(show_spinner=False)
def get_client():
    return make_client()
