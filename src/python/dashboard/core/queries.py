"""Chạy truy vấn của catalog và các truy vấn meta (giá trị cho bộ lọc).

Mọi tham số đi qua server-side binding ({tên:Kiểu}), không nối chuỗi vào SQL.
Kết quả cache 10 phút; nút "Làm mới dữ liệu" trong app xóa cache.
"""
from __future__ import annotations

import datetime as dt
import time
from typing import Any

import pandas as pd
import streamlit as st

from core.db import get_client
from core.sql_catalog import catalog

CACHE_TTL = 600


def _coerce(value: Any, ch_type: str) -> Any:
    if value is None:
        raise ValueError("thiếu giá trị tham số")
    if ch_type == "Date":
        if isinstance(value, dt.datetime):
            return value.date()
        if isinstance(value, dt.date):
            return value
        return dt.date.fromisoformat(str(value))
    if ch_type.startswith(("Int", "UInt")):
        return int(value)
    if ch_type.startswith("Float"):
        return float(value)
    return str(value)


def _tidy(df: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn hóa kiểu: cột Nullable số (object chứa None) -> float, để Plotly / định dạng số xử lý đồng nhất."""
    for col in df.columns:
        s = df[col]
        if s.dtype == object:
            non_null = s.dropna()
            if len(non_null) and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null):
                df[col] = pd.to_numeric(s, errors="coerce").astype("float64")
    return df


@st.cache_data(ttl=CACHE_TTL, show_spinner=False, max_entries=500)
def _query(sql: str, params: tuple[tuple[str, Any], ...]) -> pd.DataFrame:
    df = get_client().query_df(sql, parameters=dict(params), use_extended_dtypes=False)
    return _tidy(df)


def run(qid: str, flt: dict[str, Any]) -> pd.DataFrame:
    """Chạy truy vấn [qid] với các bộ lọc hiện tại (chỉ truyền tham số mà câu SQL dùng)."""
    q = catalog()[qid]
    missing = [p for p in q.params if flt.get(p) is None]
    if missing:
        raise KeyError(f"Truy vấn [{qid}] cần tham số {', '.join(missing)}")
    params = tuple(sorted((name, _coerce(flt[name], typ)) for name, typ in q.params.items()))
    return _query(q.sql, params)


def run_timed(qid: str, flt: dict[str, Any]) -> tuple[pd.DataFrame, float]:
    t0 = time.perf_counter()
    df = run(qid, flt)
    return df, time.perf_counter() - t0


def clear_cache() -> None:
    _query.clear()
    for fn in (date_bounds, mart_updated_at, category_options, sale_events, sale_dates, search_products, product_title):
        fn.clear()


# ---------------------------------------------------------------------------------------------------
# Truy vấn meta: giá trị cho bộ lọc
# ---------------------------------------------------------------------------------------------------
@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def date_bounds() -> tuple[dt.date, dt.date, int]:
    """Ngày tin cậy đầu / cuối trong mart_kpi_daily và số ngày tin cậy."""
    row = get_client().query(
        "SELECT min(full_date), max(full_date), count() FROM mart_kpi_daily WHERE is_reliable_day"
    ).result_rows[0]
    return row[0], row[1], int(row[2])


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def mart_updated_at() -> dt.datetime | None:
    """Lần nạp mart gần nhất (mart_kpi_daily do loader 01 nạp đầu tiên)."""
    try:
        return get_client().query("SELECT max(updated_at) FROM mart_kpi_daily").result_rows[0][0]
    except Exception:
        return None


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def category_options() -> list[str]:
    """Danh mục cấp 1 xếp theo doanh thu toàn kỳ. Thứ tự này cũng quyết định màu cố định của danh mục."""
    rows = get_client().query(
        """
        SELECT category_level_1, sum(revenue_est) AS revenue
        FROM mart_category_daily
        WHERE is_reliable_day
        GROUP BY category_level_1
        ORDER BY revenue DESC, category_level_1
        """
    ).result_rows
    return [r[0] for r in rows]


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def sale_events() -> list[str]:
    """Các ngày sale có trong mart_sale_event_product, mới nhất trước."""
    rows = get_client().query(
        "SELECT sale_event, max(event_date) AS d FROM mart_sale_event_product GROUP BY sale_event ORDER BY d DESC"
    ).result_rows
    return [r[0] for r in rows]


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def sale_dates() -> dict[dt.date, str]:
    """Ngày sale -> tên sự kiện, để đánh dấu trên các biểu đồ theo ngày."""
    rows = get_client().query(
        "SELECT full_date, sale_event FROM mart_kpi_daily WHERE sale_event != '' ORDER BY full_date"
    ).result_rows
    return {r[0]: r[1] for r in rows}


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def search_products(text: str, category_l1: str = "", limit: int = 50) -> pd.DataFrame:
    """Tìm sản phẩm theo một phần tiêu đề hoặc đúng product_id, xếp theo doanh thu."""
    return get_client().query_df(
        """
        SELECT product_id, title, category_level_1, brand_name, revenue_total, revenue_rank
        FROM mart_product_scorecard
        WHERE ({q:String} = ''
               OR positionCaseInsensitiveUTF8(title, {q:String}) > 0
               OR toString(product_id) = {q:String})
          AND ({c:String} = '' OR category_level_1 = {c:String})
        ORDER BY revenue_total DESC, product_id
        LIMIT {n:UInt32}
        """,
        parameters={"q": text.strip(), "c": category_l1, "n": limit},
        use_extended_dtypes=False,
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def product_title(product_id: int) -> str | None:
    rows = get_client().query(
        "SELECT title FROM mart_product_scorecard WHERE product_id = {p:Int64}",
        parameters={"p": int(product_id)},
    ).result_rows
    return rows[0][0] if rows else None
