"""Bộ lọc dùng chung, giữ giá trị khi chuyển trang (persist_state="session").

Khóa session_state:
    f_dates    (date_from, date_to)      khoảng ngày đang xem
    f_preset   "7 ngày" | "14 ngày" | ...  preset đang chọn (None khi chọn tay)
    f_cat      ''  hoặc tên danh mục cấp 1
    f_event    tên ngày sale ('11.11') cho [4.5]-[4.7]
    f_product  product_id cho [6.4]-[6.8]
"""
from __future__ import annotations

import datetime as dt
from typing import Any

import streamlit as st

from core.components import fmt_num
from core.queries import category_options, date_bounds, product_title, sale_events, search_products

PRESETS = {"7 ngày": 7, "14 ngày": 14, "30 ngày": 30, "Toàn kỳ": None}
DEFAULT_PRESET = "14 ngày"
ss = st.session_state


def _preset_range(name: str) -> tuple[dt.date, dt.date]:
    d_min, d_max, _ = date_bounds()
    days = PRESETS[name]
    if days is None:
        return d_min, d_max
    return max(d_min, d_max - dt.timedelta(days=days - 1)), d_max


def init_state() -> None:
    if "f_dates" not in ss:
        ss.f_dates = _preset_range(DEFAULT_PRESET)
        ss.f_preset = DEFAULT_PRESET
        ss._dates_ok = ss.f_dates
    if "f_cat" not in ss:
        ss.f_cat = ""
    if "f_event" not in ss:
        events = sale_events()
        ss.f_event = events[0] if events else ""
    if "f_product" not in ss:
        top = search_products("", "", 1)
        ss.f_product = int(top.product_id.iloc[0]) if len(top) else None


def _on_preset() -> None:
    if ss.get("f_preset"):
        ss.f_dates = _preset_range(ss.f_preset)


def _on_dates() -> None:
    ss.f_preset = None


def current() -> dict[str, Any]:
    """Giá trị bộ lọc hiện tại, đặt theo tên tham số của SQL ({date_from:Date}, ...)."""
    init_state()
    dates = ss.get("f_dates") or ss.get("_dates_ok")
    if not isinstance(dates, (tuple, list)) or len(dates) != 2:
        dates = ss.get("_dates_ok") or _preset_range(DEFAULT_PRESET)   # đang chọn dở (mới chọn 1 ngày)
    else:
        ss._dates_ok = tuple(dates)
    return {
        "date_from": dates[0],
        "date_to": dates[1],
        "category_l1": ss.get("f_cat", "") or "",
        "sale_event": ss.get("f_event", ""),
        "product_id": ss.get("f_product"),
    }


def filter_bar(*, dates: bool = True, category: bool = True, note: str | None = None) -> dict[str, Any]:
    """Một hàng bộ lọc phía trên mọi biểu đồ của trang. Trả về dict tham số cho truy vấn."""
    init_state()
    d_min, d_max, _ = date_bounds()
    with st.container(border=True):
        cols = st.columns([1.5, 1.3, 1.5], vertical_alignment="bottom") if dates else \
            st.columns([1.6, 2.4], vertical_alignment="bottom")
        i = 0
        if dates:
            cols[0].segmented_control("Khoảng thời gian", list(PRESETS), key="f_preset", on_change=_on_preset,
                                      persist_state="session")
            # value=[]: chế độ khoảng ngày, giá trị thật nằm ở session_state (preset ghi vào đó)
            cols[1].date_input("Từ ngày – đến ngày", value=[], key="f_dates", min_value=d_min, max_value=d_max,
                               format="DD/MM/YYYY", on_change=_on_dates, persist_state="session")
            i = 2
        if category:
            opts = [""] + category_options()
            if ss.get("f_cat") not in opts:
                ss.f_cat = ""
            cols[i].selectbox("Danh mục cấp 1", opts, key="f_cat", persist_state="session",
                              format_func=lambda v: "Tất cả danh mục" if v == "" else v)
        else:
            cols[i].markdown(f"<div class='note'>{note or 'Trang này tính trên toàn sàn.'}</div>",
                             unsafe_allow_html=True)
        flt = current()
        if dates:
            n = (flt["date_to"] - flt["date_from"]).days + 1
            p_to = flt["date_from"] - dt.timedelta(days=1)
            p_from = p_to - dt.timedelta(days=n - 1)
            extra = f" · {note}" if note and category else ""
            st.caption(
                f"Kỳ đang xem: {flt['date_from']:%d/%m/%Y} – {flt['date_to']:%d/%m/%Y} ({fmt_num(n)} ngày) · "
                f"kỳ so sánh: {p_from:%d/%m} – {p_to:%d/%m} · chỉ tính ngày crawl đủ (is_reliable_day){extra}"
            )
        elif note and category:
            st.caption(note)
    return flt


# ---------------------------------------------------------------------------------------------------
# Drill-down / drill-through
# ---------------------------------------------------------------------------------------------------
def drill_category(table_key: str, names: list[str]) -> None:
    """Callback của bảng: chọn 1 dòng danh mục cấp 1 -> lọc theo danh mục đó."""
    rows = ss[table_key].selection.rows
    if rows:
        ss.f_cat = names[rows[0]]


def clear_category() -> None:
    ss.f_cat = ""


def pick_product(table_key: str, ids: list[int], goto: bool = False) -> None:
    """Callback của bảng sản phẩm: chọn 1 dòng -> mở chi tiết sản phẩm đó."""
    rows = ss[table_key].selection.rows
    if rows:
        ss.f_product = int(ids[rows[0]])
        ss.f_product_q = ""
        if goto:
            ss._goto_product = True


def goto_product_if_requested() -> None:
    if ss.pop("_goto_product", False):
        st.switch_page("views/6_product.py")


def event_picker() -> str:
    events = sale_events()
    if not events:
        return ""
    if ss.get("f_event") not in events:
        ss.f_event = events[0]
    st.segmented_control("Ngày sale", events, key="f_event", persist_state="session", required=True)
    return ss.f_event


def product_picker() -> int | None:
    """Ô tìm (một phần tên hoặc product_id) + danh sách 50 kết quả theo doanh thu."""
    init_state()
    c1, c2 = st.columns([1, 2], vertical_alignment="bottom")
    q = c1.text_input("Tìm sản phẩm", key="f_product_q", placeholder="Một phần tên, hoặc product_id",
                      persist_state="session")
    res = search_products(q or "", "", 50)
    ids = [int(x) for x in res.product_id]
    titles = dict(zip(ids, res.title))
    cur = ss.get("f_product")
    if cur is not None and cur not in titles:
        title = product_title(int(cur))
        if title is not None:
            ids = [int(cur)] + ids
            titles[int(cur)] = title
    if not ids:
        c2.warning("Không tìm thấy sản phẩm phù hợp.")
        return cur
    if ss.get("f_product") not in ids:
        ss.f_product = ids[0]
    c2.selectbox("Sản phẩm", ids, key="f_product", persist_state="session",
                 format_func=lambda p: f"{p} · {titles.get(p, '')[:90]}")
    return ss.f_product
