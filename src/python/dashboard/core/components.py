"""Thành phần giao diện dùng chung: định dạng số, card biểu đồ, bảng, thẻ KPI, CSS."""
from __future__ import annotations

import html
import math
from contextlib import contextmanager
from typing import Any, Callable, Iterator

import pandas as pd
import streamlit as st

from core import theme
from core.queries import run
from core.sql_catalog import catalog

DASH = "—"


# ---------------------------------------------------------------------------------------------------
# Định dạng số kiểu Việt Nam (1.234.567 ; 12,5%)
# ---------------------------------------------------------------------------------------------------
_SWAP = str.maketrans({",": ".", ".": ","})


def missing(x: Any) -> bool:
    if x is None:
        return True
    try:
        return bool(pd.isna(x))
    except (TypeError, ValueError):
        return False


def fmt_num(x: Any, d: int = 0) -> str:
    if missing(x):
        return DASH
    return f"{float(x):,.{d}f}".translate(_SWAP)


def fmt_pct(x: Any, d: int = 1, signed: bool = False) -> str:
    if missing(x):
        return DASH
    s = f"{float(x):+,.{d}f}" if signed else f"{float(x):,.{d}f}"
    return s.translate(_SWAP) + "%"


MONEY_UNITS = ((1e12, "nghìn tỷ"), (1e9, "tỷ"), (1e6, "tr"), (1e3, "k"))


def fmt_vnd(x: Any, d: int | None = None) -> str:
    """Số tiền rút gọn: 6,63 nghìn tỷ ₫ · 1,25 tỷ ₫ · 3,4 tr ₫ · 125.000 ₫."""
    if missing(x):
        return DASH
    x = float(x)
    for factor, unit in MONEY_UNITS[:3]:
        if abs(x) >= factor:
            v = x / factor
            digits = d if d is not None else (2 if abs(v) < 100 else 1)
            return f"{fmt_num(v, digits)} {unit} ₫"
    return f"{fmt_num(x, 0)} ₫"


def fmt_count(x: Any) -> str:
    """Số đếm lớn rút gọn: 12,3 tr · 456,7 k · 980."""
    if missing(x):
        return DASH
    x = float(x)
    if abs(x) >= 1e9:
        return f"{fmt_num(x / 1e9, 2)} tỷ"
    if abs(x) >= 1e6:
        return f"{fmt_num(x / 1e6, 2)} tr"
    if abs(x) >= 1e4:
        return f"{fmt_num(x / 1e3, 1)} k"
    return fmt_num(x, 0)


def money_scale(values) -> tuple[float, str]:
    """Hệ số chia + đơn vị cho trục tiền tệ, theo giá trị lớn nhất."""
    try:
        m = float(pd.Series(values).abs().max())
    except Exception:
        m = 0.0
    if not m or math.isnan(m):
        return 1.0, "₫"
    for factor, unit in ((1e12, "nghìn tỷ ₫"), (1e9, "tỷ ₫"), (1e6, "triệu ₫"), (1e3, "nghìn ₫")):
        if m >= factor * 0.95:
            return factor, unit
    return 1.0, "₫"


# ---------------------------------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------------------------------
def inject_css() -> None:
    t = theme.tokens()
    st.markdown(
        f"""
        <style>
        .block-container {{ padding-top: 2.2rem; padding-bottom: 3rem; max-width: 1480px; }}
        h1.page-title {{ font-size: 1.65rem; font-weight: 700; letter-spacing: -0.01em; margin: 0 0 .15rem 0; padding: 0; color: {t.ink}; }}
        p.page-intro {{ color: {t.ink2}; font-size: .95rem; margin: 0 0 .4rem 0; max-width: 980px; }}
        .card-head {{ display: flex; align-items: baseline; gap: .55rem; margin-bottom: .1rem; }}
        .card-head .qid {{ font-size: .72rem; font-weight: 600; color: {t.muted}; border: 1px solid {t.border};
                           border-radius: 6px; padding: 1px 6px; font-variant-numeric: tabular-nums; white-space: nowrap; }}
        .card-head .card-title {{ font-size: 1.02rem; font-weight: 650; color: {t.ink}; line-height: 1.35; }}
        .card-sub {{ color: {t.ink2}; font-size: .86rem; margin: 0 0 .35rem 0; }}
        .section-title {{ font-size: 1.12rem; font-weight: 700; color: {t.ink}; margin: 1.1rem 0 .1rem 0; }}
        .section-sub {{ color: {t.ink2}; font-size: .88rem; margin: 0 0 .5rem 0; }}
        div[class*="st-key-card-"] {{ background: {t.surface}; }}
        div[data-testid="stMetric"] {{ background: {t.surface}; }}
        div[data-testid="stMetricLabel"] p {{ color: {t.ink2}; font-size: .84rem; }}
        div[data-testid="stMetricValue"] {{ font-weight: 650; font-size: 1.55rem; }}
        .pill {{ display: inline-block; font-size: .75rem; font-weight: 600; border-radius: 999px; padding: 1px 9px;
                 margin-right: 4px; border: 1px solid {t.border}; color: {t.ink2}; }}
        .pill.official {{ color: #fff; background: {t.series[0]}; border-color: {t.series[0]}; }}
        .prod-title {{ font-size: 1.25rem; font-weight: 700; color: {t.ink}; margin: .1rem 0 .2rem 0; line-height: 1.35; }}
        .prod-meta {{ color: {t.ink2}; font-size: .9rem; margin-bottom: .2rem; }}
        .note {{ color: {t.muted}; font-size: .8rem; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------------------------------
# Tiêu đề trang / section
# ---------------------------------------------------------------------------------------------------
def page_header(dashboard_no: int, title: str) -> None:
    d = catalog().dashboards.get(dashboard_no)
    intro = d.intro if d else ""
    if "Câu hỏi:" in intro:
        intro = "Câu hỏi: " + intro.split("Câu hỏi:", 1)[1].strip()
    st.markdown(
        f"<h1 class='page-title'>{html.escape(title)}</h1><p class='page-intro'>{html.escape(intro)}</p>",
        unsafe_allow_html=True,
    )


def section(title: str, sub: str | None = None) -> None:
    body = f"<div class='section-title'>{html.escape(title)}</div>"
    if sub:
        body += f"<div class='section-sub'>{html.escape(sub)}</div>"
    st.markdown(body, unsafe_allow_html=True)


# ---------------------------------------------------------------------------------------------------
# Card biểu đồ
# ---------------------------------------------------------------------------------------------------
def _is_control_flow(exc: BaseException) -> bool:
    """st.rerun / st.stop / st.switch_page dùng exception riêng: không được nuốt."""
    return type(exc).__module__.startswith("streamlit") and type(exc).__name__.endswith(("Exception", "Error")) and \
        any(k in type(exc).__name__ for k in ("Rerun", "Stop", "ScriptControl"))


def card_header(qid: str, subtitle: str | None = None) -> None:
    q = catalog()[qid]
    help_parts = []
    if q.notes:
        help_parts.append(q.notes)
    if q.chart_hint:
        help_parts.append(f"Gợi ý biểu đồ trong SQL: {q.chart_hint}")
    help_parts.append(f"Nguồn: `{q.source}` · truy vấn [{q.qid}]")
    st.markdown(
        f"<div class='card-head'><span class='qid'>{q.qid}</span>"
        f"<span class='card-title'>{html.escape(q.title)}</span></div>"
        + (f"<div class='card-sub'>{html.escape(subtitle)}</div>" if subtitle else ""),
        unsafe_allow_html=True,
        help="\n\n".join(help_parts),
    )


@contextmanager
def card(qid: str, flt: dict | None = None, *, subtitle: str | None = None, df: pd.DataFrame | None = None,
         table: bool = True, table_df: pd.DataFrame | None = None, border: bool = True
         ) -> Iterator[pd.DataFrame | None]:
    """Khung một biểu đồ: tiêu đề [x.y] lấy từ SQL, chạy truy vấn, empty state, bảng dữ liệu + CSV.

    with card("1.2", flt) as df:
        if df is not None:
            ...vẽ...
    """
    with st.container(border=border, key=f"card-{qid.replace('.', '-')}" if border else None):
        card_header(qid, subtitle)
        if df is None:
            try:
                df = run(qid, flt or {})
            except Exception as exc:  # lỗi kết nối / SQL: báo trong card, không làm sập cả trang
                st.error(f"Không chạy được truy vấn [{qid}]: {exc}")
                yield None
                return
        if df.empty:
            st.info("Không có dữ liệu cho bộ lọc đang chọn.", icon=":material/info:")
            yield None
            return
        try:
            yield df
        except Exception as exc:
            if _is_control_flow(exc):
                raise
            st.error(f"Lỗi khi vẽ [{qid}]: {exc}")
        if table:
            data_expander(qid, table_df if table_df is not None else df)


def data_expander(qid: str, df: pd.DataFrame) -> None:
    with st.expander("Xem bảng dữ liệu", icon=":material/table:"):
        st.dataframe(df, hide_index=True, width="stretch", height=min(38 + 35 * len(df), 380))
        st.download_button(
            "Tải CSV", df.to_csv(index=False).encode("utf-8-sig"), file_name=f"dashboard_{qid}.csv",
            mime="text/csv", key=f"dl_{qid}", icon=":material/download:", on_click="ignore",
        )


# ---------------------------------------------------------------------------------------------------
# Thẻ KPI
# ---------------------------------------------------------------------------------------------------
def kpi(col, label: str, value: str, delta: str | None = None, *, help: str | None = None,
        delta_color: str = "normal", delta_desc: str | None = None) -> None:
    """Thẻ KPI. delta_color="off" -> delta chỉ là chú thích (xám, không mũi tên) ; "red" -> cảnh báo (đỏ, không mũi tên)."""
    arrow = "auto" if delta_color in ("normal", "inverse") else "off"
    with col:
        st.metric(label, value, delta=delta, help=help, delta_color=delta_color, delta_arrow=arrow,
                  delta_description=delta_desc if delta else None, border=True)


def growth_delta(pct: Any) -> str | None:
    """% tăng trưởng có dấu cho delta của st.metric (mũi tên + màu theo dấu)."""
    return None if missing(pct) else fmt_pct(pct, 1, signed=True)


# ---------------------------------------------------------------------------------------------------
# Bảng có định dạng
# ---------------------------------------------------------------------------------------------------
# kind: text | int | num1 | num2 | vnd (đầy đủ) | vndc (rút gọn) | count | pct | pct_signed | share (thanh %) |
#       star | date | datetime | bool
def _formatter(kind: str) -> Callable[[Any], str] | None:
    return {
        "int": lambda v: fmt_num(v, 0),
        "num1": lambda v: fmt_num(v, 1),
        "num2": lambda v: fmt_num(v, 2),
        "vnd": lambda v: DASH if missing(v) else f"{fmt_num(v, 0)} ₫",
        "vndc": fmt_vnd,
        "count": fmt_count,
        "pct": lambda v: fmt_pct(v, 1),
        "pct_signed": lambda v: fmt_pct(v, 1, signed=True),
        "star": lambda v: DASH if missing(v) else f"{fmt_num(v, 2)} ★",
        "date": lambda v: DASH if missing(v) else pd.Timestamp(v).strftime("%d/%m/%Y"),
        "datetime": lambda v: DASH if missing(v) else pd.Timestamp(v).strftime("%d/%m/%Y %H:%M"),
        "bool": lambda v: "✓" if (not missing(v) and bool(v)) else "",
    }.get(kind)


def table(df: pd.DataFrame, spec: dict[str, tuple[str, str]], *, height: int | None = None, key: str | None = None,
          on_select: Any = "ignore", style: Callable | None = None, share_max: float | None = None):
    """Hiện bảng với nhãn tiếng Việt + định dạng số Việt Nam.

    spec: {cột: (nhãn, kind)} theo thứ tự hiển thị. Cột không có trong df bị bỏ qua.
    style: hàm nhận Styler, trả Styler (tô màu có điều kiện).
    """
    cols = [c for c in spec if c in df.columns]
    view = df[cols].reset_index(drop=True)
    fmt = {c: f for c in cols if (f := _formatter(spec[c][1])) is not None}
    sty = view.style.format(fmt, na_rep=DASH)
    if style is not None:
        sty = style(sty)
    cfg: dict[str, Any] = {}
    for c in cols:
        label, kind = spec[c]
        if kind == "share":
            top = share_max if share_max is not None else max(float(view[c].max() or 0), 1.0)
            cfg[c] = st.column_config.ProgressColumn(label, min_value=0.0, max_value=top, format="%.1f%%")
        else:
            cfg[c] = st.column_config.Column(label)
    kwargs: dict[str, Any] = dict(column_config=cfg, hide_index=True, width="stretch")
    if height is not None:
        kwargs["height"] = height
    if on_select != "ignore":
        kwargs.update(key=key, on_select=on_select, selection_mode="single-row")
    elif key:
        kwargs["key"] = key
    return st.dataframe(sty, **kwargs)


def color_sign(sty, cols: list[str]):
    """Chữ xanh khi > 0, đỏ khi < 0 (kèm dấu +/- trong giá trị, không chỉ dựa vào màu)."""
    t = theme.tokens()

    def _c(v):
        if missing(v):
            return ""
        return f"color: {t.good_text}" if v > 0 else (f"color: {theme.STATUS['critical']}" if v < 0 else "")

    cols = [c for c in cols if c in sty.data.columns]
    return sty.map(_c, subset=cols) if cols else sty


def highlight_over(sty, col: str, threshold: float):
    """Tô đỏ chữ khi giá trị >= ngưỡng (vd. % review tiêu cực)."""
    if col not in sty.data.columns:
        return sty
    return sty.map(lambda v: "" if missing(v) or v < threshold else f"color: {theme.STATUS['critical']}; font-weight: 600",
                   subset=[col])
