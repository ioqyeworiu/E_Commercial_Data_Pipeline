"""Builder biểu đồ Plotly dùng chung.

Quy ước (skill dataviz):
- không dùng 2 trục Y: 2 đại lượng khác đơn vị -> 2 panel xếp dọc, chung trục X (two_panel)
- một chuỗi -> một màu (slot 1) ; màu theo thực thể, không theo thứ hạng
- nhãn trực tiếp có chọn lọc, tooltip cho phần còn lại ; grid / trục mảnh
"""
from __future__ import annotations

from typing import Iterable, Sequence

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from core import theme
from core.queries import sale_dates

PLOT_CONFIG = {
    "displaylogo": False,
    "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d", "toggleSpikelines"],
    "toImageButtonOptions": {"format": "png", "scale": 2},
}


def fig(height: int = 360, **layout) -> go.Figure:
    f = go.Figure()
    f.update_layout(template=theme.template_name(), height=height, **layout)
    return f


def two_panel(top: str, bottom: str, *, height: int = 460, ratio: float = 0.66, spacing: float = 0.11) -> go.Figure:
    """2 panel xếp dọc chung trục X, mỗi panel một đơn vị (thay cho trục phụ)."""
    f = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[ratio, 1 - ratio],
                      vertical_spacing=spacing, subplot_titles=(top, bottom))
    f.update_layout(template=theme.template_name(), height=height)
    t = theme.tokens()
    for ann in f.layout.annotations:   # tiêu đề panel: canh trái, chữ phụ
        ann.update(x=0, xanchor="left", font=dict(size=12, color=t.ink2))
    f.update_xaxes(showgrid=False, showline=True, linecolor=t.axis)
    f.update_yaxes(showgrid=True, gridcolor=t.grid, zeroline=False)
    f.update_xaxes(showticklabels=False, showline=False, row=1, col=1)
    # legend dưới trục X: không đè tiêu đề panel (trên trái) hay nhãn ngày sale (trên)
    f.update_layout(legend=dict(orientation="h", x=0, xanchor="left", y=-0.07, yanchor="top", traceorder="normal"),
                    margin=dict(l=8, r=16, t=40, b=48))
    return f


def two_cols(left: str, right: str, *, height: int = 420, widths: tuple[float, float] = (0.62, 0.38),
             spacing: float = 0.04) -> go.Figure:
    """2 panel ngang chung trục Y (vd. doanh thu | tăng trưởng cho cùng danh sách danh mục)."""
    f = make_subplots(rows=1, cols=2, shared_yaxes=True, column_widths=list(widths),
                      horizontal_spacing=spacing, subplot_titles=(left, right))
    f.update_layout(template=theme.template_name(), height=height)
    t = theme.tokens()
    for ann in f.layout.annotations:
        ann.update(font=dict(size=12, color=t.ink2))
    f.update_xaxes(showgrid=True, gridcolor=t.grid, showline=False, zeroline=False)
    f.update_yaxes(showgrid=False, showline=False)
    return f


def mark_sale_days(f: go.Figure, dates: Iterable, *, rows: Sequence[int] | None = None, label: bool = True) -> None:
    """Tô nhẹ cột ngày sale (vrect) + nhãn tên sự kiện ở panel trên cùng."""
    t = theme.tokens()
    sale = sale_dates()
    present = {pd.Timestamp(d).date() for d in dates}
    for d, name in sale.items():
        if d not in present:
            continue
        x0 = pd.Timestamp(d) - pd.Timedelta(hours=12)
        x1 = pd.Timestamp(d) + pd.Timedelta(hours=12)
        targets = rows or [None]
        for i, r in enumerate(targets):
            kw = dict(row=r, col=1) if r else {}
            f.add_vrect(x0=x0, x1=x1, fillcolor=theme.STATUS["warning"], opacity=0.14, line_width=0, layer="below", **kw)
        if label:
            f.add_annotation(x=pd.Timestamp(d), y=1.0, yref="paper", yanchor="bottom", text=f"Sale {name}",
                             showarrow=False, font=dict(size=11, color=t.ink2))


def show(f: go.Figure, key: str | None = None) -> None:
    st.plotly_chart(f, theme=None, config=PLOT_CONFIG, key=key, width="stretch")


def bar_h(df: pd.DataFrame, y: str, x: str, *, text: Sequence[str] | None = None, color: str | Sequence[str] | None = None,
          hover: str | None = None, customdata=None, height: int | None = None, x_title: str = "",
          xaxis_ticksuffix: str = "") -> go.Figure:
    """Bar ngang, phần tử lớn nhất ở trên."""
    t = theme.tokens()
    n = len(df)
    f = fig(height=height or max(240, 34 * n + 70))
    f.add_bar(
        y=df[y], x=df[x], orientation="h", marker_color=color or t.series[0],
        text=text, textposition="outside", cliponaxis=False, textfont=dict(color=t.ink2, size=12),
        customdata=customdata, hovertemplate=hover,
    )
    f.update_yaxes(autorange="reversed", showgrid=False, showline=False, ticks="", tickfont=dict(color=t.ink2))
    f.update_xaxes(showgrid=True, gridcolor=t.grid, showline=False, title_text=x_title, ticksuffix=xaxis_ticksuffix)
    f.update_layout(margin=dict(l=8, r=56, t=16, b=8), showlegend=False)
    return f


def column(df: pd.DataFrame, x: str, y: str, *, text: Sequence[str] | None = None, color: str | Sequence[str] | None = None,
           hover: str | None = None, customdata=None, height: int = 320, y_title: str = "") -> go.Figure:
    t = theme.tokens()
    f = fig(height=height)
    f.add_bar(x=df[x], y=df[y], marker_color=color or t.series[0], text=text, textposition="outside",
              cliponaxis=False, textfont=dict(color=t.ink2, size=12), customdata=customdata, hovertemplate=hover)
    f.update_yaxes(title_text=y_title)
    f.update_layout(showlegend=False, margin=dict(l=8, r=16, t=28, b=8))
    return f


def end_label(f: go.Figure, x, y, text: str, color: str, *, row: int | None = None, xshift: int = 6) -> None:
    """Nhãn trực tiếp ở cuối đường (thay cho việc phải dò legend)."""
    kw = dict(row=row, col=1) if row else {}
    f.add_annotation(x=x, y=y, text=text, showarrow=False, xanchor="left", xshift=xshift,
                     font=dict(size=11, color=theme.tokens().ink2), **kw)


def ref_line(f: go.Figure, *, x=None, y=None, text: str = "", row: int | None = None, col: int | None = None,
             position: str = "top right") -> None:
    """Đường tham chiếu mảnh, liền nét (ngưỡng, mức trung bình)."""
    t = theme.tokens()
    kw = dict(line=dict(color=t.muted, width=1), annotation_text=text, annotation_position=position,
              annotation_font=dict(size=11, color=t.ink2))
    if row:
        kw.update(row=row, col=col or 1)
    if x is not None:
        f.add_vline(x=x, **kw)
    if y is not None:
        f.add_hline(y=y, **kw)


def date_axis(f: go.Figure) -> None:
    f.update_xaxes(tickformat="%d/%m", hoverformat="%d/%m/%Y")


def _padded(values, pad: float) -> list[float]:
    vals = pd.to_numeric(pd.Series(list(values)), errors="coerce").fillna(0).astype(float)
    lo, hi = min(0.0, float(vals.min())), max(0.0, float(vals.max()))
    span = (hi - lo) or 1.0
    return [lo - (span * pad if lo < 0 else 0.0), hi + (span * pad if hi > 0 else 0.0)]


def pad_x(f: go.Figure, values, *, pad: float = 0.2, row: int | None = None, col: int | None = None) -> None:
    """Nới trục X để nhãn đặt ngoài đầu thanh không bị cắt / tràn sang panel bên cạnh."""
    f.update_xaxes(range=_padded(values, pad), row=row, col=col)


def pad_y(f: go.Figure, values, *, pad: float = 0.14, row: int | None = None, col: int | None = None) -> None:
    f.update_yaxes(range=_padded(values, pad), row=row, col=col)
