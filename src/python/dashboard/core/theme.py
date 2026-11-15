"""Bảng màu và template Plotly cho dashboard.

Giá trị lấy từ bảng màu tham chiếu đã kiểm định CVD (skill dataviz, references/palette.md):
- categorical: 8 slot, dùng theo thứ tự cố định, không sinh thêm màu thứ 9 (gộp vào "khác")
- sequential / ordinal: một sắc xanh, nhạt -> đậm
- diverging: xanh <-> đỏ, giữa là xám
- status: good / warning / serious / critical, chỉ dùng khi màu mang nghĩa tốt / xấu, luôn kèm nhãn

Light và dark là hai bộ bước màu riêng (không tự đảo màu). Template được chọn theo theme người xem đang dùng.
"""
from __future__ import annotations

from dataclasses import dataclass

import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

FONT_FAMILY = 'system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif'


@dataclass(frozen=True)
class Tokens:
    mode: str
    surface: str        # nền biểu đồ / card
    plane: str          # nền trang
    ink: str            # chữ chính
    ink2: str           # chữ phụ
    muted: str          # nhãn trục
    grid: str           # gridline hairline
    axis: str           # baseline / trục
    border: str         # viền hairline
    good_text: str      # chữ "tăng tốt"
    neutral: str        # điểm giữa diverging / nhóm "khác"
    series: tuple[str, ...]
    seq: tuple[str, ...]        # ordinal ramp xanh, ít -> nhiều (bước gần nền nhất vẫn >= 2:1)
    red_seq: tuple[str, ...]    # nhánh đỏ của diverging, yếu -> mạnh


# Bước 250 -> 700 của ramp xanh (bước nhạt nhất vẫn >= 2:1 với nền sáng, theo quy tắc ordinal)
_BLUE_LIGHT = ("#86b6ef", "#6da7ec", "#5598e7", "#3987e5", "#2a78d6", "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b")
_BLUE_DARK = ("#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7", "#3987e5", "#2a78d6", "#256abf", "#1c5cab")

LIGHT = Tokens(
    mode="light",
    surface="#fcfcfb",
    plane="#f4f4f1",
    ink="#0b0b0b",
    ink2="#52514e",
    muted="#898781",
    grid="#e1e0d9",
    axis="#c3c2b7",
    border="rgba(11,11,11,0.10)",
    good_text="#006300",
    neutral="#b9b7ae",
    series=("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"),
    seq=_BLUE_LIGHT,
    red_seq=("#f2a7a6", "#e87170", "#e34948", "#c23434", "#9e2626"),
)

DARK = Tokens(
    mode="dark",
    surface="#1a1a19",
    plane="#0d0d0d",
    ink="#f0efec",
    ink2="#c3c2b7",
    muted="#898781",
    grid="#2c2c2a",
    axis="#383835",
    border="rgba(255,255,255,0.10)",
    good_text="#0ca30c",
    neutral="#5f5e59",
    series=("#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#0ca30c", "#9085e9", "#e66767"),
    seq=_BLUE_DARK[::-1],   # trên nền tối: "nhiều" = sáng hơn (bước tối nhất = 550, không tối hơn 600)
    red_seq=("#7a2222", "#a83030", "#d04545", "#e66767", "#f09a9a"),
)

# Status: cố định cho cả hai mode, luôn đi kèm icon / nhãn
STATUS = {
    "good": "#0ca30c",
    "warning": "#fab219",
    "serious": "#ec835a",
    "critical": "#d03b3b",
}

# Verdict khuyến mãi trong ngày sale (mart_sale_event_product.discount_verdict)
VERDICT_ORDER = ["giảm thật", "giảm danh nghĩa", "giảm giá ảo", "không giảm", "không đủ dữ liệu"]


def verdict_colors(t: Tokens) -> dict[str, str]:
    return {
        "giảm thật": STATUS["good"],
        "giảm danh nghĩa": STATUS["warning"],
        "giảm giá ảo": STATUS["critical"],
        "không giảm": t.neutral,
        "không đủ dữ liệu": t.grid if t.mode == "light" else t.axis,
    }


def current_mode() -> str:
    """'light' hoặc 'dark' theo theme người xem đang chọn (menu ⋮ > Settings của Streamlit)."""
    try:
        mode = st.context.theme.type
    except Exception:
        mode = None
    return "dark" if mode == "dark" else "light"


def tokens() -> Tokens:
    return DARK if current_mode() == "dark" else LIGHT


def ordinal(n: int, t: Tokens | None = None) -> list[str]:
    """n màu xanh có thứ tự (nhạt -> đậm), trải đều trên ramp."""
    t = t or tokens()
    ramp = t.seq
    if n <= 1:
        return [ramp[len(ramp) // 2]]
    idx = [round(i * (len(ramp) - 1) / (n - 1)) for i in range(n)]
    return [ramp[i] for i in idx]


def diverging_sign(values, t: Tokens | None = None, dead_zone: float = 0.0) -> list[str]:
    """Màu theo dấu: dương xanh (slot 1), âm đỏ ; |v| <= dead_zone hoặc thiếu dữ liệu -> xám trung tính
    (chênh lệch quá nhỏ không đáng tô màu "tốt / xấu")."""
    t = t or tokens()
    out = []
    for v in values:
        if v is None or v != v or abs(v) <= dead_zone:  # NaN / gần 0
            out.append(t.neutral)
        elif v > 0:
            out.append(t.series[0])
        else:
            out.append(t.red_seq[2])
    return out


def _template(t: Tokens) -> go.layout.Template:
    axis = dict(
        gridcolor=t.grid,
        gridwidth=1,
        linecolor=t.axis,
        zeroline=False,
        ticks="",
        tickfont=dict(color=t.muted, size=12),
        title=dict(font=dict(color=t.ink2, size=12), standoff=8),
        automargin=True,
        showspikes=False,
    )
    tpl = go.layout.Template()
    tpl.layout = go.Layout(
        font=dict(family=FONT_FAMILY, color=t.ink2, size=13),
        paper_bgcolor="rgba(0,0,0,0)",   # trong suốt: lấy nền của card
        plot_bgcolor="rgba(0,0,0,0)",
        colorway=list(t.series),
        separators=",.",                       # kiểu Việt Nam: 1.234,5
        margin=dict(l=8, r=16, t=40, b=8),
        xaxis=dict(axis, showgrid=False, showline=True),
        yaxis=dict(axis, showgrid=True, showline=False),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
            font=dict(color=t.ink2, size=12), bgcolor="rgba(0,0,0,0)", title=dict(text=""),
        ),
        hoverlabel=dict(bgcolor=t.surface, bordercolor=t.axis, font=dict(color=t.ink, family=FONT_FAMILY, size=13)),
        bargap=0.28,
        bargroupgap=0.08,
        barcornerradius=4,
        title=dict(font=dict(color=t.ink, size=14), x=0, xanchor="left"),
        hovermode="closest",
        uniformtext=dict(minsize=10, mode="hide"),
    )
    # Khe 1px màu nền giữa các khối (stacked segment, cột liền nhau) thay cho viền
    tpl.data.bar = [go.Bar(marker=dict(line=dict(color=t.surface, width=1)))]
    tpl.data.scatter = [go.Scatter(line=dict(width=2), marker=dict(size=8, line=dict(color=t.surface, width=2)))]
    tpl.data.treemap = [go.Treemap(marker=dict(line=dict(color=t.surface, width=2)))]
    return tpl


pio.templates["ecom_light"] = _template(LIGHT)
pio.templates["ecom_dark"] = _template(DARK)


def template_name() -> str:
    return "ecom_dark" if current_mode() == "dark" else "ecom_light"


# ---------------------------------------------------------------------------------------------------
# Màu cố định theo thực thể (danh mục cấp 1): cùng một danh mục giữ cùng một màu trên mọi biểu đồ.
# Thứ tự slot = thứ hạng doanh thu toàn kỳ (tính một lần trong queries.category_options()).
# ---------------------------------------------------------------------------------------------------
OTHER_LABEL = "khác"


def entity_colors(names: list[str], ranked_universe: list[str] | None = None, t: Tokens | None = None) -> dict[str, str]:
    """Gán màu cho các thực thể trong một biểu đồ.

    names: thực thể có mặt trong biểu đồ (tối đa 8, cộng "khác").
    ranked_universe: danh sách thực thể xếp hạng cố định (vd. toàn bộ danh mục cấp 1 theo doanh thu toàn kỳ);
                     thực thể trong top 8 của danh sách này luôn nhận đúng slot của nó.
    """
    t = t or tokens()
    colors: dict[str, str] = {}
    used: set[int] = set()
    fixed = {n: i for i, n in enumerate((ranked_universe or [])[: len(t.series)])}
    for n in names:
        if n == OTHER_LABEL:
            colors[n] = t.neutral
        elif n in fixed:
            colors[n] = t.series[fixed[n]]
            used.add(fixed[n])
    free = [i for i in range(len(t.series)) if i not in used]
    for n in names:
        if n not in colors:
            colors[n] = t.series[free.pop(0)] if free else t.neutral
    return colors
