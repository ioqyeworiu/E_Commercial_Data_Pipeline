"""Dashboard 2: Danh mục & thương hiệu (02_category_brand.sql, [2.1]-[2.8])."""
from functools import partial

import pandas as pd
import streamlit as st

from core import charts as C
from core import filters, theme
from core.components import card, color_sign, fmt_num, fmt_pct, money_scale, page_header, table
from core.queries import category_options

page_header(2, "Danh mục & thương hiệu")
flt = filters.filter_bar(dates=True, category=True,
                         note="[2.4] luôn so sánh mọi danh mục cấp 1 ; [2.7], [2.8] tính trên toàn kỳ, không theo khoảng ngày.")
t = theme.tokens()
cat = flt["category_l1"]
level = "danh mục cấp 2 của " + cat if cat else "danh mục cấp 1"

# ---- [2.1] bảng hiệu suất + drill-down
with card("2.1", flt, subtitle=f"Mỗi dòng là một {level}." +
          ("" if cat else " Chọn một dòng để drill-down xuống danh mục cấp 2.")) as df:
    if df is not None:
        key = f"t21_{cat or 'all'}"
        spec = {
            "category": ("Danh mục", "text"),
            "revenue": ("Doanh thu", "vndc"),
            "revenue_share_pct": ("Thị phần", "share"),
            "revenue_per_day_growth_pct": ("Tăng trưởng/ngày", "pct_signed"),
            "units": ("Số bán", "count"),
            "products_per_day": ("SP/ngày", "int"),
            "sell_through_pct": ("% SP có bán", "pct"),
            "avg_sell_price": ("Giá niêm yết TB", "vndc"),
            "avg_discount_pct": ("% giảm TB", "pct"),
            "avg_rating": ("Rating TB", "star"),
            "price_changes": ("Lần đổi giá", "int"),
            "inflated_events": ("Nghi giảm giá ảo", "int"),
        }
        on_sel = "ignore" if cat else partial(filters.drill_category, key, list(df.category))
        table(df, spec, key=key, on_select=on_sel, style=lambda s: color_sign(s, ["revenue_per_day_growth_pct"]),
              height=min(38 + 35 * len(df), 520))
        if cat:
            st.button(f"← Về tất cả danh mục (đang xem: {cat})", on_click=filters.clear_category, type="tertiary")

# ---- [2.2] stacked area top 5 + khác
with card("2.2", flt, subtitle=f"Doanh thu theo ngày của 5 {level} lớn nhất, phần còn lại gộp \"khác\" (xám).") as df:
    if df is not None:
        k, unit = money_scale(df.groupby("full_date").revenue.sum())
        piv = df.pivot_table(index="full_date", columns="category", values="revenue", aggfunc="sum").fillna(0)
        order = [c for c in piv.sum().sort_values(ascending=False).index if c != theme.OTHER_LABEL]
        if theme.OTHER_LABEL in piv.columns:
            order.append(theme.OTHER_LABEL)
        colors = theme.entity_colors(order, category_options() if not cat else None)
        x = pd.to_datetime(piv.index)
        f = C.fig(height=400)
        for name in order:
            f.add_scatter(x=x, y=piv[name] / k, name=name, mode="lines", stackgroup="one",
                          line=dict(width=0.8, color=colors[name]), fillcolor=colors[name],
                          hovertemplate=f"{name}: " + "%{y:,.2f} " + unit + "<extra></extra>")
        f.update_layout(hovermode="x unified", legend=dict(traceorder="normal"))
        f.update_yaxes(title_text=f"Doanh thu ({unit})")
        C.mark_sale_days(f, x)
        C.date_axis(f)
        C.show(f, "c22")

# ---- [2.3] top 15 brand | [2.4] HHI
left, right = st.columns([1.1, 1])
with left:
    with card("2.3", flt, subtitle="Thị phần trong phạm vi đang lọc, không tính \"Không rõ thương hiệu\".") as df:
        if df is not None:
            d = df.copy()
            d["label"] = d.brand_name + d.brand_origin.map(lambda o: f"  ({o})" if o and o not in ("unknown", "") else "")
            f = C.bar_h(d, "label", "market_share_pct", text=[fmt_pct(v, 2) for v in d.market_share_pct],
                        customdata=d[["revenue", "avg_selling_price", "avg_discount_pct", "avg_rating", "revenue_per_day_growth_pct"]],
                        hover="<b>%{y}</b><br>Thị phần: %{x:,.2f}%<br>Doanh thu: %{customdata[0]:,.0f} ₫"
                              "<br>Giá bán BQ: %{customdata[1]:,.0f} ₫<br>% giảm TB: %{customdata[2]:,.1f}%"
                              "<br>Rating: %{customdata[3]:,.2f}<br>Tăng trưởng/ngày: %{customdata[4]:+,.1f}%<extra></extra>",
                        x_title="Thị phần doanh thu (%)")
            C.pad_x(f, d.market_share_pct, pad=0.18)
            C.show(f, "c23")
            table(d, {"brand_name": ("Thương hiệu", "text"), "revenue": ("Doanh thu", "vndc"),
                      "avg_selling_price": ("Giá bán BQ", "vndc"), "avg_discount_pct": ("% giảm", "pct"),
                      "avg_rating": ("Rating", "star"), "revenue_per_day_growth_pct": ("Tăng trưởng", "pct_signed")},
                  style=lambda s: color_sign(s, ["revenue_per_day_growth_pct"]), height=280)
with right:
    with card("2.4", flt, subtitle="HHI = tổng bình phương thị phần brand (0–10.000). "
                                   "< 1.000 phân tán · 1.000–1.800 tập trung vừa · > 1.800 tập trung cao.") as df:
        if df is not None:
            d = df.copy()
            status = {"phân tán": theme.STATUS["good"], "tập trung vừa": theme.STATUS["warning"],
                      "tập trung cao": theme.STATUS["critical"]}
            f = C.bar_h(d, "category_level_1", "hhi", color=[status.get(s, t.neutral) for s in d.market_structure],
                        text=[f"{fmt_num(h)} · {s}" for h, s in zip(d.hhi, d.market_structure)],
                        customdata=d[["brands", "top1_share_pct", "top5_share_pct", "top10_share_pct"]],
                        hover="<b>%{y}</b><br>HHI: %{x:,.0f}<br>Số brand: %{customdata[0]:,.0f}"
                              "<br>Top 1: %{customdata[1]:,.1f}% · Top 5: %{customdata[2]:,.1f}% · Top 10: %{customdata[3]:,.1f}%<extra></extra>",
                        x_title="HHI")
            for thr, lbl in ((1000, "1.000"), (1800, "1.800")):
                if d.hhi.max() >= thr * 0.6:
                    C.ref_line(f, x=thr, text=lbl, position="top")
            C.pad_x(f, list(d.hhi) + [1000], pad=0.45)
            C.show(f, "c24")
            table(d, {"category_level_1": ("Danh mục", "text"), "brands": ("Số brand", "int"),
                      "top1_share_pct": ("Top 1", "pct"), "top5_share_pct": ("Top 5", "pct"),
                      "top10_share_pct": ("Top 10", "pct")}, height=280)

# ---- [2.5] ma trận brand
with card("2.5", flt, subtitle="X = tăng trưởng doanh thu/ngày, Y = rating TB, kích thước = doanh thu. "
                               "30 brand lớn nhất; đường dọc = 0%, đường ngang = rating TB của nhóm.") as df:
    if df is not None:
        d = df.dropna(subset=["growth_pct", "avg_rating"]).copy()
        if d.empty:
            st.info("Kỳ trước không có dữ liệu để tính tăng trưởng: hãy chọn khoảng ngày muộn hơn.")
        else:
            y_mid = float(d.avg_rating.mean())
            size = (d.revenue / d.revenue.max()) ** 0.5 * 46 + 8
            top = set(d.nlargest(8, "revenue").brand_name)
            f = C.fig(height=480)
            f.add_scatter(x=d.growth_pct, y=d.avg_rating, mode="markers+text",
                          marker=dict(size=size, color=t.series[0], opacity=0.55, line=dict(color=t.surface, width=2)),
                          text=[b if b in top else "" for b in d.brand_name], textposition="top center",
                          textfont=dict(size=11, color=t.ink2), customdata=d[["brand_name", "revenue", "quadrant"]],
                          hovertemplate="<b>%{customdata[0]}</b><br>Tăng trưởng: %{x:+,.1f}%<br>Rating: %{y:,.2f}"
                                        "<br>Doanh thu: %{customdata[1]:,.0f} ₫<br>Nhóm: %{customdata[2]}<extra></extra>")
            C.ref_line(f, x=0)
            C.ref_line(f, y=y_mid)
            xr = C._padded(d.growth_pct, 0.15)
            yr = [float(d.avg_rating.min()) - 0.05, float(d.avg_rating.max()) + 0.08]
            f.update_xaxes(range=[min(xr[0], -abs(xr[1]) * 0.25), xr[1]], ticksuffix="%", title_text="Tăng trưởng doanh thu/ngày so với kỳ trước",
                           showgrid=True, gridcolor=t.grid)
            f.update_yaxes(range=yr, title_text="Rating trung bình")
            corners = [(1, 1, "right", "top", "Ngôi sao"), (1, 0, "right", "bottom", "Tăng nhưng rating thấp"),
                       (0, 1, "left", "top", "Rating tốt nhưng chững lại"), (0, 0, "left", "bottom", "Cần chú ý")]
            for xp, yp, xa, ya, txt in corners:
                f.add_annotation(x=xp, y=yp, xref="paper", yref="paper", xanchor=xa, yanchor=ya, text=txt,
                                 showarrow=False, font=dict(size=12, color=t.muted))
            C.show(f, "c25")

# ---- [2.6] phân khúc giá
with card("2.6", flt, subtitle="Tỉ trọng doanh thu của từng phân khúc giá trong mỗi danh mục (tổng mỗi hàng = 100%).") as df:
    if df is not None:
        bands = sorted(df.price_band.unique())
        colors = dict(zip(bands, theme.ordinal(len(bands))))
        order = df.groupby("category_level_1").revenue.sum().sort_values(ascending=False).index.tolist()
        f = C.fig(height=max(260, 30 * len(order) + 110))
        for b in bands:
            sub = df[df.price_band == b].set_index("category_level_1").reindex(order)
            share = sub.revenue_share_in_category_pct.fillna(0)
            dark = bands.index(b) >= len(bands) / 2
            f.add_bar(y=order, x=share, name=b.split(". ", 1)[-1], orientation="h", marker_color=colors[b],
                      text=[fmt_pct(v, 0) if v >= 7 else "" for v in share], textposition="inside", insidetextanchor="middle",
                      textfont=dict(color="#ffffff" if (dark or t.mode == "dark") else t.ink, size=11),
                      customdata=sub[["revenue"]].fillna(0),
                      hovertemplate="<b>%{y}</b> · " + b.split(". ", 1)[-1] + "<br>%{x:,.1f}% doanh thu danh mục"
                                    "<br>Doanh thu: %{customdata[0]:,.0f} ₫<extra></extra>")
        f.update_layout(barmode="stack", legend=dict(traceorder="normal", title=dict(text="Phân khúc giá ")))
        f.update_yaxes(autorange="reversed", showgrid=False)
        f.update_xaxes(range=[0, 100], ticksuffix="%", showgrid=False)
        C.show(f, "c26")

# ---- [2.7] ABC | [2.8] Pareto (toàn kỳ)
left, right = st.columns(2)
with left:
    with card("2.7", flt, subtitle="Tính trên toàn kỳ đã nạp (không theo khoảng ngày). A = nhóm tạo 80% doanh thu đầu, "
                                   "B = 80–95%, C = phần còn lại, N = chưa có doanh thu.") as df:
        if df is not None:
            names = {"A": "A · 80% DT đầu", "B": "B · 80–95%", "C": "C · còn lại", "N": "N · không bán"}
            x = [names.get(c, c) for c in df.abc_class]
            f = C.fig(height=340)
            f.add_bar(x=x, y=df.products_pct, name="% số sản phẩm", marker_color=t.series[0],
                      text=[fmt_pct(v) for v in df.products_pct], textposition="outside", cliponaxis=False,
                      customdata=df[["products"]], hovertemplate="%{x}<br>%{y:,.1f}% sản phẩm (%{customdata[0]:,.0f})<extra></extra>")
            f.add_bar(x=x, y=df.revenue_pct, name="% doanh thu", marker_color=t.series[1],
                      text=[fmt_pct(v) for v in df.revenue_pct], textposition="outside", cliponaxis=False,
                      customdata=df[["revenue"]], hovertemplate="%{x}<br>%{y:,.1f}% doanh thu (%{customdata[0]:,.0f} ₫)<extra></extra>")
            f.update_layout(barmode="group")
            f.update_yaxes(ticksuffix="%")
            C.pad_y(f, list(df.products_pct) + list(df.revenue_pct), pad=0.15)
            C.show(f, "c27")
with right:
    with card("2.8", flt, subtitle="Đọc: x% sản phẩm (xếp theo doanh thu) tạo ra y% doanh thu. Tính trên toàn kỳ.") as df:
        if df is not None:
            f = C.fig(height=340)
            f.add_scatter(x=df.product_percentile, y=df.cum_revenue_pct, mode="lines", line=dict(color=t.series[0]),
                          fill="tozeroy", fillcolor="rgba(42,120,214,0.10)" if t.mode == "light" else "rgba(57,135,229,0.16)",
                          hovertemplate="Top %{x}% sản phẩm → %{y:,.1f}% doanh thu<extra></extra>", name="")
            for p in (10, 20):
                row = df[df.product_percentile == p]
                if len(row):
                    y = float(row.cum_revenue_pct.iloc[0])
                    f.add_scatter(x=[p], y=[y], mode="markers", marker=dict(size=9, color=t.series[0]),
                                  hoverinfo="skip", showlegend=False)
                    f.add_annotation(x=p, y=y, text=f"{p}% SP → {fmt_pct(y)} DT", showarrow=False, xanchor="left",
                                     yanchor="top", xshift=8, yshift=-4, font=dict(size=12, color=t.ink))
            C.ref_line(f, y=80, text="80% doanh thu", position="bottom right")
            f.update_xaxes(ticksuffix="%", title_text="% sản phẩm (xếp theo doanh thu giảm dần)", range=[0, 100], showgrid=True, gridcolor=t.grid)
            f.update_yaxes(ticksuffix="%", range=[0, 102], title_text="% doanh thu lũy kế")
            f.update_layout(showlegend=False)
            C.show(f, "c28")
