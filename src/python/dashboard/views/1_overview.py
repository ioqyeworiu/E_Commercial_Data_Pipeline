"""Dashboard 1: Tổng quan thị trường (01_overview.sql, [1.1]-[1.6])."""
import pandas as pd
import streamlit as st
from plotly.colors import sample_colorscale

from core import charts as C
from core import filters, theme
from core.components import (card, fmt_count, fmt_num, fmt_pct, fmt_vnd, growth_delta, kpi, money_scale,
                             page_header)

page_header(1, "Tổng quan thị trường")
flt = filters.filter_bar(dates=True, category=False,
                         note="Trang tổng quan tính trên toàn sàn, không lọc danh mục.")
t = theme.tokens()

# ---- [1.1] KPI
with card("1.1", flt, border=False) as df:
    if df is not None:
        r = df.iloc[0]
        c = st.columns(4)
        kpi(c[0], "Doanh thu ước tính", fmt_vnd(r.revenue), help=f"Tổng {fmt_num(r.days)} ngày tin cậy trong kỳ")
        kpi(c[1], "Doanh thu / ngày", fmt_vnd(r.revenue_per_day), growth_delta(r.revenue_per_day_growth_pct),
            delta_desc="so với kỳ trước")
        kpi(c[2], "Số bán / ngày", fmt_count(r.units_per_day), growth_delta(r.units_per_day_growth_pct),
            delta_desc="so với kỳ trước")
        kpi(c[3], "Giá bán bình quân", fmt_vnd(r.avg_selling_price),
            help="Doanh thu / số bán: bình quân gia quyền theo lượng bán")
        c = st.columns(4)
        kpi(c[0], "Sản phẩm có bán / ngày", fmt_count(r.products_selling_per_day),
            help=f"Tổng số bán trong kỳ: {fmt_count(r.units)}")
        kpi(c[1], "% giảm giá trung bình", fmt_pct(r.avg_discount_pct),
            help=f"{fmt_pct(r.discounted_share_pct)} lượt sản phẩm-ngày có giảm giá")
        kpi(c[2], "Rating trung bình", f"{fmt_num(r.avg_rating, 2)} ★",
            help=f"{fmt_count(r.new_ratings)} lượt đánh giá mới trong kỳ")
        kpi(c[3], "Seller / brand hoạt động", f"{fmt_count(r.active_sellers)} / {fmt_count(r.active_brands)}",
            help=f"{fmt_count(r.price_changes)} lần đổi giá, {fmt_count(r.inflated_events)} lần nghi giảm giá ảo")

# ---- [1.2] Xu hướng doanh thu
with card("1.2", flt, subtitle="Cột = doanh thu từng ngày, đường = trung bình trượt 7 ngày. Vùng vàng = ngày sale.") as df:
    if df is not None:
        x = pd.to_datetime(df.full_date)
        k, unit = money_scale(df.revenue_est)
        f = C.two_panel(f"Doanh thu ước tính ({unit})", "% giảm giá trung bình", height=480)
        f.add_bar(x=x, y=df.revenue_est / k, name="Doanh thu / ngày", marker_color=t.seq[2], row=1, col=1,
                  customdata=df[["day_name", "units_sold_est", "products_selling"]],
                  hovertemplate="%{x|%d/%m/%Y} · %{customdata[0]}<br>Doanh thu: %{y:,.2f} " + unit +
                                "<br>Số bán: %{customdata[1]:,.0f}<br>SP có bán: %{customdata[2]:,.0f}<extra></extra>")
        f.add_scatter(x=x, y=df.revenue_ma7 / k, name="TB trượt 7 ngày", mode="lines", line=dict(color=t.seq[-2], width=2.5),
                      row=1, col=1, hovertemplate="TB 7 ngày: %{y:,.2f} " + unit + "<extra></extra>")
        f.add_scatter(x=x, y=df.avg_discount_pct, name="% giảm TB", mode="lines+markers", showlegend=False,
                      line=dict(color=t.series[1]), marker=dict(size=6), row=2, col=1,
                      hovertemplate="%{x|%d/%m/%Y}<br>% giảm TB: %{y:,.1f}%<extra></extra>")
        f.update_yaxes(ticksuffix="%", row=2, col=1)
        C.mark_sale_days(f, x, rows=[1, 2])
        C.date_axis(f)
        C.show(f, "c12")

# ---- [1.3] danh mục cấp 1 | [1.4] treemap
left, right = st.columns([1.05, 1])
with left:
    with card("1.3", flt, subtitle="Trái: doanh thu, nhãn = thị phần. Phải: tăng trưởng doanh thu/ngày so với kỳ trước.") as df:
        if df is not None:
            d = df.sort_values("revenue", ascending=False)
            k, unit = money_scale(d.revenue)
            f = C.two_cols(f"Doanh thu ({unit})", "Tăng trưởng", height=max(340, 27 * len(d) + 90))
            f.add_bar(y=d.category_level_1, x=d.revenue / k, orientation="h", marker_color=t.series[0],
                      text=[fmt_pct(v) for v in d.revenue_share_pct], textposition="outside", cliponaxis=False,
                      textfont=dict(color=t.ink2, size=11), row=1, col=1,
                      customdata=d[["revenue_share_pct", "units", "avg_selling_price", "products_per_day"]],
                      hovertemplate="<b>%{y}</b><br>Doanh thu: %{x:,.2f} " + unit +
                                    "<br>Thị phần: %{customdata[0]:,.1f}%<br>Số bán: %{customdata[1]:,.0f}"
                                    "<br>Giá bán BQ: %{customdata[2]:,.0f} ₫<br>SP/ngày: %{customdata[3]:,.0f}<extra></extra>")
            g = d.revenue_per_day_growth_pct
            f.add_bar(y=d.category_level_1, x=g.fillna(0), orientation="h", marker_color=theme.diverging_sign(g, dead_zone=1),
                      text=[fmt_pct(v, 1, signed=True) for v in g], textposition="outside", cliponaxis=False,
                      textfont=dict(color=t.ink2, size=11), row=1, col=2,
                      hovertemplate="<b>%{y}</b><br>Tăng trưởng DT/ngày: %{x:+,.1f}%<extra></extra>")
            f.update_yaxes(autorange="reversed", tickfont=dict(size=12, color=t.ink2))
            f.update_xaxes(ticksuffix="%", row=1, col=2)
            C.pad_x(f, d.revenue / k, pad=0.22, row=1, col=1)
            C.pad_x(f, g, pad=0.45, row=1, col=2)
            f.update_layout(showlegend=False, margin=dict(l=8, r=16, t=40, b=8))
            C.show(f, "c13")
with right:
    with card("1.4", flt, subtitle="Diện tích = doanh thu. Bấm vào một danh mục cấp 1 để phóng to, bấm tiêu đề để quay lại.") as df:
        if df is not None:
            l1 = df.groupby("category_level_1", sort=False)["revenue"].sum().sort_values(ascending=False)
            total = float(l1.sum())
            # Gốc tường minh "Toàn sàn" tô màu nền: gốc ngầm định của Plotly bị tô xám đậm (viền + pathbar) ở theme sáng.
            ids = ["ALL"] + [f"L1/{c}" for c in l1.index] + [f"L1/{a}/{b}" for a, b in zip(df.category_level_1, df.category_level_2)]
            labels = ["Toàn sàn"] + list(l1.index) + list(df.category_level_2)
            parents = [""] + ["ALL"] * len(l1) + [f"L1/{a}" for a in df.category_level_1]
            values = [total] + [float(v) for v in l1.values] + [float(v) for v in df.revenue]
            shares = [v / total * 100 for v in values]
            ramp = ["#e6f0fc", "#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5"] if t.mode == "light" else                 ["#1f2a38", "#1c3d66", "#1c5cab", "#2a78d6", "#5598e7"]
            scale = [[i / (len(ramp) - 1), c] for i, c in enumerate(ramp)]
            depth = [s ** 0.5 for s in shares[1:]]
            top = max(depth) or 1.0
            colors = [t.surface] + sample_colorscale(scale, [d / top for d in depth])
            f = C.fig(height=max(340, 27 * len(l1) + 90))
            f.add_treemap(ids=ids, labels=labels, parents=parents, values=values, branchvalues="total",
                          marker=dict(colors=colors, line=dict(width=2, color=t.surface)),
                          customdata=shares, maxdepth=3, tiling=dict(pad=2),
                          texttemplate="<b>%{label}</b><br>%{customdata:,.1f}%",
                          hovertemplate="<b>%{label}</b><br>Doanh thu: %{value:,.0f} ₫<br>Tỉ trọng toàn sàn: %{customdata:,.2f}%<extra></extra>",
                          pathbar=dict(visible=True, thickness=22),
                          insidetextfont=dict(color=t.ink, size=12), outsidetextfont=dict(color=t.ink2, size=12))
            f.update_layout(margin=dict(l=4, r=4, t=8, b=4))
            C.show(f, "c14")

# ---- [1.5] hiệu ứng ngày sale | [1.6] thứ trong tuần
left, right = st.columns(2)
with left:
    with card("1.5", flt, subtitle="Nhãn = bội số so với ngày thường. \"Ngày sau sale\" tách riêng vì số bán của ngày sale bị ghi dồn sang hôm sau.") as df:
        if df is not None:
            d = df.copy()
            colors = [t.neutral if v == "Ngày thường" else (t.seq[3] if v == "Ngày sau sale" else t.series[0]) for v in d.day_type]
            k, unit = money_scale(d.revenue_per_day)
            f = C.two_cols(f"Doanh thu / ngày ({unit})", "Bán TB / sản phẩm / ngày", height=300, widths=(0.5, 0.5),
                           spacing=0.06)
            f.add_bar(y=d.day_type, x=d.revenue_per_day / k, orientation="h", marker_color=colors, row=1, col=1,
                      text=[f"×{fmt_num(v, 2)}" for v in d.revenue_uplift_vs_normal], textposition="outside",
                      cliponaxis=False, textfont=dict(color=t.ink2, size=11), customdata=d[["days"]],
                      hovertemplate="<b>%{y}</b> (%{customdata[0]} ngày)<br>Doanh thu/ngày: %{x:,.2f} " + unit + "<extra></extra>")
            f.add_bar(y=d.day_type, x=d.units_per_product_day, orientation="h", marker_color=colors, row=1, col=2,
                      text=[f"×{fmt_num(v, 2)}" for v in d.units_uplift_vs_normal], textposition="outside",
                      cliponaxis=False, textfont=dict(color=t.ink2, size=11),
                      customdata=d[["selling_products_pct", "avg_discount_pct", "deep_discount_share_pct"]],
                      hovertemplate="<b>%{y}</b><br>Bán TB/SP/ngày: %{x:,.2f}<br>% SP có bán: %{customdata[0]:,.1f}%"
                                    "<br>% giảm TB: %{customdata[1]:,.1f}%<br>% SP giảm ≥ 30%: %{customdata[2]:,.1f}%<extra></extra>")
            f.update_yaxes(autorange="reversed")
            C.pad_x(f, d.revenue_per_day / k, pad=0.3, row=1, col=1)
            C.pad_x(f, d.units_per_product_day, pad=0.3, row=1, col=2)
            f.update_layout(showlegend=False, margin=dict(l=8, r=16, t=40, b=8))
            C.show(f, "c15")
with right:
    with card("1.6", flt, subtitle="Doanh thu TB theo thứ, đã bỏ ngày sale và ngày ngay sau sale. Nhãn = chênh lệch so với TB tuần.") as df:
        if df is not None:
            k, unit = money_scale(df.revenue_per_day)
            d = df.assign(rev=df.revenue_per_day / k)
            f = C.column(d, "day_name", "rev", height=300, y_title=f"Doanh thu / ngày ({unit})",
                         color=theme.diverging_sign(d.vs_week_avg_pct, dead_zone=2),
                         text=[fmt_pct(v, 1, signed=True) for v in d.vs_week_avg_pct],
                         customdata=d[["days", "units_per_day"]],
                         hover="<b>%{x}</b> (%{customdata[0]} ngày)<br>Doanh thu/ngày: %{y:,.2f} " + unit +
                               "<br>Số bán/ngày: %{customdata[1]:,.0f}<extra></extra>")
            f.update_xaxes(categoryorder="array", categoryarray=list(d.day_name))
            C.pad_y(f, d.rev, pad=0.12)
            C.show(f, "c16")
            st.caption("Xanh / đỏ = cao / thấp hơn TB tuần quá 2% ; xám = chênh lệch không đáng kể.")
