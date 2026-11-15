"""Dashboard 4: Giá & khuyến mãi (04_pricing_promotion.sql, [4.1]-[4.8])."""
from functools import partial

import pandas as pd
import streamlit as st

from core import charts as C
from core import filters, theme
from core.components import card, fmt_num, fmt_pct, missing, page_header, section, table

filters.goto_product_if_requested()   # drill-through từ bảng [4.7]
page_header(4, "Giá & khuyến mãi")
flt = filters.filter_bar(dates=True, category=True,
                         note="[4.1], [4.5], [4.8] không lọc danh mục ; [4.3]–[4.7] không theo khoảng ngày "
                          "mà theo các ngày sale đã nạp.")
t = theme.tokens()
UP, DOWN = t.red_seq[2], t.series[0]          # tăng giá = đỏ, giảm giá = xanh (cặp diverging)
VERDICT = theme.verdict_colors(t)

# ---- [4.1] số lần đổi giá
with card("4.1", flt, subtitle="Panel trên: số lần đổi giá mỗi ngày (giảm / tăng / chỉ đổi giá gốc). "
                               "Panel dưới: số lần nghi giảm giá ảo (giá gốc bị nâng).") as df:
    if df is not None:
        x = pd.to_datetime(df.full_date)
        f = C.two_panel("Lần đổi giá / ngày", "Nghi giảm giá ảo / ngày", height=470)
        for col, name, color in (("price_down", "Giảm giá", DOWN), ("price_up", "Tăng giá", UP),
                                 ("list_price_only", "Chỉ đổi giá gốc", t.neutral)):
            f.add_bar(x=x, y=df[col], name=name, marker_color=color, row=1, col=1,
                      hovertemplate=f"{name}: " + "%{y:,.0f}<extra></extra>")
        f.add_scatter(x=x, y=df.inflated_events, name="Nghi giảm giá ảo", mode="lines+markers", showlegend=False,
                      line=dict(color=theme.STATUS["critical"]), marker=dict(size=6), row=2, col=1,
                      customdata=df[["changes_per_100_products"]],
                      hovertemplate="Nghi giảm giá ảo: %{y:,.0f}<br>Đổi giá / 100 SP: %{customdata[0]:,.2f}<extra></extra>")
        f.update_layout(barmode="stack", hovermode="x unified")
        C.mark_sale_days(f, x, rows=[1, 2])
        C.date_axis(f)
        C.show(f, "c41")

# ---- [4.2] mức giảm giá vs bán | [4.8] phân bố đổi giá
left, right = st.columns(2)
with left:
    with card("4.2", flt, subtitle="Chỉ ngày thường (không tính ngày sale). Nhãn = bội số so với nhóm không giảm. "
                                   "Đây là tương quan, không phải nhân quả.") as df:
        if df is not None:
            d = df.copy()
            d["band"] = d.discount_band.str.split(". ", n=1).str[-1]
            colors = theme.ordinal(len(d))
            f = C.two_panel("Bán TB / sản phẩm / ngày", "% doanh thu", height=440, ratio=0.6)
            f.add_bar(x=d.band, y=d.units_per_product_day, marker_color=colors, row=1, col=1, showlegend=False,
                      text=[f"×{fmt_num(v, 2)}" for v in d.index_vs_no_discount], textposition="outside", cliponaxis=False,
                      textfont=dict(color=t.ink2, size=11), customdata=d[["selling_pct", "product_days"]],
                      hovertemplate="Giảm %{x}<br>Bán TB/SP/ngày: %{y:,.2f}<br>% SP có bán: %{customdata[0]:,.1f}%"
                                    "<br>Số SP-ngày: %{customdata[1]:,.0f}<extra></extra>")
            f.add_bar(x=d.band, y=d.revenue_share_pct, marker_color=colors, row=2, col=1, showlegend=False,
                      text=[fmt_pct(v) for v in d.revenue_share_pct], textposition="outside", cliponaxis=False,
                      textfont=dict(color=t.ink2, size=11),
                      hovertemplate="Giảm %{x}<br>%{y:,.1f}% doanh thu ngày thường<extra></extra>")
            f.update_xaxes(showticklabels=True, row=1, col=1)
            f.update_xaxes(title_text="Mức giảm giá hiển thị", row=2, col=1)
            C.pad_y(f, d.units_per_product_day, pad=0.2, row=1, col=1)
            C.pad_y(f, d.revenue_share_pct, pad=0.3, row=2, col=1)
            f.update_yaxes(ticksuffix="%", row=2, col=1)
            C.show(f, "c42")
with right:
    with card("4.8", flt, subtitle="Mỗi cột = khoảng 5%. Hai đầu gộp ≤ −50% và ≥ +50%. "
                                   "Xanh = giảm giá, đỏ = tăng giá. Đọc gold F3 (từng lần đổi giá).") as df:
        if df is not None:
            d = df.copy()
            d["mid"] = d.change_pct_bucket + 2.5
            d["lbl"] = [("≤ −50%" if b <= -50 else ("≥ +50%" if b >= 50 else f"{int(b):+d}% … {int(b) + 5:+d}%"))
                        for b in d.change_pct_bucket]
            f = C.fig(height=440)
            f.add_bar(x=d.mid, y=d.price_down, name="Giảm giá", marker_color=DOWN, width=4.6, customdata=d[["lbl"]],
                      hovertemplate="%{customdata[0]}<br>Giảm giá: %{y:,.0f} lần<extra></extra>")
            f.add_bar(x=d.mid, y=d.price_up, name="Tăng giá", marker_color=UP, width=4.6, customdata=d[["lbl"]],
                      hovertemplate="%{customdata[0]}<br>Tăng giá: %{y:,.0f} lần<extra></extra>")
            f.add_scatter(x=d.mid, y=d.inflated, name="trong đó nghi giảm giá ảo", mode="markers",
                          marker=dict(size=8, color=theme.STATUS["critical"], symbol="diamond"), customdata=d[["lbl"]],
                          hovertemplate="%{customdata[0]}<br>Nghi giảm giá ảo: %{y:,.0f}<extra></extra>")
            f.update_layout(barmode="overlay")
            C.ref_line(f, x=0)
            f.update_xaxes(ticksuffix="%", dtick=10, title_text="% thay đổi giá bán", range=[-52, 57])
            f.update_yaxes(title_text="Số lần đổi giá")
            C.show(f, "c48")

# ---- [4.3] bảng điểm ngày sale | [4.4] verdict
with card("4.3", flt, subtitle="Tham gia = sản phẩm ghi giảm ≥ 10% trong ngày sale. \"Giảm thật\" = giá bán thấp hơn giá bình thường "
                               "30 ngày trước ≥ 5%. Thấp nhất 30 ngày = chuẩn Omnibus của EU. 10.10 chỉ có ~2 ngày nền "
                               "nên giá tham chiếu kém tin cậy hơn.") as df:
    if df is not None:
        d = df.copy()
        d["median_uplift_x"] = d.median_uplift.map(lambda v: "—" if missing(v) else f"×{fmt_num(v, 2)}")
        table(d, {
            "sale_event": ("Sự kiện", "text"), "event_date": ("Ngày", "date"),
            "products_on_event_day": ("SP có mặt", "int"), "participants": ("SP tham gia", "int"),
            "participation_pct": ("% tham gia", "pct"), "avg_shown_discount_pct": ("% giảm hiển thị", "pct"),
            "avg_real_discount_pct": ("% giảm thật", "pct"), "real_deal_pct": ("% giảm thật ≥ 5%", "pct"),
            "lowest_30d_pct": ("% giá thấp nhất 30 ngày", "pct"), "fake_discount_products": ("SP giảm giá ảo", "int"),
            "fake_discount_pct": ("% giảm giá ảo", "pct"), "units_event_day": ("Bán ngày sale", "int"),
            "units_next_day": ("Bán ngày sau", "int"), "revenue_event_day": ("Doanh thu ngày sale", "vndc"),
            "median_uplift_x": ("Uplift trung vị", "text"), "avg_baseline_days": ("Ngày nền TB", "num1"),
        })

with card("4.4", flt, subtitle="Tỉ lệ sản phẩm theo kết luận khuyến mãi trong mỗi ngày sale (tổng = 100%). "
                               "Bảng dưới: uplift trung vị = bán ngày sale / bán TB ngày thường.") as df:
    if df is not None:
        events = sorted(df.sale_event.unique(), key=lambda e: tuple(int(x) for x in e.split(".")[::-1]), reverse=True)
        verdicts = [v for v in theme.VERDICT_ORDER if v in set(df.discount_verdict)] + \
                   [v for v in df.discount_verdict.unique() if v not in theme.VERDICT_ORDER]
        f = C.fig(height=60 * len(events) + 120)
        for v in verdicts:
            sub = df[df.discount_verdict == v].set_index("sale_event").reindex(events)
            pct = sub.products_pct.fillna(0)
            dark_text = v in ("giảm danh nghĩa", "không đủ dữ liệu") or (v == "không giảm" and t.mode == "light")
            f.add_bar(y=[f"Sale {e}" for e in events], x=pct, name=v, orientation="h",
                      marker_color=VERDICT.get(v, t.neutral),
                      text=[fmt_pct(p, 0) if p >= 4 else "" for p in pct], textposition="inside", insidetextanchor="middle",
                      textfont=dict(color="#0b0b0b" if dark_text else "#ffffff", size=12),
                      customdata=sub[["products", "avg_shown_discount_pct", "avg_real_discount_pct", "median_uplift"]].fillna(0),
                      hovertemplate=f"<b>{v}</b> · " + "%{y}<br>%{x:,.1f}% sản phẩm (%{customdata[0]:,.0f})"
                                    "<br>% giảm hiển thị TB: %{customdata[1]:,.1f}%<br>% giảm thật TB: %{customdata[2]:,.1f}%"
                                    "<br>Uplift trung vị: ×%{customdata[3]:,.2f}<extra></extra>")
        f.update_layout(barmode="stack", legend=dict(traceorder="normal"))
        f.update_xaxes(range=[0, 100], ticksuffix="%", showgrid=False)
        f.update_yaxes(showgrid=False)
        C.show(f, "c44")
        piv = df.pivot_table(index="discount_verdict", columns="sale_event", values="median_uplift").reindex(verdicts)
        piv = piv[[e for e in events if e in piv.columns]]
        piv.columns = [f"Uplift {c}" for c in piv.columns]
        st.dataframe(piv.map(lambda v: "—" if missing(v) else f"×{fmt_num(v, 2)}").reset_index()
                     .rename(columns={"discount_verdict": "Kết luận"}), hide_index=True, width="stretch")

# ---- Phân tích một ngày sale: [4.5] [4.6] [4.7]
section("Phân tích một ngày sale", "Chọn sự kiện để xem chi tiết theo danh mục, seller và sản phẩm.")
filters.event_picker()
flt = filters.current()
ev = flt["sale_event"]

left, right = st.columns(2)
with left:
    with card("4.5", flt, subtitle=f"Sale {ev}: khoảng cách giữa hai điểm = phần giảm \"trên giấy\". "
                                   "Chỉ tính sản phẩm ghi giảm ≥ 10%, danh mục có ≥ 20 sản phẩm tham gia.") as df:
        if df is not None:
            d = df.sort_values("gap_pct_points", ascending=False)
            f = C.fig(height=max(300, 28 * len(d) + 110))
            for _, r in d.iterrows():
                f.add_scatter(x=[r.avg_real_discount_pct, r.avg_shown_discount_pct], y=[r.category_level_1] * 2,
                              mode="lines", line=dict(color=t.axis, width=3), hoverinfo="skip", showlegend=False)
            f.add_scatter(x=d.avg_real_discount_pct, y=d.category_level_1, mode="markers", name="% giảm thật (so với giá 30 ngày)",
                          marker=dict(size=11, color=t.series[0], line=dict(color=t.surface, width=2)),
                          hovertemplate="<b>%{y}</b><br>% giảm thật: %{x:,.1f}%<extra></extra>")
            f.add_scatter(x=d.avg_shown_discount_pct, y=d.category_level_1, mode="markers", name="% giảm hiển thị",
                          marker=dict(size=11, color=t.series[1], line=dict(color=t.surface, width=2)),
                          customdata=d[["gap_pct_points", "participants", "fake_discount_pct"]],
                          hovertemplate="<b>%{y}</b><br>% giảm hiển thị: %{x:,.1f}%<br>Chênh lệch: %{customdata[0]:,.1f} điểm %"
                                        "<br>SP tham gia: %{customdata[1]:,.0f}<br>% giảm giá ảo: %{customdata[2]:,.1f}%<extra></extra>")
            f.update_yaxes(autorange="reversed", showgrid=False)
            f.update_xaxes(ticksuffix="%", showgrid=True, gridcolor=t.grid, title_text="% giảm giá trung bình")
            C.show(f, "c45")
with right:
    with card("4.6", flt, subtitle=f"Sale {ev}: 15 seller có nhiều sản phẩm bị kết luận \"giảm giá ảo\" nhất.") as df:
        if df is not None:
            d = df.copy()
            d["label"] = d.seller_name + d.badge.map(lambda b: " ✓" if b == "official" else "")
            f = C.bar_h(d, "label", "fake_discount_products",
                        text=[f"{fmt_num(n)} SP · {fmt_pct(p)}" for n, p in zip(d.fake_discount_products, d.fake_discount_pct)],
                        customdata=d[["products_on_event_day", "avg_list_inflation_pct", "avg_real_discount_pct", "fake_discount_revenue"]],
                        hover="<b>%{y}</b><br>SP giảm giá ảo: %{x:,.0f} / %{customdata[0]:,.0f}"
                              "<br>Giá gốc bị nâng TB: %{customdata[1]:+,.1f}%<br>% giảm thật TB: %{customdata[2]:,.1f}%"
                              "<br>Doanh thu từ các SP này: %{customdata[3]:,.0f} ₫<extra></extra>",
                        x_title="Số sản phẩm giảm giá ảo")
            C.pad_x(f, d.fake_discount_products, pad=0.35)
            C.show(f, "c46")
            st.caption("✓ = shop official. Nhãn = số sản phẩm giảm giá ảo · tỉ lệ trên số sản phẩm của seller trong ngày sale.")

with card("4.7", flt, subtitle=f"Sale {ev}: 100 sản phẩm giảm giá ảo có doanh thu cao nhất. "
                               "Chọn một dòng để mở trang chi tiết sản phẩm.") as df:
    if df is not None:
        key = f"t47_{ev}_{flt['category_l1']}"
        table(df, {
            "product_id": ("ID", "text"), "title": ("Sản phẩm", "text"), "category_level_1": ("Danh mục", "text"),
            "brand_name": ("Thương hiệu", "text"), "seller_name": ("Seller", "text"),
            "ref_list_median_30d": ("Giá gốc thường", "vnd"), "event_list_price": ("Giá gốc ngày sale", "vnd"),
            "list_inflation_pct": ("Giá gốc nâng", "pct_signed"), "shown_discount_pct": ("% giảm hiển thị", "pct"),
            "ref_sell_median_30d": ("Giá bán thường", "vnd"), "event_sell_price": ("Giá bán ngày sale", "vnd"),
            "real_discount_pct": ("% giảm thật", "pct_signed"), "units_event_day": ("Bán ngày sale", "int"),
            "revenue_event_day": ("Doanh thu", "vndc"),
        }, key=key, on_select=partial(filters.pick_product, key, list(df.product_id), True), height=420)
