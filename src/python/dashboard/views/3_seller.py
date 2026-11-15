"""Dashboard 3: Người bán (03_seller.sql, [3.1]-[3.6])."""
import pandas as pd
import streamlit as st

from core import charts as C
from core import filters, theme
from core.components import (card, color_sign, fmt_count, fmt_num, fmt_pct, fmt_vnd, highlight_over, kpi, page_header,
                             table)

page_header(3, "Người bán")
flt = filters.filter_bar(dates=True, category=True,
                         note="Lọc danh mục theo danh mục chính của seller trong kỳ. Bỏ nhóm \"Không rõ người bán\".")
t = theme.tokens()

# ---- [3.1] KPI
with card("3.1", flt, border=False) as df:
    if df is not None:
        r = df.iloc[0]
        c = st.columns(4)
        kpi(c[0], "Seller hoạt động", fmt_count(r.active_sellers),
            help=f"{fmt_count(r.sellers_with_sales)} seller có doanh thu trong kỳ")
        kpi(c[1], "Seller official", fmt_pct(r.official_sellers_pct),
            help=f"{fmt_count(r.official_sellers)} shop official / chính hãng")
        kpi(c[2], "Thị phần của official", fmt_pct(r.official_revenue_share_pct),
            help="% doanh thu của shop official trong tổng doanh thu seller")
        kpi(c[3], "Rating seller TB", f"{fmt_num(r.avg_seller_rating, 2)} ★", help="Bỏ seller chưa có đánh giá")
        c = st.columns(4)
        kpi(c[0], "Doanh thu trung vị / seller", fmt_vnd(r.median_seller_revenue),
            help="Trung vị (không bị kéo lệch bởi vài seller lớn)")
        kpi(c[1], "Doanh thu TB / seller có bán", fmt_vnd(r.revenue_per_selling_seller))
        kpi(c[2], "Sản phẩm TB / seller", fmt_num(r.avg_products_per_seller, 1), help="Số sản phẩm quan sát TB mỗi ngày")
        kpi(c[3], "Seller có doanh thu", fmt_count(r.sellers_with_sales))

# ---- [3.2] official vs thường | [3.5] độ tập trung
left, right = st.columns([1.15, 1])
with left:
    with card("3.2", flt, subtitle="So sánh hai nhóm seller trên cùng thước đo.") as df:
        if df is not None:
            d = df.set_index("seller_type")
            rows = [
                ("Số seller", "sellers", lambda v: fmt_count(v)),
                ("Thị phần doanh thu", "revenue_share_pct", fmt_pct),
                ("Doanh thu / seller / ngày", "revenue_per_seller_per_day", fmt_vnd),
                ("Sản phẩm / seller", "products_per_seller", lambda v: fmt_num(v, 1)),
                ("Giá bán bình quân", "avg_selling_price", fmt_vnd),
                ("Bán TB / sản phẩm / ngày", "units_per_product_day", lambda v: fmt_num(v, 2)),
                ("% giảm giá TB", "avg_discount_pct", fmt_pct),
                ("Rating sản phẩm TB", "avg_product_rating", lambda v: f"{fmt_num(v, 2)} ★"),
                ("Rating seller TB", "avg_seller_rating", lambda v: f"{fmt_num(v, 2)} ★"),
                ("Nghi giảm giá ảo / 1.000 lần đổi giá", "inflated_per_1k_changes", lambda v: fmt_num(v, 1)),
            ]
            comp = pd.DataFrame({"Chỉ số": [r[0] for r in rows]})
            for typ in d.index:
                comp[typ] = [fn(d.loc[typ, col]) for _, col, fn in rows]
            st.dataframe(comp, hide_index=True, width="stretch", height=38 + 35 * len(comp))
            colors = {typ: (t.series[0] if typ.startswith("Official") else t.neutral) for typ in d.index}
            f = C.bar_h(df, "seller_type", "revenue_per_seller_per_day", height=150,
                        color=[colors[x] for x in df.seller_type],
                        text=[fmt_vnd(v) for v in df.revenue_per_seller_per_day],
                        hover="<b>%{y}</b><br>Doanh thu / seller / ngày: %{x:,.0f} ₫<extra></extra>",
                        x_title="Doanh thu / seller / ngày (₫)")
            C.pad_x(f, df.revenue_per_seller_per_day, pad=0.3)
            C.show(f, "c32")
with right:
    with card("3.5", flt, subtitle="Top x% seller (xếp theo doanh thu) chiếm bao nhiêu % doanh thu. "
                                   "Gini: 0 = đều nhau, 1 = một seller chiếm hết.") as df:
        if df is not None:
            r = df.iloc[0]
            c = st.columns(2)
            kpi(c[0], "Hệ số Gini", fmt_num(r.gini, 3))
            kpi(c[1], "Số seller xét", fmt_count(r.sellers))
            labels = ["Top 1%", "Top 5%", "Top 10%", "Top 20%", "Top 50%"]
            vals = [r.top_1pct_share, r.top_5pct_share, r.top_10pct_share, r.top_20pct_share, r.top_50pct_share]
            d = pd.DataFrame({"group": labels, "share": vals})
            f = C.column(d, "group", "share", height=300, color=theme.ordinal(5)[::-1],
                         text=[fmt_pct(v) for v in vals], y_title="% doanh thu",
                         hover="%{x} seller → %{y:,.1f}% doanh thu<extra></extra>")
            f.update_yaxes(ticksuffix="%", range=[0, 108])
            C.show(f, "c35")

# ---- [3.3] top 20 seller
with card("3.3", flt, subtitle="Cột review lấy từ scorecard sản phẩm (toàn kỳ). % review tiêu cực ≥ 15% được tô đỏ.") as df:
    if df is not None:
        d = df.copy()
        d["badge"] = d.badge.map(lambda b: "✓" if b == "official" else "")
        table(d, {
            "rank": ("#", "int"), "seller_name": ("Seller", "text"), "badge": ("Official", "text"),
            "main_category": ("Danh mục chính", "text"), "revenue": ("Doanh thu", "vndc"),
            "market_share_pct": ("Thị phần", "share"), "growth_pct": ("Tăng trưởng", "pct_signed"),
            "units": ("Số bán", "count"), "products_per_day": ("SP/ngày", "int"),
            "avg_discount_pct": ("% giảm", "pct"), "seller_rating": ("Rating seller", "num1"),
            "seller_rating_cnt": ("Lượt đánh giá", "count"), "new_seller_ratings": ("Đánh giá mới", "int"),
            "avg_review_star": ("Sao review TB", "star"), "negative_review_pct": ("% review tiêu cực", "pct"),
            "inflated_events": ("Nghi giảm giá ảo", "int"),
        }, style=lambda s: highlight_over(color_sign(s, ["growth_pct"]), "negative_review_pct", 15),
            height=38 + 35 * len(d))

# ---- [3.4] rating 5 seller lớn nhất
with card("3.4", flt, subtitle="Rating là chỉ số không cộng được nên vẽ riêng từng seller. Panel dưới: lượt đánh giá mới mỗi ngày.") as df:
    if df is not None:
        d = df.copy()
        d["full_date"] = pd.to_datetime(d.full_date)
        order = d.groupby("seller_name").revenue_est.sum().sort_values(ascending=False).index.tolist()
        colors = theme.entity_colors(order)
        f = C.two_panel("Rating seller cuối ngày (★)", "Lượt đánh giá mới / ngày", height=470, ratio=0.6)
        for name in order:
            s = d[d.seller_name == name]
            rated = s[s.seller_rate_cnt_eod > 0]
            f.add_scatter(x=rated.full_date, y=rated.seller_rate_eod, name=name, mode="lines+markers", legendgroup=name,
                          line=dict(color=colors[name]), marker=dict(size=6),
                          hovertemplate=f"<b>{name}</b><br>" + "%{x|%d/%m}: %{y:,.2f} ★<extra></extra>", row=1, col=1)
            f.add_bar(x=s.full_date, y=s.new_seller_ratings, name=name, legendgroup=name, showlegend=False,
                      marker_color=colors[name],
                      hovertemplate=f"<b>{name}</b><br>" + "%{x|%d/%m}: %{y:,.0f} đánh giá mới<extra></extra>", row=2, col=1)
        f.update_layout(barmode="stack", legend=dict(x=0, xanchor="left", y=-0.12, yanchor="top"),
                        margin=dict(l=8, r=16, t=40, b=60))
        C.mark_sale_days(f, d.full_date, rows=[1, 2])
        C.date_axis(f)
        C.show(f, "c34")

# ---- [3.6] seller rủi ro
with card("3.6", flt, subtitle="Điểm rủi ro 0–3, mỗi dấu hiệu 1 điểm: ≥ 3 lần nghi giảm giá ảo · ≥ 15% review 1–2 sao "
                               "(từ 30 review) · rating seller giảm ≥ 0,2 trong kỳ. Tối đa 50 seller.") as df:
    if df is not None:
        d = df.copy()
        flag = {3: "🔴 3 · cao", 2: "🟠 2 · vừa", 1: "🟡 1 · thấp"}
        d["risk"] = d.risk_score.map(lambda s: flag.get(int(s), str(s)))
        d["badge"] = d.badge.map(lambda b: "✓" if b == "official" else "")
        c = st.columns(3)
        for col, score in zip(c, (3, 2, 1)):
            n = int((d.risk_score == score).sum())
            kpi(col, flag[score], f"{n} seller")
        table(d, {
            "risk": ("Rủi ro", "text"), "seller_name": ("Seller", "text"), "badge": ("Official", "text"),
            "reasons": ("Dấu hiệu", "text"), "revenue": ("Doanh thu", "vndc"),
            "inflated_events": ("Nghi giảm giá ảo", "int"), "price_changes": ("Lần đổi giá", "int"),
            "rated_reviews": ("Review có sao", "int"), "negative_review_pct": ("% review tiêu cực", "pct"),
            "rating_first": ("Rating đầu kỳ", "num2"), "rating_last": ("Rating cuối kỳ", "num2"),
            "rating_change": ("Thay đổi rating", "num2"),
        }, style=lambda s: highlight_over(color_sign(s, ["rating_change"]), "negative_review_pct", 15),
            height=min(38 + 35 * len(d), 560))
