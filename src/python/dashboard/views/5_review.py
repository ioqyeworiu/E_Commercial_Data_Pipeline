"""Dashboard 5: Đánh giá khách hàng (05_review.sql, [5.1]-[5.7])."""
import pandas as pd
import streamlit as st

from core import charts as C
from core import filters, theme
from core.components import card, fmt_count, fmt_num, fmt_pct, kpi, page_header, table

page_header(5, "Đánh giá khách hàng")
flt = filters.filter_bar(dates=False, category=True,
                         note="Trang này không lọc theo ngày: ngày viết review chỉ là ước tính (\"4 năm trước\") và trải từ "
                              "2019, nên trục thời gian là tháng. Tỉ lệ tính trên review có sao.")
t = theme.tokens()
NEG = t.red_seq[2]
cat = flt["category_l1"]

# ---- [5.1] KPI
with card("5.1", flt, border=False) as df:
    if df is not None:
        r = df.iloc[0]
        c = st.columns(4)
        kpi(c[0], "Số review", fmt_count(r.reviews))
        kpi(c[1], "Sao trung bình", f"{fmt_num(r.avg_star, 2)} ★")
        kpi(c[2], "% tích cực (4–5 sao)", fmt_pct(r.positive_pct))
        kpi(c[3], "% tiêu cực (1–2 sao)", fmt_pct(r.negative_pct))
        c = st.columns(4)
        kpi(c[0], "Net sentiment", fmt_num(r.net_sentiment, 1), help="% 5 sao − % 1–3 sao, thang −100 … 100")
        kpi(c[1], "% review có ảnh", fmt_pct(r.with_image_pct))
        kpi(c[2], "% review có bình luận", fmt_pct(r.with_comment_pct))
        kpi(c[3], "Số từ TB / bình luận", fmt_num(r.avg_words_per_comment, 1))

# ---- [5.2] phân bố sao | [5.5] thâm niên
left, right = st.columns(2)
with left:
    with card("5.2", flt, subtitle="Xanh = hài lòng, xám = trung tính, đỏ = không hài lòng.") as df:
        if df is not None:
            d = df.sort_values("stars", ascending=False)
            star_color = {5: t.series[0], 4: t.seq[2], 3: t.neutral, 2: t.red_seq[1], 1: t.red_seq[3]}
            d["label"] = d.stars.map(lambda s: f"{int(s)} ★")
            f = C.bar_h(d, "label", "pct", color=[star_color[int(s)] for s in d.stars],
                        text=[f"{fmt_pct(p)} · {fmt_count(n)}" for p, n in zip(d.pct, d.reviews)],
                        hover="<b>%{y}</b><br>%{x:,.1f}% review<extra></extra>", x_title="% review", height=300)
            f.update_xaxes(ticksuffix="%")
            C.pad_x(f, d.pct, pad=0.3)
            C.show(f, "c52")
with right:
    with card("5.5", flt, subtitle="Người review lâu năm có khó tính hơn không? Thâm niên tính từ ngày tạo tài khoản.") as df:
        if df is not None:
            d = df.copy()
            colors = theme.ordinal(len(d))
            f = C.two_cols("Sao trung bình", "% review tiêu cực", height=300, widths=(0.5, 0.5), spacing=0.08)
            f.add_bar(y=d.tenure_band, x=d.avg_star, orientation="h", marker_color=colors, row=1, col=1,
                      text=[fmt_num(v, 2) for v in d.avg_star], textposition="outside", cliponaxis=False,
                      textfont=dict(color=t.ink2, size=11), customdata=d[["reviews", "reviews_pct"]],
                      hovertemplate="<b>%{y}</b><br>Sao TB: %{x:,.2f}<br>%{customdata[0]:,.0f} review "
                                    "(%{customdata[1]:,.1f}%)<extra></extra>")
            f.add_bar(y=d.tenure_band, x=d.negative_pct, orientation="h", marker_color=colors, row=1, col=2,
                      text=[fmt_pct(v) for v in d.negative_pct], textposition="outside", cliponaxis=False,
                      textfont=dict(color=t.ink2, size=11), customdata=d[["with_image_pct", "avg_words_per_comment"]],
                      hovertemplate="<b>%{y}</b><br>% tiêu cực: %{x:,.1f}%<br>% có ảnh: %{customdata[0]:,.1f}%"
                                    "<br>Số từ TB: %{customdata[1]:,.1f}<extra></extra>")
            lo = max(0.0, float(d.avg_star.min()) - 0.4)
            f.update_xaxes(range=[lo, 5.15], row=1, col=1)
            C.pad_x(f, d.negative_pct, pad=0.35, row=1, col=2)
            f.update_xaxes(ticksuffix="%", row=1, col=2)
            f.update_yaxes(autorange="reversed")
            f.update_layout(showlegend=False)
            C.show(f, "c55")
            st.caption(f"Trục sao TB bắt đầu từ {fmt_num(lo, 1)} để thấy rõ chênh lệch nhỏ.")

# ---- [5.3] theo tháng
with card("5.3", flt, subtitle="Theo tháng viết review (ước tính). Panel dưới: sao TB. "
                                   "Rê chuột vào cột để xem % review có ảnh và % tiêu cực.") as df:
    if df is not None:
        x = pd.to_datetime(df.review_month)
        f = C.two_panel("Số review / tháng", "Sao trung bình (★)", height=440, ratio=0.55)
        f.add_bar(x=x, y=df.reviews, name="Số review", marker_color=t.series[0], row=1, col=1, showlegend=False,
                  customdata=df[["with_image_pct", "negative_pct"]],
                  hovertemplate="%{x|%m/%Y}<br>Số review: %{y:,.0f}<br>% có ảnh: %{customdata[0]:,.1f}%"
                                "<br>% tiêu cực: %{customdata[1]:,.1f}%<extra></extra>")
        f.add_scatter(x=x, y=df.avg_star, name="Sao TB", mode="lines", line=dict(color=t.series[1]), row=2, col=1,
                      showlegend=False, hovertemplate="%{x|%m/%Y}<br>Sao TB: %{y:,.2f}<extra></extra>")
        f.update_xaxes(tickformat="%m/%Y", hoverformat="%m/%Y")
        C.show(f, "c53")

# ---- [5.4] theo danh mục | [5.6] brand bị chê
left, right = st.columns(2)
with left:
    with card("5.4", flt, subtitle=("Danh mục cấp 2 của " + cat if cat else "Danh mục cấp 1") +
                                   " xếp theo % review tiêu cực (chỉ nhóm ≥ 100 review có sao). "
                                   + ("" if cat else "Bấm vào một thanh để drill-down.")) as df:
        if df is not None:
            d = df.sort_values("negative_pct", ascending=False)
            f = C.bar_h(d, "category", "negative_pct", color=NEG,
                        text=[f"{fmt_pct(n)} · {fmt_num(s, 2)} ★" for n, s in zip(d.negative_pct, d.avg_star)],
                        customdata=d[["rated_reviews", "avg_star", "positive_pct", "with_image_pct"]],
                        hover="<b>%{y}</b><br>% tiêu cực: %{x:,.1f}%<br>Sao TB: %{customdata[1]:,.2f}"
                              "<br>% tích cực: %{customdata[2]:,.1f}%<br>Review có sao: %{customdata[0]:,.0f}<extra></extra>",
                        x_title="% review 1–2 sao")
            f.update_xaxes(ticksuffix="%")
            C.pad_x(f, d.negative_pct, pad=0.4)
            if cat:
                C.show(f, "c54")
                st.button(f"← Về tất cả danh mục (đang xem: {cat})", on_click=filters.clear_category, type="tertiary")
            else:
                def _drill():
                    pts = st.session_state["c54_sel"].selection.points
                    if pts:
                        st.session_state.f_cat = pts[0]["y"]
                st.plotly_chart(f, theme=None, config=C.PLOT_CONFIG, key="c54_sel", width="stretch",
                                on_select=_drill, selection_mode="points")
with right:
    with card("5.6", flt, subtitle="Xếp theo cận dưới khoảng tin cậy Wilson 95% của % tiêu cực, để brand ít review "
                                   "mà tỉ lệ cao do ngẫu nhiên không lọt lên đầu. Tối thiểu 100 review có sao.") as df:
        if df is not None:
            d = df.copy()
            f = C.fig(height=max(300, 30 * len(d) + 90))
            f.add_bar(y=d.brand_name, x=d.negative_pct_wilson_low, orientation="h", marker_color=NEG,
                      name="Cận dưới Wilson 95%", customdata=d[["rated_reviews", "avg_star", "revenue"]],
                      hovertemplate="<b>%{y}</b><br>Cận dưới Wilson: %{x:,.1f}%<br>Review có sao: %{customdata[0]:,.0f}"
                                    "<br>Sao TB: %{customdata[1]:,.2f}<extra></extra>")
            f.add_scatter(y=d.brand_name, x=d.negative_pct, mode="markers", name="% tiêu cực thực tế",
                          marker=dict(size=9, color=t.ink, symbol="line-ns-open", line=dict(width=2, color=t.ink)),
                          hovertemplate="<b>%{y}</b><br>% tiêu cực thực tế: %{x:,.1f}%<extra></extra>")
            f.update_yaxes(autorange="reversed", showgrid=False)
            f.update_xaxes(ticksuffix="%", showgrid=True, gridcolor=t.grid, title_text="% review 1–2 sao")
            C.show(f, "c56")

# ---- [5.7] rating vs bán
with card("5.7", flt, subtitle="Mỗi sản phẩm xếp vào một nhóm theo sao TB từ review. Rating cao có đi kèm bán chạy không? "
                               "(toàn kỳ, tương quan)") as df:
    if df is not None:
        d = df.copy()
        d["band"] = d.star_band.str.split(". ", n=1).str[-1]
        colors = [t.neutral] + theme.ordinal(len(d) - 1) if d.star_band.iloc[0].startswith("0.") else theme.ordinal(len(d))
        c1, c2 = st.columns([1.4, 1])
        with c1:
            f = C.column(d, "band", "avg_daily_units", height=320, color=colors,
                         text=[fmt_num(v, 2) for v in d.avg_daily_units], y_title="Bán TB / sản phẩm / ngày",
                         customdata=d[["products", "class_a_pct", "avg_price"]],
                         hover="<b>%{x}</b><br>Bán TB/ngày: %{y:,.2f}<br>Số SP: %{customdata[0]:,.0f}"
                               "<br>% thuộc nhóm A: %{customdata[1]:,.1f}%<br>Giá TB: %{customdata[2]:,.0f} ₫<extra></extra>")
            C.pad_y(f, d.avg_daily_units, pad=0.15)
            C.show(f, "c57")
        with c2:
            table(d, {"band": ("Nhóm sao", "text"), "products": ("Số SP", "int"),
                      "avg_daily_units": ("Bán TB/ngày", "num2"), "avg_revenue_per_product": ("DT TB / SP", "vndc"),
                      "class_a_pct": ("% nhóm A", "pct"), "avg_price": ("Giá TB", "vndc")})
