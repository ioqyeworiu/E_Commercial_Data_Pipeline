"""Dashboard 6: Sản phẩm, danh sách + chi tiết một sản phẩm (06_product.sql, [6.1]-[6.8])."""
import html
from functools import partial

import pandas as pd
import streamlit as st

from core import charts as C
from core import filters, theme
from core.components import (card, card_header, color_sign, data_expander, fmt_count, fmt_num, fmt_pct, fmt_vnd,
                             growth_delta, highlight_over, kpi, missing, page_header, section, table)
from core.queries import run

page_header(6, "Sản phẩm")
flt = filters.filter_bar(dates=True, category=True,
                         note="Danh mục lọc 3 danh sách ; khoảng ngày chỉ áp dụng cho biểu đồ giá & số bán [6.5].")
t = theme.tokens()
cat = flt["category_l1"]

# ---------------------------------------------------------------------------------------------------
# Danh sách [6.1] [6.2] [6.3]: chọn một dòng để mở chi tiết bên dưới
# ---------------------------------------------------------------------------------------------------
section("Danh sách sản phẩm", "Tính trên toàn kỳ đã nạp. Chọn một dòng trong bảng để xem chi tiết sản phẩm ở phần dưới.")
tab1, tab2, tab3 = st.tabs([":material/trophy: Bán chạy nhất", ":material/trending_up: Đang lên", ":material/warning: Doanh thu cao nhưng bị chê"])


def _product_table(qid: str, spec: dict, style=None, subtitle: str | None = None):
    with card(qid, flt, subtitle=subtitle) as df:
        if df is not None:
            key = f"t{qid.replace('.', '')}_{cat or 'all'}"
            table(df, spec, key=key, on_select=partial(filters.pick_product, key, list(df.product_id)),
                  style=style, height=min(38 + 35 * len(df), 460))


with tab1:
    _product_table("6.1", {
        "revenue_rank": ("#", "int"), "product_id": ("ID", "text"), "title": ("Sản phẩm", "text"),
        "category_level_1": ("Danh mục", "text"), "brand_name": ("Thương hiệu", "text"), "seller_name": ("Seller", "text"),
        "current_sell_price": ("Giá hiện tại", "vnd"), "current_discount_pct": ("% giảm", "pct"),
        "units_sold_total": ("Số bán", "int"), "revenue_total": ("Doanh thu", "vndc"),
        "cum_share_pct": ("% DT lũy kế", "pct"), "abc_class": ("ABC", "text"), "sales_trend": ("Xu hướng", "text"),
        "avg_star": ("Sao TB", "star"), "n_reviews": ("Review", "int"), "inflated_events": ("Nghi giảm giá ảo", "int"),
    })
with tab2:
    _product_table("6.2", {
        "product_id": ("ID", "text"), "title": ("Sản phẩm", "text"), "category_level_1": ("Danh mục", "text"),
        "brand_name": ("Thương hiệu", "text"), "units_prev_7d": ("Bán 7 ngày trước", "int"),
        "units_last_7d": ("Bán 7 ngày gần nhất", "int"), "growth_7d_pct": ("Tăng trưởng 7 ngày", "pct_signed"),
        "current_sell_price": ("Giá hiện tại", "vnd"), "current_discount_pct": ("% giảm", "pct"),
        "avg_star": ("Sao TB", "star"), "as_of_date": ("Tính tới", "date"),
    }, style=lambda s: color_sign(s, ["growth_7d_pct"]),
        subtitle="Tối thiểu 50 sản phẩm bán ở 7 ngày trước. Nếu 7 ngày gần nhất chứa ngày sale thì gần như mọi sản phẩm đều \"tăng\".")
with tab3:
    _product_table("6.3", {
        "product_id": ("ID", "text"), "title": ("Sản phẩm", "text"), "category_level_1": ("Danh mục", "text"),
        "brand_name": ("Thương hiệu", "text"), "seller_name": ("Seller", "text"), "revenue_total": ("Doanh thu", "vndc"),
        "revenue_rank": ("Hạng DT", "int"), "n_rated_reviews": ("Review có sao", "int"), "avg_star": ("Sao TB", "star"),
        "negative_pct": ("% tiêu cực", "pct"), "negative_reviews": ("Review 1–2 sao", "int"),
        "n_with_image": ("Review có ảnh", "int"), "rating_latest": ("Rating trên Tiki", "num1"),
    }, style=lambda s: highlight_over(s, "negative_pct", 15),
        subtitle="Nhóm A (tạo 80% doanh thu) có sao TB < 4 hoặc ≥ 15% review 1–2 sao (tối thiểu 10 review có sao).")

# ---------------------------------------------------------------------------------------------------
# Chi tiết một sản phẩm [6.4]-[6.8]
# ---------------------------------------------------------------------------------------------------
section("Chi tiết sản phẩm", "Tìm theo tên hoặc product_id, hoặc chọn một dòng ở danh sách phía trên.")
with st.container(border=True):
    filters.product_picker()
flt = filters.current()
pid = flt["product_id"]
if pid is None:
    st.info("Chưa chọn sản phẩm.")
    st.stop()

# ---- [6.4] scorecard
with st.container(border=True, key="card-6-4"):
    card_header("6.4")
    try:
        sc = run("6.4", flt)
    except Exception as exc:
        sc = None
        st.error(f"Không chạy được truy vấn [6.4]: {exc}")
    if sc is not None and sc.empty:
        st.info(f"Không có sản phẩm {pid} trong mart_product_scorecard.")
    elif sc is not None:
        r = sc.iloc[0]
        pills = [f"<span class='pill'>Nhóm {html.escape(str(r.abc_class))}</span>",
                 f"<span class='pill'>Xu hướng: {html.escape(str(r.sales_trend))}</span>",
                 f"<span class='pill'>Phân khúc {html.escape(str(r.price_band).split('. ', 1)[-1])}</span>"]
        if r.seller_is_official:
            pills.insert(0, "<span class='pill official'>Official</span>")
        if int(r.n_versions) > 1:
            pills.append(f"<span class='pill'>{int(r.n_versions)} phiên bản thuộc tính</span>")
        meta = " · ".join(html.escape(str(x)) for x in (r.category_path, r.brand_name, r.seller_name) if x)
        extra = " · ".join(html.escape(str(x)) for x in (
            f"Xuất xứ: {r.made_in}" if r.made_in else "", f"Bảo hành: {r.warranty}" if r.warranty else "",
            f"Quan sát {r.first_seen_date:%d/%m} – {r.last_seen_date:%d/%m/%Y} ({int(r.days_observed)} ngày)") if x)
        st.markdown(f"<div class='prod-title'>{html.escape(str(r.title))}</div>"
                    f"<div class='prod-meta'>ID {int(r.product_id)} · {meta}</div>"
                    f"<div class='prod-meta'>{extra}</div><div style='margin:.35rem 0 .6rem 0'>{''.join(pills)}</div>",
                    unsafe_allow_html=True)
        c = st.columns(4)
        disc = f"−{fmt_pct(r.current_discount_pct, 0)} so với giá gốc {fmt_vnd(r.current_list_price)}" \
            if not missing(r.current_discount_pct) and r.current_discount_pct > 0 else None
        kpi(c[0], "Giá hiện tại", fmt_vnd(r.current_sell_price), disc, delta_color="off",
            help=f"Thấp nhất {fmt_vnd(r.min_sell_price)} · cao nhất {fmt_vnd(r.max_sell_price)} · TB {fmt_vnd(r.avg_sell_price)}")
        kpi(c[1], "Doanh thu toàn kỳ", fmt_vnd(r.revenue_total), f"hạng #{fmt_num(r.revenue_rank)}", delta_color="off",
            help=f"Tính tới hết sản phẩm này, top sản phẩm tạo {fmt_pct(r.revenue_cum_share_pct)} doanh thu")
        kpi(c[2], "Số bán toàn kỳ", fmt_count(r.units_sold_total), f"{fmt_num(r.avg_daily_units, 1)} / ngày",
            delta_color="off", help=f"Lũy kế \"đã bán\" trên Tiki: {fmt_count(r.sold_cum_latest)}")
        kpi(c[3], "Bán 7 ngày gần nhất", fmt_count(r.units_last_7d), growth_delta(r.growth_7d_pct),
            delta_desc="so với 7 ngày trước", help=f"Tính tới {r.as_of_date:%d/%m/%Y}")
        c = st.columns(4)
        kpi(c[0], "Rating trên Tiki", f"{fmt_num(r.rating_latest, 1)} ★", f"{fmt_count(r.rating_cnt_latest)} lượt",
            delta_color="off")
        kpi(c[1], "Sao TB từ review", "—" if missing(r.avg_star) else f"{fmt_num(r.avg_star, 2)} ★",
            f"{fmt_count(r.n_rated_reviews)} review có sao", delta_color="off")
        neg = r.negative_pct
        kpi(c[2], "% review tiêu cực", fmt_pct(neg),
            None if missing(neg) else ("⚠ cao (≥ 15%)" if neg >= 15 else "dưới ngưỡng 15%"),
            delta_color="red" if not missing(neg) and neg >= 15 else "off")
        kpi(c[3], "Lần đổi giá", fmt_num(r.price_changes),
            f"{fmt_num(r.inflated_events)} lần nghi giảm giá ảo" if r.inflated_events else "không có giảm giá ảo",
            delta_color="red" if r.inflated_events else "off",
            help=f"Tăng {fmt_num(r.price_up)} · giảm {fmt_num(r.price_down)}")
        if r.n_rated_reviews:
            stars = pd.DataFrame({"s": [5, 4, 3, 2, 1], "n": [r.star_5, r.star_4, r.star_3, r.star_2, r.star_1]})
            stars["pct"] = stars.n / stars.n.sum() * 100
            star_color = {5: t.series[0], 4: t.seq[2], 3: t.neutral, 2: t.red_seq[1], 1: t.red_seq[3]}
            stars["label"] = stars.s.map(lambda s: f"{s} ★")
            f = C.bar_h(stars, "label", "pct", color=[star_color[s] for s in stars.s], height=200,
                        text=[f"{fmt_pct(p)} · {fmt_num(n)}" for p, n in zip(stars.pct, stars.n)],
                        hover="<b>%{y}</b>: %{x:,.1f}% review<extra></extra>")
            f.update_xaxes(ticksuffix="%", showgrid=False, showticklabels=False)
            C.pad_x(f, stars.pct, pad=0.35)
            st.markdown("<div class='note'>Phân bố sao của review sản phẩm</div>", unsafe_allow_html=True)
            C.show(f, "c64stars")
        data_expander("6.4", sc.T.reset_index().set_axis(["Cột", "Giá trị"], axis=1).astype(str))

# ---- [6.5] giá & số bán theo ngày
with card("6.5", flt, subtitle="Panel trên: giá bán đóng cửa, vùng nhạt = min–max trong ngày, đường xám = giá gốc ; "
                               "chấm = ngày có đổi giá. Panel dưới: số bán ước tính. Theo khoảng ngày đang chọn.") as df:
    if df is not None:
        x = pd.to_datetime(df.full_date)
        f = C.two_panel("Giá (₫)", "Số bán ước tính / ngày", height=480, ratio=0.62)
        band = "rgba(42,120,214,0.14)" if t.mode == "light" else "rgba(57,135,229,0.22)"
        f.add_scatter(x=x, y=df.max_sell_price, mode="lines", line=dict(width=0), hoverinfo="skip", showlegend=False, row=1, col=1)
        f.add_scatter(x=x, y=df.min_sell_price, mode="lines", line=dict(width=0), fill="tonexty", fillcolor=band,
                      name="Min–max trong ngày", row=1, col=1, customdata=df[["max_sell_price"]],
                      hovertemplate="Min–max: %{y:,.0f} – %{customdata[0]:,.0f} ₫<extra></extra>")
        f.add_scatter(x=x, y=df.close_list_price, mode="lines", name="Giá gốc", line=dict(color=t.neutral, width=2),
                      row=1, col=1, hovertemplate="Giá gốc: %{y:,.0f} ₫<extra></extra>")
        f.add_scatter(x=x, y=df.close_sell_price, mode="lines", name="Giá bán đóng cửa", line=dict(color=t.series[0]),
                      row=1, col=1, customdata=df[["avg_discount_pct", "price_change_cnt"]],
                      hovertemplate="Giá bán: %{y:,.0f} ₫<br>% giảm TB: %{customdata[0]:,.1f}%"
                                    "<br>Số lần đổi giá: %{customdata[1]:,.0f}<extra></extra>")
        chg = df[df.price_change_cnt > 0]
        if len(chg):
            f.add_scatter(x=pd.to_datetime(chg.full_date), y=chg.close_sell_price, mode="markers", name="Ngày đổi giá",
                          marker=dict(size=9, color=t.series[0], line=dict(color=t.surface, width=2)), row=1, col=1,
                          hoverinfo="skip")
        f.add_bar(x=x, y=df.units_sold_est, name="Số bán", marker_color=t.seq[3], row=2, col=1, showlegend=False,
                  customdata=df[["revenue_est", "n_observations"]],
                  hovertemplate="Số bán: %{y:,.0f}<br>Doanh thu: %{customdata[0]:,.0f} ₫"
                                "<br>Số lần crawl: %{customdata[1]:,.0f}<extra></extra>")
        f.update_layout(hovermode="x unified")
        C.mark_sale_days(f, x, rows=[1, 2])
        C.date_axis(f)
        C.show(f, "c65")

# ---- [6.6] nhật ký đổi giá | [6.7] lịch sử thuộc tính
left, right = st.columns([1.25, 1])
with left:
    with card("6.6", flt, subtitle="Toàn bộ các lần đổi giá (bỏ đổi giá giả do nguồn thiếu giá). Dòng tô đỏ = nghi giảm giá ảo.") as df:
        if df is not None:
            d = df.copy()
            d["flag"] = d.is_list_price_inflated.map(lambda b: "⚠ giảm giá ảo" if b else "")
            d["dir"] = d.direction.map({"up": "↑ tăng", "down": "↓ giảm"}).fillna(d.direction)
            bg = "rgba(208,59,59,0.12)" if t.mode == "light" else "rgba(208,59,59,0.22)"

            def _rows(sty):
                flags = d.is_list_price_inflated.tolist()
                return sty.apply(lambda row: [f"background-color: {bg}" if flags[row.name] else "" for _ in row], axis=1)

            table(d, {
                "changed_at": ("Thời điểm", "datetime"), "dir": ("Chiều", "text"),
                "old_sell_price": ("Giá cũ", "vnd"), "new_sell_price": ("Giá mới", "vnd"),
                "change_pct": ("% đổi", "pct_signed"), "old_list_price": ("Giá gốc cũ", "vnd"),
                "new_list_price": ("Giá gốc mới", "vnd"), "new_discount_pct": ("% giảm mới", "num1"),
                "old_price_held_hours": ("Giá cũ giữ (giờ)", "num1"), "flag": ("Cờ", "text"),
            }, style=lambda s: color_sign(_rows(s), ["change_pct"]), height=min(38 + 35 * len(d), 420))
with right:
    with card("6.7", flt, subtitle="Mỗi thanh = một phiên bản (SCD Type 2) của tiêu đề / xuất xứ / bảo hành.") as df:
        if df is not None:
            d = df.copy()
            d["valid_from"] = pd.to_datetime(d.valid_from).dt.tz_localize(None).astype("datetime64[ns]")
            end = pd.to_datetime(d.valid_to).dt.tz_localize(None).astype("datetime64[ns]")
            d["valid_end"] = end.fillna(pd.Timestamp.now())
            d["ver"] = d.version_no.map(lambda v: f"v{int(v)}")
            f = C.fig(height=max(160, 46 * len(d) + 90))
            f.add_bar(y=d.ver, x=(d.valid_end - d.valid_from).dt.total_seconds() * 1000, base=d.valid_from,
                      orientation="h", marker_color=[t.series[0] if c else t.neutral for c in d.is_current],
                      customdata=d[["title", "made_in", "warranty", "days_valid"]],
                      hovertemplate="<b>%{y}</b> · %{customdata[3]:,.1f} ngày<br>%{customdata[0]}"
                                    "<br>Xuất xứ: %{customdata[1]} · Bảo hành: %{customdata[2]}<extra></extra>")
            f.update_xaxes(type="date", tickformat="%d/%m", showgrid=True, gridcolor=t.grid)
            f.update_yaxes(autorange="reversed", showgrid=False)
            f.update_layout(showlegend=False)
            C.show(f, "c67")
            table(d, {"ver": ("Bản", "text"), "title": ("Tiêu đề", "text"), "made_in": ("Xuất xứ", "text"),
                      "warranty": ("Bảo hành", "text"), "valid_from": ("Từ", "datetime"),
                      "is_current": ("Hiện tại", "bool")})

# ---- [6.8] trong các ngày sale
with card("6.8", flt, subtitle="Giá tham chiếu = trung vị giá 30 ngày trước sự kiện. Uplift = bán ngày sale / bán TB ngày thường.") as df:
    if df is not None:
        d = df.copy()
        vc = theme.verdict_colors(t)
        d["uplift"] = d.uplift_ratio.map(lambda v: "—" if missing(v) else f"×{fmt_num(v, 2)}")
        table(d, {
            "sale_event": ("Sự kiện", "text"), "event_date": ("Ngày", "date"), "discount_verdict": ("Kết luận", "text"),
            "ref_sell_median_30d": ("Giá bán thường", "vnd"), "ref_sell_min_30d": ("Thấp nhất 30 ngày", "vnd"),
            "event_sell_price": ("Giá ngày sale", "vnd"), "shown_discount_pct": ("% giảm hiển thị", "pct"),
            "real_discount_pct": ("% giảm thật", "pct_signed"), "ref_list_median_30d": ("Giá gốc thường", "vnd"),
            "event_list_price": ("Giá gốc ngày sale", "vnd"), "list_inflation_pct": ("Giá gốc nâng", "pct_signed"),
            "is_lowest_30d": ("Thấp nhất 30 ngày", "bool"), "baseline_units_avg": ("Bán TB ngày thường", "num1"),
            "units_event_day": ("Bán ngày sale", "int"), "units_next_day": ("Bán ngày sau", "int"), "uplift": ("Uplift", "text"),
        }, style=lambda s: s.map(lambda v: f"color: {vc[v]}; font-weight: 650" if v in ("giảm thật", "giảm giá ảo") else "",
                                 subset=["discount_verdict"]))
