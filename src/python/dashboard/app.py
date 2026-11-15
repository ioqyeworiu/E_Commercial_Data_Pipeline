"""Dashboard thị trường TMĐT (dữ liệu crawl Tiki.vn) đọc từ data mart ClickHouse.

Chạy:  streamlit run app.py      (trong container: service `dashboard` của docker-compose.yml, cổng 8501)
6 trang tương ứng 6 file SQL trong src/sql_script/clickhouse/query_dashboard/.
"""
from __future__ import annotations

import logging

import streamlit as st

# Bộ lọc ghi giá trị widget qua session_state (preset ngày, drill-down) -> Streamlit log cảnh báo
# "created with a default value but also had its value set via the Session State API": cố ý, không cần log.
logging.getLogger("streamlit.elements.lib.policies").setLevel(logging.ERROR)

st.set_page_config(
    page_title="Dashboard thị trường TMĐT",
    page_icon=":material/shopping_cart:",
    layout="wide",
    initial_sidebar_state="expanded",
)

from core import components as UI  # noqa: E402  (sau set_page_config)
from core.db import settings  # noqa: E402
from core.queries import clear_cache, date_bounds, mart_updated_at  # noqa: E402
from core.sql_catalog import catalog, sql_dir  # noqa: E402

UI.inject_css()

pages = {
    "Thị trường": [
        st.Page("views/1_overview.py", title="Tổng quan thị trường", icon=":material/monitoring:", default=True),
        st.Page("views/2_category_brand.py", title="Danh mục & thương hiệu", icon=":material/category:"),
        st.Page("views/3_seller.py", title="Người bán", icon=":material/storefront:"),
    ],
    "Giá & khách hàng": [
        st.Page("views/4_pricing_promotion.py", title="Giá & khuyến mãi", icon=":material/sell:"),
        st.Page("views/5_review.py", title="Đánh giá khách hàng", icon=":material/reviews:"),
    ],
    "Sản phẩm": [
        st.Page("views/6_product.py", title="Sản phẩm", icon=":material/inventory_2:"),
    ],
}
pg = st.navigation(pages, position="sidebar")

# ---- kiểm tra kết nối + catalog trước khi vẽ trang
try:
    cat = catalog()
except Exception as exc:
    st.error(f"Không đọc được file SQL dashboard trong `{sql_dir()}`: {exc}")
    st.stop()
try:
    d_min, d_max, n_days = date_bounds()
except Exception as exc:
    s = settings()
    st.error(
        f"Không kết nối được ClickHouse `{s.host}:{s.port}/{s.database}` (user `{s.user}`).\n\n"
        f"Kiểm tra container `clickhouse_server` đang chạy và mart đã được nạp "
        f"(src/python/ETL/e_mart/01..08).\n\nChi tiết: {exc}",
        icon=":material/cloud_off:",
    )
    st.stop()

with st.sidebar:
    st.markdown(
        "<div style='font-weight:700;font-size:1.05rem;margin-bottom:.1rem'>Thị trường TMĐT · Tiki.vn</div>"
        "<div class='note'>Pipeline crawl → Kafka → Spark → ClickHouse → data mart</div>",
        unsafe_allow_html=True,
    )
    st.divider()
    upd = mart_updated_at()
    st.caption(
        f"**Dữ liệu bán:** {d_min:%d/%m/%Y} – {d_max:%d/%m/%Y} · {n_days} ngày tin cậy  \n"
        + (f"**Mart cập nhật:** {upd:%d/%m/%Y %H:%M}  \n" if upd else "")
        + f"**Truy vấn:** {len(cat.queries)} truy vấn từ `query_dashboard/`"
    )
    st.button("Làm mới dữ liệu", icon=":material/refresh:", on_click=clear_cache, width="stretch",
              help="Kết quả truy vấn được cache 10 phút. Bấm để đọc lại data mart ngay (vd. sau khi chạy lại loader mart).")
    with st.expander("Đọc số liệu thế nào?", icon=":material/help:"):
        st.markdown(
            "- **Doanh thu, số bán là ước tính** từ chênh lệch \"đã bán\" lũy kế giữa các lần crawl.\n"
            "- Chỉ tính **ngày tin cậy** (`is_reliable_day`): bỏ ngày đầu/cuối kỳ crawl chưa đủ dữ liệu.\n"
            "- **Tăng trưởng** so sánh trung bình/ngày của kỳ đang xem với kỳ liền trước cùng độ dài.\n"
            "- Tỉ lệ luôn là `tổng tử / tổng mẫu`, không lấy trung bình của trung bình.\n"
            "- Thị phần, xếp hạng brand / seller bỏ nhóm \"Không rõ\".\n"
            "- Rê chuột lên biểu tượng ⓘ cạnh tiêu đề biểu đồ để xem ghi chú gốc trong file SQL."
        )

pg.run()
