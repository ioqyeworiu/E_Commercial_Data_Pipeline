"""Kiểm tra nhanh dashboard.

    python smoke_test.py            # cả hai phần
    python smoke_test.py queries    # chạy 43 truy vấn (toàn sàn + 1 danh mục), in số dòng / thời gian / lỗi
    python smoke_test.py pages      # render 6 trang bằng streamlit.testing (AppTest), báo exception / st.error

Trong container:  docker exec streamlit_dashboard python /app/smoke_test.py
Thoát với mã 1 nếu có lỗi.
"""
from __future__ import annotations

import datetime as dt
import logging
import sys
import time
import warnings

warnings.filterwarnings("ignore")
logging.getLogger("streamlit").setLevel(logging.ERROR)

SLOW_SECONDS = 2.5
PAGES = [
    "views/1_overview.py",
    "views/2_category_brand.py",
    "views/3_seller.py",
    "views/4_pricing_promotion.py",
    "views/5_review.py",
    "views/6_product.py",
]


def _quiet_streamlit() -> None:
    """Chạy ngoài `streamlit run` nên cache báo "No runtime found": bình thường, tắt log."""
    for name in list(logging.root.manager.loggerDict):
        if name.startswith("streamlit"):
            logging.getLogger(name).setLevel(logging.ERROR)


def check_queries() -> int:
    from core import queries as Q
    from core.sql_catalog import catalog
    _quiet_streamlit()

    cat = catalog()
    d_min, d_max, _ = Q.date_bounds()
    top = Q.search_products("", "", 1)
    cats = Q.category_options()
    base = {
        "date_from": max(d_min, d_max - dt.timedelta(days=15)),
        "date_to": d_max,
        "sale_event": (Q.sale_events() or [""])[0],
        "product_id": int(top.product_id.iloc[0]),
    }
    scenarios = [("toàn sàn", ""), (f"danh mục '{cats[0]}'", cats[0])]
    print(f"Catalog: {len(cat.queries)} truy vấn · tham số: {base}")
    errors, slow = 0, 0
    for label, cat_l1 in scenarios:
        print(f"\n== {label}")
        flt = dict(base, category_l1=cat_l1)
        for qid in sorted(cat.queries, key=lambda s: tuple(int(p) for p in s.split("."))):
            q = cat[qid]
            if cat_l1 and "category_l1" not in q.params:
                continue
            t0 = time.perf_counter()
            try:
                df = Q._query.__wrapped__(q.sql, tuple(sorted(
                    (k, Q._coerce(flt[k], typ)) for k, typ in q.params.items())))
                el = time.perf_counter() - t0
                mark = " SLOW" if el > SLOW_SECONDS else ""
                slow += bool(mark)
                print(f"  [{qid:>4}] {len(df):>6} dòng  {el:5.2f}s{mark}  {q.title[:60]}")
            except Exception as exc:
                errors += 1
                print(f"  [{qid:>4}] LỖI: {exc}")
    print(f"\nTruy vấn: {errors} lỗi, {slow} chậm hơn {SLOW_SECONDS}s")
    return errors


def check_pages() -> int:
    from streamlit.testing.v1 import AppTest

    errors = 0
    at = AppTest.from_file("app.py", default_timeout=120)
    for page in PAGES:
        t0 = time.perf_counter()
        at.switch_page(page)
        at.run()
        el = time.perf_counter() - t0
        problems = [str(e.value)[:300] for e in at.exception] + [str(e.value)[:300] for e in at.error]
        n_charts = len(at.get("plotly_chart"))
        n_tables = len(at.dataframe)
        status = "OK " if not problems else "LỖI"
        print(f"{status} {page:<32} {el:5.1f}s  {n_charts:>2} biểu đồ, {n_tables:>2} bảng")
        for p in problems:
            print(f"      - {p}")
        errors += len(problems)
    print(f"\nTrang: {errors} lỗi")
    return errors


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    n = 0
    if what in ("all", "queries"):
        n += check_queries()
    if what in ("all", "pages"):
        n += check_pages()
    sys.exit(1 if n else 0)
