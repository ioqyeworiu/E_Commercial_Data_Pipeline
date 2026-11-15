"""Đọc 6 file SQL dashboard (src/sql_script/clickhouse/query_dashboard/*.sql) thành catalog truy vấn.

File SQL là nguồn sự thật duy nhất: app không chép SQL vào Python. Mỗi truy vấn bắt đầu bằng dòng
    -- [x.y] Tên biểu đồ | Biểu đồ: gợi ý loại biểu đồ
theo sau là các dòng comment giải thích, rồi tới câu SQL (kết thúc bằng ';').
Tham số dùng cú pháp query parameter của ClickHouse {tên:Kiểu} và được bind phía server.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import streamlit as st

EXPECTED_QUERIES = 43

# Header phải có "| Biểu đồ:" để không nhầm với comment kiểu "-- [6.1] - [6.4] đọc mart..." ở đầu file
HEADER_RE = re.compile(r"^--\s*\[(\d+)\.(\d+)\]\s*(.*\|\s*Biểu đồ\s*:.*)$")
PARAM_RE = re.compile(r"\{(\w+):(\w+)\}")
DASHBOARD_TITLE_RE = re.compile(r"^--\s*DASHBOARD\s+(\d+)\s*:\s*(.+)$", re.IGNORECASE)


@dataclass(frozen=True)
class Query:
    qid: str                 # "1.2"
    dashboard: int           # 1
    title: str               # "Xu hướng doanh thu theo ngày"
    chart_hint: str          # gợi ý biểu đồ ghi trong SQL
    notes: str               # các dòng comment giải thích
    sql: str                 # câu SQL, đã bỏ ';' cuối
    params: dict[str, str]   # {tên: Kiểu ClickHouse}
    source: str              # "01_overview.sql"


@dataclass(frozen=True)
class Dashboard:
    number: int
    title: str
    intro: str
    source: str
    query_ids: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class Catalog:
    queries: dict[str, Query]
    dashboards: dict[int, Dashboard]

    def __getitem__(self, qid: str) -> Query:
        return self.queries[qid]


def sql_dir() -> Path:
    env = os.getenv("DASHBOARD_SQL_DIR")
    if env:
        return Path(env)
    # Chạy trên host: src/python/dashboard/core -> src/sql_script/clickhouse/query_dashboard
    return Path(__file__).resolve().parents[3] / "sql_script" / "clickhouse" / "query_dashboard"


def _strip_comment(line: str) -> str:
    return re.sub(r"^--\s?", "", line).rstrip()


def _balanced(text: str) -> bool:
    return text.count("(") <= text.count(")")


def _parse_file(path: Path) -> tuple[Dashboard, list[Query]]:
    lines = path.read_text(encoding="utf-8").splitlines()

    # ---- phần đầu file: "-- DASHBOARD n: TÊN" + đoạn mô tả tới dòng comment trống đầu tiên
    number, title, intro_lines = 0, path.stem, []
    first_header = next((i for i, l in enumerate(lines) if HEADER_RE.match(l)), len(lines))
    in_intro = False
    for line in lines[:first_header]:
        m = DASHBOARD_TITLE_RE.match(line)
        if m:
            number, title = int(m.group(1)), m.group(2).strip()
            in_intro = True
            continue
        if in_intro:
            text = _strip_comment(line)
            if not line.startswith("--") or not text.strip() or set(text.strip()) <= {"="}:
                break
            intro_lines.append(text.strip())

    # ---- các truy vấn
    queries: list[Query] = []
    i = first_header
    while i < len(lines):
        m = HEADER_RE.match(lines[i])
        if not m:
            i += 1
            continue
        dash_no, sub_no, rest = int(m.group(1)), int(m.group(2)), m.group(3)
        qid = f"{dash_no}.{sub_no}"
        name, _, hint = rest.partition("|")
        hint = re.sub(r"^\s*Biểu đồ\s*:\s*", "", hint).strip()

        # comment ngay sau header: phần nối của gợi ý biểu đồ (ngoặc chưa đóng / dòng kết thúc bằng ',')
        # rồi tới ghi chú
        j = i + 1
        notes: list[str] = []
        while j < len(lines) and lines[j].startswith("--") and not HEADER_RE.match(lines[j]):
            text = _strip_comment(lines[j]).strip()
            if not notes and (not _balanced(hint) or hint.endswith(",") or text.startswith("(")):
                hint = f"{hint} {text}".strip()
            else:
                notes.append(text)
            j += 1

        # SQL: tới header kế tiếp
        k = j
        while k < len(lines) and not HEADER_RE.match(lines[k]):
            k += 1
        sql = "\n".join(lines[j:k]).strip()
        sql = re.sub(r";\s*$", "", sql)
        params = dict(PARAM_RE.findall(sql))
        queries.append(Query(
            qid=qid, dashboard=dash_no, title=name.strip(), chart_hint=hint, notes="\n".join(notes).strip(),
            sql=sql, params=params, source=path.name,
        ))
        i = k

    dash = Dashboard(
        number=number or (queries[0].dashboard if queries else 0),
        title=title,
        intro=" ".join(intro_lines),
        source=path.name,
        query_ids=tuple(q.qid for q in queries),
    )
    return dash, queries


@st.cache_resource(show_spinner=False)
def _load(signature: tuple[tuple[str, float], ...]) -> Catalog:
    queries: dict[str, Query] = {}
    dashboards: dict[int, Dashboard] = {}
    for name, _mtime in signature:
        dash, qs = _parse_file(sql_dir() / name)
        dashboards[dash.number] = dash
        for q in qs:
            if q.qid in queries:
                raise ValueError(f"Trùng ID truy vấn [{q.qid}] trong {name} và {queries[q.qid].source}")
            queries[q.qid] = q
    if len(queries) != EXPECTED_QUERIES:
        raise ValueError(f"Đọc được {len(queries)} truy vấn từ {sql_dir()}, cần {EXPECTED_QUERIES}")
    return Catalog(queries=queries, dashboards=dashboards)


def catalog() -> Catalog:
    """Catalog đã parse. Khóa cache = (tên file, mtime) nên sửa file SQL là có hiệu lực ở lần rerun kế tiếp."""
    files = sorted(sql_dir().glob("[0-9][0-9]_*.sql"))
    if not files:
        raise FileNotFoundError(f"Không tìm thấy file SQL dashboard trong {sql_dir()}")
    return _load(tuple((f.name, f.stat().st_mtime) for f in files))
