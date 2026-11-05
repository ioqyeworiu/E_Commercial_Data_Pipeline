from pyspark.sql import Column, functions as F
from pyspark.sql.types import ArrayType, StringType

# Hệ số quy đổi -> giờ (thứ tự ưu tiên: kiểm tra từ trên xuống)
UNIT_HOURS = [
    ("năm",   365 * 24),     # 8760
    ("tháng", 365 * 24 / 12),  # 730
    ("tuần",  7 * 24),       # 168
    ("ngày",  24),
    ("giờ",   1),
    ("phút",  1 / 60),
]

def to_hours(col: Column) -> Column:
    """'đã tham gia 7 năm' -> 61320.0 ; không parse được -> NULL"""
    s = F.lower(F.trim(col))

    num_str = F.regexp_extract(s, r"(\d+)", 1)
    num = F.when(num_str != "", num_str.cast("double"))   # '' -> NULL

    unit = None
    for kw, hours in UNIT_HOURS:
        cond = s.contains(kw)
        unit = F.when(cond, F.lit(float(hours))) if unit is None \
               else unit.when(cond, F.lit(float(hours)))

    return num * unit


def parse_count(col: Column) -> Column:
    """'353 đánh giá' -> 353 ; '1.234 đánh giá' -> 1234 ; '' / NULL / không có số -> 0"""
    s = F.regexp_replace(F.trim(col), r"[.,]", "")
    digits = F.regexp_extract(s, r"(\d+)", 1)
    return F.coalesce(F.when(digits != "", digits.cast("int")), F.lit(0))

def parse_star_score(col: Column) -> Column:
    """'5' -> 5 ; '' / NULL / không có số -> 0 (chưa có đánh giá)"""
    digits = F.regexp_extract(F.trim(col), r"(\d+)", 1)
    return F.coalesce(F.when(digits != "", digits.cast("int")), F.lit(0))

def clean_comment(col: Column) -> Column:
    """Lowercase, bỏ dấu câu / ký tự đặc biệt / emoji, gộp khoảng trắng.
       'Êm chân! Bền!' -> 'êm chân bền'"""
    s = F.lower(col)
    s = F.regexp_replace(s, r"\\[nrt]", " ")       
    s = F.regexp_replace(s, r"[^\p{L}\p{M}\p{N}\s]", " ")  
    s = F.regexp_replace(s, r"\s+", " ")                   
    return F.trim(s)

def clean_prod_properties(col: Column) -> Column:
    """'["màu: đen","size: size 37 =23.5cm"]' -> 'màu: đen, size: 37 =23.5cm'
       '[]' / NULL / JSON lỗi -> NULL"""
    arr = F.from_json(col, ArrayType(StringType()))

    def fix(x):
        key = F.lower(F.trim(F.regexp_extract(x, r"^([^:]*):", 1)))
        val = F.lower(F.trim(F.regexp_extract(x, r"^[^:]*:\s*(.*)$", 1)))
        val = F.when(val.startswith(F.concat(key, F.lit(" "))),
                     F.trim(val.substr(F.length(key) + 2, F.length(val)))).otherwise(val)
        return F.when(x.contains(":"), F.concat_ws(": ", key, val)).otherwise(F.lower(F.trim(x)))

    pairs = F.array_distinct(F.transform(arr, fix))
    return F.when(F.size(pairs) > 0, F.concat_ws(", ", pairs))