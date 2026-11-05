from pyspark.sql import functions as F, Column


def normalize_price(c: Column, dtype: str = "decimal(18,0)") -> Column:
    """
    Chuẩn hóa giá dạng '173.700₫' thành số 173700.

    - Xóa mọi ký tự không phải chữ số (₫, đ, VNĐ, khoảng trắng, dấu chấm ngăn nghìn...)
    - Chuỗi rỗng hoặc không có chữ số sẽ thành NULL
    - Dùng try_cast nên giá trị lỗi cho NULL thay vì văng exception khi bật ANSI
    """
    digits = F.regexp_replace(c.cast("string"), r"[^0-9]", "")
    digits = F.when(F.length(digits) > 0, digits)          # rỗng -> NULL
    return digits.try_cast(dtype) if hasattr(Column, "try_cast") else digits.cast(dtype)




def normalize_discount(c, as_fraction=False, keep_sign=False, null_as_zero=True):
    """
    Chuẩn hóa giảm giá dạng '-20%', '20 %', '15,5%' thành số thực (double).

    - NULL, chuỗi rỗng, chuỗi chỉ có khoảng trắng -> 0.0 (nếu null_as_zero=True)
    - Dạng lạ (chữ, nhiều số, ...) -> cũng thành 0.0 khi null_as_zero=True
    - as_fraction=True: trả về 0.20 thay vì 20.0
    - keep_sign=False: bỏ dấu âm, '-20%' -> 20.0
    """
    s = F.regexp_replace(c.cast("string"), r"\s+", "")
    num = F.regexp_extract(s, r"^([-+]?\d+(?:[.,]\d+)?)%?$", 1)
    num = F.regexp_replace(num, ",", ".")

    val = F.when(F.length(num) > 0, num.cast("double"))
    if not keep_sign:
        val = F.abs(val)
    if as_fraction:
        val = val / 100
    if null_as_zero:
        val = F.coalesce(val, F.lit(0.0))
    return val


def normalize_sold(c, dtype="int"):
    """
    Chuẩn hóa số lượng bán ra: '1.234', 'Đã bán 56', '' , NULL -> số nguyên.
    - NULL, rỗng, không có chữ số -> 0
    """
    digits = F.regexp_replace(c.cast("string"), r"[^0-9]", "")
    val = F.when(F.length(digits) > 0, digits.cast(dtype))
    return F.coalesce(val, F.lit(0).cast(dtype))


def normalize_title(c, lower=False, keep_chars=r"\-&+%./"):
    """
    Chuẩn hóa tên sản phẩm:
    - Chuẩn hóa Unicode về dạng dựng sẵn (NFC) để không mất dấu tiếng Việt
    - Bỏ ký tự đặc biệt/emoji, giữ chữ, số, khoảng trắng và các ký tự trong keep_chars
    - Gộp khoảng trắng thừa, trim
    - NULL, rỗng -> NULL
    - lower=True: chuyển thành chữ thường
    """
    s = c.cast("string")
    s = F.regexp_replace(s, r"[\u200b-\u200d\ufeff\u00a0]", " ")
    s = F.regexp_replace(s, rf"[^\p{{L}}\p{{N}}\s{keep_chars}]", " ")
    s = F.regexp_replace(s, r"\s+", " ")
    s = F.trim(s)
    if lower:
        s = F.lower(s)
    return F.when(F.length(s) > 0, s)


def normalize_warranty(c, default="không", lower=True):
    """
    Chuẩn hóa thông tin bảo hành.
    - NULL, rỗng, chỉ có khoảng trắng -> default ('không')
    - Các giá trị mang nghĩa "không có" ('-', '0', 'N/A', 'none', 'Không bảo hành'...) -> default
    - Còn lại: bỏ khoảng trắng thừa, ký tự vô hình; lower=True thì đổi chữ thường
    """
    NO_WARRANTY_PATTERN = r"(?iu)^(?:-+|0|n/?a|null|none|no|không|không có|không bảo hành|ko|k)$"
    s = c.cast("string")
    s = F.regexp_replace(s, r"[\u200b-\u200d\ufeff\u00a0]", " ")
    s = F.trim(F.regexp_replace(s, r"\s+", " "))
    if lower:
        s = F.lower(s)
    is_empty = s.isNull() | (s == "") | s.rlike(NO_WARRANTY_PATTERN)
    return F.when(is_empty, F.lit(default)).otherwise(s)

def normalize_rating(c, max_value=5.0):
    """
    Chuẩn hóa điểm đánh giá: '4.8', '4,8', '4.8/5', '4.8 sao' -> 4.8 (double)
    - NULL, rỗng, 'Chưa có đánh giá', dạng lạ -> 0.0
    - Giá trị ngoài khoảng [0, max_value] -> 0.0
    """
    s = F.regexp_replace(c.cast("string"), r"\s+", "")
    num = F.regexp_extract(s, r"^(\d+(?:[.,]\d+)?)", 1)      
    num = F.regexp_replace(num, ",", ".")
    val = F.when(F.length(num) > 0, num.cast("double"))
    val = F.when((val >= 0) & (val <= max_value), val)      
    return F.coalesce(val, F.lit(0.0))


def normalize_rating_count(c, dtype="int"):
    """
    '1.234', '(1,2k)', '5k+', '56 đánh giá' -> số nguyên.
    NULL, rỗng, 'Chưa có đánh giá' -> 0
    """
    s = F.lower(F.regexp_replace(c.cast("string"), r"\s+", ""))

    k_num = F.regexp_extract(s, r"(\d+(?:[.,]\d+)?)k", 1)
    from_k = F.when(F.length(k_num) > 0,
                    F.round(F.regexp_replace(k_num, ",", ".").cast("double") * 1000))

    digits = F.regexp_replace(s, r"[^0-9]", "")
    plain = F.when(F.length(digits) > 0, digits.cast("double"))

    return F.coalesce(from_k, plain, F.lit(0.0)).cast(dtype)