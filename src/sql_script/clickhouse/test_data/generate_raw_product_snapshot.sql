-- =====================================================================================
-- Sinh dữ liệu TEST cho raw_product_snapshot
--   50.000 sản phẩm x 200 lần crawl = 10.000.000 dòng
--   + 50.000 dòng lặp nguyên bản (mô phỏng Kafka gửi lại -> bronze phải loại bằng idempotency_key)
--
-- url_id >= 1.000.000 để tách khỏi dữ liệu crawl thật (xóa bằng cleanup_test_data.sql).
-- Khoảng thời gian 2026-10-08 -> 2026-11-12, có 2 ngày sale 10.10 và 11.11.
-- Chuỗi giữ đúng định dạng crawler thật: '151050₫', '-10%', '(9.9k+ đánh giá)', 'Chưa có đánh giá',
-- observed_at dạng ISO '2026-10-08T03:12:45.123456+07:00'.
-- Mọi giá trị "ngẫu nhiên" đều là cityHash64(sản phẩm, ...) nên chạy lại cho ra đúng dữ liệu cũ.
--
-- Hành vi được cài sẵn để kiểm tra các tầng sau:
--   - mức giảm giá đổi theo từng đoạn ~4 ngày ; 60% sản phẩm sale >= 30% vào 10.10 và 11.11
--   - 10% sản phẩm "giảm giá ảo": 04/11 -> 11/11 nâng giá gốc x1,4 rồi ghi "-30%" (giá bán gần như không đổi)
--   - 20% sản phẩm tăng giá 8% sau ngày thứ 20
--   - sold lũy kế tăng dần, ngày sale bán gấp 4 ; product_rate_cnt = sold x hệ số
--   - 3% sản phẩm đổi title giữa kỳ (SCD2)
--   - brand viết nhiều kiểu (HOA / thường / ' Official' / ' Chính Hãng') ; 30% thiếu brand_origin
--   - dữ liệu lỗi: sell_price rỗng / 'Liên hệ', sold rỗng, category rỗng, made_in rỗng, seller rỗng
-- =====================================================================================
INSERT INTO raw_product_snapshot (
    url_id, title, brand, price, discount, sell_price, sold, product_rate, product_rate_cnt,
    category_tree, brand_origin, made_in, warranty, description, seller, seller_rate, seller_rate_cnt,
    additional_info, observed_at, ingested_at, idempotency_key
)
SELECT
    url_id_str, title_raw, brand_raw, price_raw, discount_raw, sell_price_raw, sold_raw,
    product_rate_raw, product_rate_cnt_raw, cat_raw, brand_origin_raw, made_in_raw, warranty_raw,
    description_raw, seller_raw, seller_rate_raw, seller_rate_cnt_raw, additional_info_raw,
    observed_at_raw, ingested_at_raw, idempotency_key_raw
FROM (
    WITH
        ['Lu','Mi','Ka','Zo','Ve','Ra','No','Ti','Sa','Ho','An','Bi','Co','Da','El',
         'Fi','Ga','Ha','Ki','Le','Ma','Ne','Ou','Pa','Qu','Ro','Su','Ta','Ul','Va'] AS syl_a,
        ['ra','mo','li','va','ne','to','si','ku','de','po'] AS syl_b,
        ['n','x','ra','ia','on','us','ix','a','o','e'] AS syl_c,
        ['Shop','Nhà Sách','Cửa Hàng','Kho Sỉ','Tiệm','Store','Thời Trang','Gia Dụng'] AS seller_prefix,
        ['Việt Nam','Việt Nam','Việt Nam','Trung Quốc','Trung Quốc','Hàn Quốc','Nhật Bản','Mỹ','Thái Lan','Pháp'] AS origins,
        ['Việt Nam','Việt Nam','Việt Nam','Trung Quốc','Trung Quốc','Thái Lan','Hàn Quốc','Nhật Bản','Indonesia','Bangladesh'] AS countries,
        ['', '', 'Không', 'Không bảo hành', '6 tháng', '12 tháng', 'Đổi trả trong 7 ngày', 'Bảo hành điện tử 12 tháng'] AS warranties,
        ['Cotton','Polyester','Linen','Lụa','Da PU','Denim','Kaki','Thun lạnh','Nhựa ABS','Thép không gỉ'] AS materials,
        ['basic','cao cấp','thời trang','giá rẻ','phong cách Hàn Quốc','oversize','mini','công sở'] AS styles,
        ['Đen','Trắng','Be','Xám','Xanh navy','Đỏ đô','Hồng pastel','Nâu','Xanh rêu'] AS colors,
        [0, 0, 0, 0, 5, 10, 10, 15, 20, 25, 30, 40] AS discount_levels,
        toDateTime('2026-10-08 00:00:00', 'Asia/Ho_Chi_Minh') AS t_start,
        15120 AS obs_interval,                                   -- 35 ngày / 200 lần crawl = 4,2 giờ
        toDateTime('2026-10-10 00:00:00', 'Asia/Ho_Chi_Minh') AS sale_1,
        toDateTime('2026-11-11 00:00:00', 'Asia/Ho_Chi_Minh') AS sale_2,
        -- 125 category_tree của dữ liệu crawl thật (chụp ngày 2026-11-14), nhúng cố định để bộ sinh
        -- không phụ thuộc vào raw: vẫn đúng khi raw đã TRUNCATE và cho kết quả giống hệt các lần trước
        -- (cùng thứ tự với arraySort(groupUniqArray(category_tree)) trên 251 dòng thật)
        [
            'Balo và Vali > Balo > Balo & túi chống sốc > Túi chống sốc',
            'Balo và Vali > Balo > Balo laptop',
            'Balo và Vali > Balo > Balo thời trang',
            'Giày - Dép nam > Dép nam > Dép nam quai ngang',
            'Giày - Dép nữ > Dép - Guốc nữ > Dép quai ngang',
            'Giày - Dép nữ > Giày cao gót > Giày bít mũi quai ngang',
            'Giày - Dép nữ > Giày cao gót > Giày cao gót hở mũi',
            'Giày - Dép nữ > Giày cao gót > Giày cao gót nhọn',
            'Giày - Dép nữ > Giày cao gót > Giày cao gót quai sau',
            'Giày - Dép nữ > Giày cao gót > Giày cao gót sục',
            'Giày - Dép nữ > Giày lười nữ > Giày lười hở gót',
            'Giày - Dép nữ > Giày sandals nữ > Giày sandals đế bằng',
            'Làm Đẹp - Sức Khỏe > Chăm sóc cá nhân > Dung dịch vệ sinh > Dung dịch vệ sinh phụ nữ',
            'Làm Đẹp - Sức Khỏe > Chăm sóc cơ thể > Tẩy tế bào chết cơ thể',
            'Làm Đẹp - Sức Khỏe > Chăm sóc da mặt > Mặt nạ > Mặt nạ giấy',
            'Làm Đẹp - Sức Khỏe > Dụng cụ làm đẹp > Dụng cụ tỉa lông mũi',
            'Làm Đẹp - Sức Khỏe > Dụng cụ làm đẹp > Massage thư giãn và làm đẹp > Làm thon gọn cơ thể',
            'Làm Đẹp - Sức Khỏe > Máy Massage & Thiết bị chăm sóc sức khỏe > Túi chườm > Chườm nóng',
            'NGON > Thịt, Trứng > Thịt bò, bê',
            'NGON > Thực phẩm đóng hộp và khô > Thực phẩm chế biến sẵn > Cháo ăn liền',
            'Nhà Cửa - Đời Sống > Dụng cụ nhà bếp > Nồi và chảo > Bộ nồi',
            'Nhà Cửa - Đời Sống > Ngoài trời & sân vườn > Áo mưa, ô dù và phụ kiện đi mưa > Áo mưa',
            'Nhà Cửa - Đời Sống > Nội thất > Nội thất phòng ngủ > Nội thất phòng ngủ khác',
            'Nhà Cửa - Đời Sống > Nội thất > Nội thất phòng ngủ > Tủ > Tủ quần áo',
            'Nhà Cửa - Đời Sống > Sửa chữa nhà cửa > Các loại máy và phụ kiện > Máy chà nhám, đánh bóng',
            'Nhà Cửa - Đời Sống > Sửa chữa nhà cửa > Các loại máy và phụ kiện > Máy vặn vít, tua vít',
            'Nhà Cửa - Đời Sống > Trang trí nhà cửa > Tranh trang trí > Tranh sơn dầu',
            'Nhà Cửa - Đời Sống > Đồ dùng phòng ăn > Bình/ Ly giữ nhiệt > Bình giữ nhiệt',
            'Nhà Cửa - Đời Sống > Đồ dùng và thiết bị nhà tắm > Phụ kiện giặt ủi > Phụ kiện giặt ủi khác',
            'Nhà Sách Tiki > English Books > Education - Teaching > ELTs',
            'Nhà Sách Tiki > Quà lưu niệm > Quà tặng trang trí khác',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách Chính Trị - Pháp Lý > Lý Luận Chính Trị > Kinh tế chính trị',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách Giáo Khoa - Giáo Trình > Giáo Trình Đại Học - Cao Đẳng > Kinh tế - Tài chính - Quản trị',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách Giáo Khoa - Giáo Trình > Sách Giáo Khoa Cấp 1',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách Kiến Thức Tổng Hợp > Lĩnh vực khác',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách Kiến Thức Tổng Hợp > Sách giáo dục',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách Lịch sử > Lịch Sử Việt Nam',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách Tham Khảo > Sách tham khảo cấp I',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách Tham Khảo > Sách tham khảo cấp II',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách Tham Khảo > Sách tham khảo cấp III',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách Tâm lý - Giới tính > Sách Hôn Nhân - Giới Tính',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách kỹ năng sống > Sách tư duy - Kỹ năng sống',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách thiếu nhi > Truyện tranh thiếu nhi',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách văn học > Phê Bình - Lý Luận Văn Học',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách văn học > Tiểu Thuyết > Tiểu Thuyết Phương Tây',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách văn học > Tiểu sử - Hồi ký',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách văn học > Truyện Giả tưởng - Huyền Bí - Phiêu Lưu',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách văn học > Truyện dài',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách văn học > Truyện kinh dị',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách văn học > Truyện trinh thám',
            'Nhà Sách Tiki > Sách tiếng Việt > Sách văn học > Truyện đam mỹ > Truyện Chữ Đam Mỹ',
            'Nhà Sách Tiki > Sách tiếng Việt > Truyện Tranh, Manga, Comic',
            'Phụ kiện thời trang > Mắt kính > Gọng kính > Gọng kính nữ',
            'Phụ kiện thời trang > Mắt kính > Kính mát > Kính mát nam',
            'Phụ kiện thời trang > Phụ kiện thời trang nam > Nón nam > Nón lưỡi trai nam',
            'Phụ kiện thời trang > Phụ kiện thời trang nam > Thắt lưng, dây nịt nam > Dây thắt lưng - Dây nịt',
            'Phụ kiện thời trang > Phụ kiện thời trang nữ > Khăn - Tất - Vớ nữ > Vớ dài nữ',
            'Phụ kiện thời trang > Phụ kiện thời trang nữ > Nón nữ > Nón chống nắng nữ',
            'Phụ kiện thời trang > Phụ kiện thời trang nữ > Phụ kiện tóc nữ > Cài tóc',
            'Thiết Bị Số - Phụ Kiện Số > Phụ Kiện Điện Thoại và Máy Tính Bảng > Bao Da - Ốp Lưng > Bao Da - Ốp Lưng Điện Thoại Samsung',
            'Thể Thao - Dã Ngoại > Giày thể thao nữ > Giày thể thao nữ khác',
            'Thể Thao - Dã Ngoại > Gym và Boxing > Trang phục tập Gym và Boxing > Trang phục Gym và Boxing Nam > Áo thun có tay tập Gym và Boxing Nam',
            'Thể Thao - Dã Ngoại > Gym và Boxing > Trang phục tập Gym và Boxing > Trang phục Gym và Boxing Nữ > Áo tập Gym và Boxing Nữ',
            'Thể Thao - Dã Ngoại > Trang phục thể thao > Trang phục thể thao nam > Trang phục Golf nam > Quần Golf nam',
            'Thể Thao - Dã Ngoại > Trang phục thể thao > Trang phục thể thao nữ > Trekking & leo núi > Áo khoác dã ngoại nữ',
            'Thể Thao - Dã Ngoại > Đồ dùng dã ngoại & Leo núi > Đồ dùng dã ngoại > Đồ dùng dã ngoại khác',
            'Thời Trang Cho Mẹ Và Bé > Phụ kiện cho bé > Dù, áo mưa cho bé',
            'Thời Trang Cho Mẹ Và Bé > Thời trang bé gái > Trang phục bé gái > Đầm bé gái',
            'Thời Trang Cho Mẹ Và Bé > Thời trang bé gái > Trang phục bé gái > Đồ lót & Đồ ngủ bé gái',
            'Thời Trang Cho Mẹ Và Bé > Thời trang bé gái > Đồ bộ bé gái',
            'Thời trang nam > Quần dài nam > Quần jeans nam',
            'Thời trang nam > Quần dài nam > Quần jogger nam',
            'Thời trang nam > Quần dài nam > Quần tây nam',
            'Thời trang nam > Quần short nam > Quần short kaki nam',
            'Thời trang nam > Quần short nam > Quần short thun nam',
            'Thời trang nam > Áo hoodie nam > Áo hoodies nam vải nỉ',
            'Thời trang nam > Áo nỉ - Áo len nam > Áo len dệt kim',
            'Thời trang nam > Áo sơ mi nam > Áo sơ mi nam tay dài > Áo sơ mi nam tay dài họa tiết',
            'Thời trang nam > Áo sơ mi nam > Áo sơ mi nam tay dài > Áo sơ mi nam tay dài trơn',
            'Thời trang nam > Áo sơ mi nam > Áo sơ mi nam tay ngắn > Áo sơ mi nam tay ngắn họa tiết',
            'Thời trang nam > Áo sơ mi nam > Áo sơ mi nam tay ngắn > Áo sơ mi nam tay ngắn trơn',
            'Thời trang nam > Áo thun nam',
            'Thời trang nam > Áo thun nam > Áo thun nam ba lỗ',
            'Thời trang nam > Áo thun nam > Áo thun nam dài tay có cổ',
            'Thời trang nam > Áo thun nam > Áo thun nam ngắn tay có cổ',
            'Thời trang nam > Áo thun nam > Áo thun nam ngắn tay không cổ',
            'Thời trang nam > Áo vest - Áo khoác nam',
            'Thời trang nam > Áo vest - Áo khoác nam > Áo khoác bomber nam',
            'Thời trang nam > Áo vest - Áo khoác nam > Áo khoác da nam',
            'Thời trang nam > Áo vest - Áo khoác nam > Áo khoác gió',
            'Thời trang nam > Áo vest - Áo khoác nam > Áo khoác jeans nam',
            'Thời trang nam > Áo vest - Áo khoác nam > Áo khoác phao nam',
            'Thời trang nam > Áo vest - Áo khoác nam > Áo vest và Blazer nam',
            'Thời trang nam > Đồ bơi - Đồ đi biển nam > Quần bơi - đi biển nam',
            'Thời trang nam > Đồ lót nam > Quần lót nam > Quần lót boxer nam',
            'Thời trang nam > Đồ lót nam > Quần lót nam > Quần lót kiểu brief nam',
            'Thời trang nam > Đồ lót nam > Quần lót nam > Quần lót nam khác',
            'Thời trang nam > Đồ ngủ, đồ mặc nhà nam > Đồ mặc nhà nam - Bộ dài',
            'Thời trang nữ > Chân váy > Chân váy ngắn',
            'Thời trang nữ > Quần nữ > Quần baggy nữ',
            'Thời trang nữ > Quần nữ > Quần shorts nữ',
            'Thời trang nữ > Trang phục bơi nữ > Đồ bơi hai mảnh',
            'Thời trang nữ > Trang phục bơi nữ > Đồ bơi một mảnh',
            'Thời trang nữ > Áo liền quần - Bộ trang phục > Áo dài > Áo dài cách tân',
            'Thời trang nữ > Áo liền quần - Bộ trang phục > Áo liền quần',
            'Thời trang nữ > Áo sơ mi nữ > Áo sơ mi nữ tay dài > Áo sơ mi nữ tay dài hoạ tiết',
            'Thời trang nữ > Áo thun nữ > Áo hai dây - Áo ba lỗ nữ',
            'Thời trang nữ > Áo thun nữ > Áo thun nữ ngắn tay không cổ',
            'Thời trang nữ > Áo vest - Áo khoác nữ > Áo khoác chống nắng nữ',
            'Thời trang nữ > Áo vest - Áo khoác nữ > Áo len - Áo khoác nỉ nữ',
            'Thời trang nữ > Áo vest - Áo khoác nữ > Áo vest, blazer nữ > Áo vest, blazer nữ rời',
            'Thời trang nữ > Đồ lót nữ > Quần lót nữ > Quần lọt khe',
            'Thời trang nữ > Đồ lót nữ > Quần lót nữ > Quần ôm',
            'Thời trang nữ > Đồ lót nữ > Áo ngực > Áo không đệm - Bralette',
            'Thời trang nữ > Đồ lót nữ > Áo ngực > Áo nâng ngực',
            'Thời trang nữ > Đồ ngủ - Đồ mặc nhà nữ > Đồ mặc nhà > Đồ bộ dài',
            'Thời trang nữ > Đồ ngủ - Đồ mặc nhà nữ > Đồ mặc nhà > Đồ bộ ngắn',
            'Thời trang nữ > Đồ ngủ - Đồ mặc nhà nữ > Đồ ngủ nữ > Đầm ngủ',
            'Thời trang nữ > Đồ ngủ - Đồ mặc nhà nữ > Đồ ngủ nữ > Đồ ngủ cosplay, gợi cảm',
            'Thời trang nữ > Đồ đôi - Đồ gia đình > Áo đôi > Áo thun đôi',
            'Túi thời trang nam > Túi đeo chéo nam',
            'Túi thời trang nữ > Túi đeo chéo, túi đeo vai nữ > Túi đeo chéo dạo phố',
            'Điện Gia Dụng > Đồ dùng nhà bếp > Bếp các loại > Bếp gas > Bếp gas du lịch - bếp gas đơn',
            'Đồng hồ và Trang sức > Trang sức > Bông tai',
            'Đồng hồ và Trang sức > Đồng hồ nữ > Đồng hồ dây kim loại'
        ] AS cats
    SELECT
        n, p, k,
        toString(1000000 + p) AS url_id_str,

        -- ---------------- thuộc tính tĩnh của sản phẩm ----------------
        cats[1 + cityHash64(p, 'cat') % length(cats)] AS cat_full,
        if(cityHash64(p, 'cat_miss') % 1000 < 3, '', cat_full) AS cat_raw,
        splitByString(' > ', cat_full)[-1] AS leaf,

        cityHash64(p, 'brand_kind') % 100 AS brand_kind,        -- < 8: không brand ; < 23: OEM ; còn lại: brand có tên
        toUInt32(floor(3000 * pow((cityHash64(p, 'brand_idx') % 1000000) / 1e6, 1.7))) AS bi,
        concat(syl_a[1 + bi % 30], syl_b[1 + intDiv(bi, 30) % 10], syl_c[1 + intDiv(bi, 300) % 10]) AS brand_name,
        cityHash64(p, 'brand_variant') % 100 AS bv,
        multiIf(
            brand_kind < 8, '',
            brand_kind < 23, 'OEM',
            bv < 85, brand_name,
            bv < 90, upper(brand_name),
            bv < 94, concat(brand_name, ' Official'),
            bv < 97, lower(brand_name),
            concat(brand_name, ' Chính Hãng')
        ) AS brand_raw,
        multiIf(
            brand_kind < 8, '',
            brand_kind < 23, countries[1 + cityHash64(p, 'oem_origin') % 10],
            cityHash64(p, 'origin_miss') % 100 < 30, '',
            origins[1 + cityHash64(bi, 'origin') % 10]
        ) AS brand_origin_raw,
        if(cityHash64(p, 'made_miss') % 100 < 5, '', countries[1 + cityHash64(p, 'made_in') % 10]) AS made_in_raw,
        warranties[1 + cityHash64(p, 'warranty') % 8] AS warranty_raw,
        materials[1 + cityHash64(p, 'material') % 10] AS material,
        styles[1 + cityHash64(p, 'style') % 8] AS style,
        arrayStringConcat(arraySlice(colors, 1 + cityHash64(p, 'color') % 7, 1 + cityHash64(p, 'ncolor') % 3), ', ') AS color_list,
        concat(leaf, ' ', style, ' ', lowerUTF8(material),
               if(brand_kind >= 23, concat(' ', brand_name), ''),
               ' - mã ', toString(1000 + cityHash64(p, 'code') % 9000)) AS title_base,
        cityHash64(p, 'title_change') % 100 < 3 AS has_title_change,
        80 + cityHash64(p, 'title_change_at') % 100 AS title_change_k,
        if(has_title_change AND k >= title_change_k, concat(title_base, ' (mẫu mới 2026)'), title_base) AS title_raw,

        cityHash64(p, 'seller_kind') % 100 AS sk,
        toUInt32(floor(5000 * pow((cityHash64(p, 'seller_idx') % 1000000) / 1e6, 1.5))) AS si,
        multiIf(
            sk < 2, '',
            sk < 5, 'Tiki Trading',
            brand_kind >= 23 AND sk < 30, concat(brand_name, ' Official Store'),
            concat(seller_prefix[1 + si % 8], ' ', syl_a[1 + intDiv(si, 8) % 30],
                   syl_b[1 + intDiv(si, 240) % 10], syl_c[1 + intDiv(si, 2400) % 10])
        ) AS seller_raw,
        -- rating của seller suy ra từ tên seller -> cùng seller có cùng rating ở mọi sản phẩm
        (cityHash64(seller_raw, 'seller_u') % 1000000) / 1e6 AS su,
        multiIf(seller_raw = 'Tiki Trading', 5600000, su > 0.95, 0,
                toUInt64(least(20 / greatest(su, 0.00001), 3000000))) AS seller_cnt_base,
        40 + cityHash64(seller_raw, 'seller_rate') % 11 AS seller_rate10,      -- 4.0 .. 5.0

        toUInt64(round(exp(log(49000) + pow((cityHash64(p, 'price') % 1000000) / 1e6, 1.6) * log(300)) / 1000) * 1000) AS base_price,
        cityHash64(p, 'price_up') % 100 < 20 AS has_price_up,
        cityHash64(p, 'fake') % 100 < 10 AS is_fake_discounter,
        (cityHash64(p, 'sales_u') % 1000000) / 1e6 AS sales_u,
        if(sales_u < 0.3, 0, pow(sales_u, 3) * 40) AS sales_per_day,
        toUInt64(floor(pow((cityHash64(p, 'sold0') % 1000000) / 1e6, 4) * 5000)) AS sold_base,
        0.05 + (cityHash64(p, 'rate_ratio') % 1000) / 1000 * 0.25 AS rate_ratio,
        35 + cityHash64(p, 'rating') % 16 AS rating10,                          -- 3.5 .. 5.0

        -- ---------------- giá trị thay đổi theo lần crawl k ----------------
        t_start + toIntervalSecond(
            toInt64(cityHash64(p, 'offset') % obs_interval) + toInt64(k) * obs_interval
            + toInt64(cityHash64(p, k, 'jitter') % 1200) - 600) AS t,
        greatest((toInt64(t) - toInt64(t_start)) / 86400, 0) AS days,
        toDate(t) AS obs_date,
        intDiv(k, 24) AS seg,
        obs_date IN (toDate('2026-10-10'), toDate('2026-11-11')) AS is_sale_day,
        is_fake_discounter AND obs_date BETWEEN toDate('2026-11-04') AND toDate('2026-11-11') AS in_fake_window,
        if(has_price_up AND days >= 20, toUInt64(round(base_price * 1.08 / 1000) * 1000), base_price) AS cur_base,
        multiIf(
            in_fake_window, 30,
            is_sale_day AND cityHash64(p, 'join_sale') % 100 < 60,
                greatest(discount_levels[1 + cityHash64(p, seg, 'disc') % 12], 30),
            discount_levels[1 + cityHash64(p, seg, 'disc') % 12]
        ) AS disc,
        if(in_fake_window, toUInt64(round(cur_base * 1.4 / 1000) * 1000), cur_base) AS list_price,
        if(disc = 0, list_price, toUInt64(round(list_price * (100 - disc) / 100 / 1000) * 1000)) AS sell_price_num,
        least(greatest((toInt64(t) - toInt64(sale_1)) / 86400, 0), 1)
          + least(greatest((toInt64(t) - toInt64(sale_2)) / 86400, 0), 1) AS sale_days_elapsed,
        sold_base + toUInt64(floor(sales_per_day * (days + 3 * sale_days_elapsed))) AS sold_num,
        toUInt64(floor(sold_num * rate_ratio)) AS rate_cnt_num,
        seller_cnt_base + toUInt64(floor(seller_cnt_base * 0.01 * days)) AS seller_cnt_num,

        -- ---------------- định dạng chuỗi như crawler ----------------
        cityHash64(n, 'row_err') % 10000 AS row_err,
        if(disc > 0, concat(toString(list_price), '₫'), '') AS price_raw,
        if(disc > 0, concat('-', toString(disc), '%'), '') AS discount_raw,
        multiIf(row_err < 10, '', row_err < 15, 'Liên hệ', toString(sell_price_num)) AS sell_price_raw,
        if(row_err >= 15 AND row_err < 65, '', toString(sold_num)) AS sold_raw,
        if(rate_cnt_num = 0, '', concat(toString(intDiv(rating10, 10)), '.', toString(rating10 % 10))) AS product_rate_raw,
        if(rate_cnt_num = 0, '', toString(rate_cnt_num)) AS product_rate_cnt_raw,
        if(seller_raw = '' OR seller_cnt_num = 0, 'Chưa có đánh giá',
           concat(toString(intDiv(seller_rate10, 10)), '.', toString(seller_rate10 % 10))) AS seller_rate_raw,
        multiIf(
            seller_raw = '' OR seller_cnt_num = 0, 'Chưa có đánh giá',
            seller_cnt_num < 1000, concat('(', toString(seller_cnt_num), ' đánh giá)'),
            seller_cnt_num < 1000000, concat('(', toString(floor(seller_cnt_num / 100) / 10), 'k+ đánh giá)'),
            concat('(', toString(floor(seller_cnt_num / 100000) / 10), 'tr+ đánh giá)')
        ) AS seller_rate_cnt_raw,
        concat(title_raw, '. Chất liệu ', material, ', kiểu dáng ', style, ', dễ phối đồ. Màu sắc: ', color_list,
               '. Hướng dẫn bảo quản: giặt nhẹ ở 30°C, không dùng chất tẩy, phơi nơi thoáng mát.') AS description_raw,
        concat('["key_Thương hiệu: value_', if(brand_raw = '', 'None', brand_raw),
               '","key_Xuất xứ thương hiệu: value_', if(brand_origin_raw = '', 'None', brand_origin_raw),
               '","key_Sản xuất tại: value_', if(made_in_raw = '', 'None', made_in_raw),
               '","key_Chất liệu: value_', material,
               '","key_Màu sắc: value_', color_list, '"]') AS additional_info_raw,
        concat(formatDateTime(t, '%Y-%m-%dT%H:%i:%S'), '.',
               leftPad(toString(cityHash64(p, k, 'micro') % 1000000), 6, '0'), '+07:00') AS observed_at_raw,
        t + toIntervalSecond(5 + cityHash64(p, k, 'lag') % 115) AS ingested_at_raw,
        -- giống load_raw_product_snapshot.py: sha2(concat_ws('\u0001', url_id, observed_at), 256)
        lower(hex(SHA256(concat(url_id_str, '\x01', observed_at_raw)))) AS idempotency_key_raw
    FROM (
        SELECT number AS n, intDiv(number, 200) AS p, number % 200 AS k
        FROM numbers(10000000)
        UNION ALL
        SELECT d AS n, intDiv(d, 200) AS p, d % 200 AS k
        FROM (SELECT cityHash64(number, 'dup') % 10000000 AS d FROM numbers(50000))
    )
);
