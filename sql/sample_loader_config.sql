-- ================================================================
-- sample_loader_config.sql
-- Cấu hình Auto Loader trong bảng sqlite_metadata.data_loader
--
-- ĐỌC KỸ TRƯỚC KHI CHẠY:
-- 1. Thay các giá trị placeholder (VD_...) bằng giá trị thực tế
-- 2. Hỏi DBA hoặc team DevOps để lấy đúng connection string
-- 3. Chỉ cấu hình loader MỘT LẦN cho cả team, không chạy lại
-- ================================================================

-- ---------------------------------------------------------------
-- PHẦN 1: KIỂM TRA CẤU TRÚC BẢNG DATA_LOADER
-- (Chạy câu này trước để xem schema thực tế)
-- ---------------------------------------------------------------
-- PRAGMA table_info(data_loader);


-- ---------------------------------------------------------------
-- PHẦN 2: THÊM CẤU HÌNH LOADER MỚI
--
-- Giải thích các trường:
--   loader_name     : tên định danh duy nhất cho loader này
--   source_dir      : thư mục Auto Loader sẽ quét tìm CSV mới
--                     (= thư mục OUTBOX_DIR trong .env của ETL)
--   success_dir     : thư mục chuyển file khi load thành công
--   failure_dir     : thư mục chuyển file khi load thất bại
--   target_table    : bảng đích trong MariaDB
--   target_conn     : connection string MariaDB
--   file_pattern    : chỉ quét file khớp pattern này (glob)
--   delimiter       : ký tự phân tách cột trong CSV
--   has_header      : CSV có dòng header hay không (1 = có)
--   enabled         : bật/tắt loader (1 = bật)
-- ---------------------------------------------------------------
INSERT INTO data_loader (
    loader_name,
    source_dir,
    success_dir,
    failure_dir,
    target_table,
    target_conn,
    file_pattern,
    delimiter,
    has_header,
    enabled
) VALUES (
    'telecom_cdr_pknguyen',                          -- Tên loader (unique)
    '/opt/etl/data/outbox',                          -- ⚠️ Thay bằng đường dẫn thực tế OUTBOX
    '/opt/etl/data/success',                         -- ⚠️ Thay bằng đường dẫn thực tế SUCCESS
    '/opt/etl/data/failure',                         -- ⚠️ Thay bằng đường dẫn thực tế FAILURE
    'telecom_cdr_pknguyen',                          -- Bảng đích MariaDB
    'mariadb://VD_USER:VD_PASS@VD_HOST:3306/VD_DB', -- ⚠️ Thay connection string thực tế
    'telecom_cdr_*.csv',                             -- Chỉ quét file có tên đúng pattern
    ',',                                             -- CSV dùng dấu phẩy làm delimiter
    1,                                               -- CSV có dòng header
    1                                                -- Bật loader
);


-- ---------------------------------------------------------------
-- PHẦN 3: KIỂM TRA SAU KHI CẤU HÌNH
-- ---------------------------------------------------------------
-- Xem cấu hình vừa tạo:
-- SELECT * FROM data_loader WHERE loader_name = 'telecom_cdr_pknguyen';

-- Xem log gần nhất của loader:
-- SELECT * FROM loader_log
-- WHERE loader_name = 'telecom_cdr_pknguyen'
-- ORDER BY created_at DESC
-- LIMIT 20;
