-- ================================================================
-- create_target_table.sql
-- Tạo bảng đích trong MariaDB để Auto Loader load dữ liệu vào
--
-- Chạy file này MỘT LẦN trên MariaDB target database
-- trước khi cấu hình Auto Loader.
-- ================================================================

-- Tạo bảng nếu chưa tồn tại (an toàn khi chạy lại)
CREATE TABLE IF NOT EXISTS telecom_cdr_pknguyen (
    -- id: khóa chính, ánh xạ từ id của bảng nguồn PostgreSQL
    id                BIGINT          NOT NULL,

    -- Thông tin cuộc gọi
    caller            VARCHAR(50)     NOT NULL COMMENT 'Số điện thoại gọi đi',
    receiver          VARCHAR(50)     NOT NULL COMMENT 'Số điện thoại nhận',
    device_imei       VARCHAR(20)              COMMENT 'IMEI thiết bị',

    -- Thời gian sự kiện
    event_time_unix   BIGINT          NOT NULL COMMENT 'Thời gian Unix timestamp (giây)',
    event_time_utc    DATETIME        NOT NULL COMMENT 'Thời gian UTC dạng đọc được',

    -- Thời lượng cuộc gọi
    duration_seconds  INT             NOT NULL DEFAULT 0 COMMENT 'Thời lượng tính bằng giây',
    duration_minutes  DECIMAL(10, 2)  NOT NULL DEFAULT 0.00 COMMENT 'Thời lượng tính bằng phút',

    -- Loại cuộc gọi
    call_type_code    VARCHAR(10)     NOT NULL COMMENT 'Mã loại cuộc gọi: MO, MT, ...',
    call_type_name    VARCHAR(50)     NOT NULL COMMENT 'Tên loại cuộc gọi: Outgoing, Incoming',

    -- Vị trí tháp phát sóng
    tower_lat         DECIMAL(10, 6)           COMMENT 'Vĩ độ tháp: ví dụ 10.823456',
    tower_lng         DECIMAL(10, 6)           COMMENT 'Kinh độ tháp: ví dụ 106.629999',

    -- Quốc gia
    country           VARCHAR(100)             COMMENT 'Tên quốc gia',

    -- Thời điểm bản ghi được tạo trong nguồn
    created_at        DATETIME                 COMMENT 'Thời điểm tạo bản ghi gốc',

    -- Khóa chính
    PRIMARY KEY (id),

    -- Index hỗ trợ tìm kiếm nhanh theo các trường hay dùng
    INDEX idx_caller         (caller),
    INDEX idx_receiver       (receiver),
    INDEX idx_device_imei    (device_imei),
    INDEX idx_country        (country),
    INDEX idx_call_type_name (call_type_name),
    INDEX idx_event_time_utc (event_time_utc)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Bảng chứa dữ liệu CDR viễn thông đã transform — ETL bởi pknguyen';
