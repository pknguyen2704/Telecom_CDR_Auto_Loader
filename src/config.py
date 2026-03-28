"""
config.py — Đọc và lưu tất cả cấu hình từ file .env

Mọi giá trị cấu hình đều được kéo từ biến môi trường.
Nếu biến môi trường chưa được set, dùng giá trị mặc định (default).
Module này được import ở tất cả các file khác để lấy cấu hình,
tránh hardcode giá trị ở nhiều nơi.
"""

import os
from dotenv import load_dotenv

# Tải file .env vào các biến môi trường
# Nếu file .env không tồn tại thì không báo lỗi, dùng biến hệ thống
load_dotenv()


# ============================================================
# HÀM HỖ TRỢ ĐỌC BIẾN MÔI TRƯỜNG
# ============================================================

def get_env(key: str, default: str = "") -> str:
    """Đọc một biến môi trường dạng chuỗi, trả về default nếu không có."""
    return os.environ.get(key, default)


def get_env_int(key: str, default: int = 0) -> int:
    """Đọc một biến môi trường và ép kiểu sang số nguyên."""
    value = os.environ.get(key, str(default))
    try:
        return int(value)
    except ValueError:
        return default


# ============================================================
# CẤU HÌNH KẾT NỐI POSTGRESQL (NGUỒN DỮ LIỆU)
# ============================================================
POSTGRES_HOST = get_env("POSTGRES_HOST", "mariadb.emerald.dataplatformsolution.com")
POSTGRES_PORT = get_env_int("POSTGRES_PORT", 5432)
POSTGRES_DB   = get_env("POSTGRES_DB", "ps_db")
POSTGRES_USER = get_env("POSTGRES_USER", "ps_user")
POSTGRES_PASSWORD = get_env("POSTGRES_PASSWORD", "nGqEC3P009PSiKDe")

# Bảng nguồn trong PostgreSQL
SOURCE_TABLE = get_env("SOURCE_TABLE", "public.telecom_cdr")

# ============================================================
# CẤU HÌNH LỊCH CHẠY VÀ KÍCH THƯỚC BATCH
# ============================================================

# Cứ mỗi bao nhiêu phút thì chạy ETL một lần
SCHEDULE_INTERVAL_MINUTES = get_env_int("SCHEDULE_INTERVAL_MINUTES", 5)

# Số bản ghi tối đa lấy mỗi lần chạy (tránh file CSV quá lớn)
BATCH_SIZE = get_env_int("BATCH_SIZE", 10000)

# ============================================================
# CẤU HÌNH CÁC THƯ MỤC DỮ LIỆU
# ============================================================

# Thư mục outbox: nơi đặt CSV để Auto Loader đọc
OUTBOX_DIR = get_env("OUTBOX_DIR", "data/outbox")

# Thư mục Auto Loader chuyển file sau khi load thành công
SUCCESS_DIR = get_env("SUCCESS_DIR", "data/success")

# Thư mục Auto Loader chuyển file khi load thất bại
FAILURE_DIR = get_env("FAILURE_DIR", "data/failure")

# Thư mục lưu file CSV chứa các bản ghi bị loại (không hợp lệ)
REJECTED_DIR = get_env("REJECTED_DIR", "data/rejected")

# Thư mục lưu file checkpoint (SQLite)
CHECKPOINT_DIR = get_env("CHECKPOINT_DIR", "data/checkpoint")

# ============================================================
# CẤU HÌNH LOG
# ============================================================
LOG_DIR = get_env("LOG_DIR", "logs")

# ============================================================
# TÊN FILE CHECKPOINT
# ============================================================
CHECKPOINT_DB_NAME = get_env("CHECKPOINT_DB_NAME", "checkpoint.db")
