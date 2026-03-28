# ================================================================
# Dockerfile — Đóng gói ETL service thành Docker image
#
# Dùng Python 3.11 slim để image nhỏ gọn hơn
# Cài uv để quản lý dependencies nhanh hơn pip
# ================================================================

# Bước 1: Dùng Python 3.11 phiên bản slim (nhỏ gọn, không có compiler)
FROM python:3.11-slim

# Bước 2: Cài uv — package manager nhanh cho Python
# Dùng script cài đặt chính thức từ Astral
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Bước 3: Đặt thư mục làm việc trong container
WORKDIR /app

# Bước 4: Copy file cấu hình project trước (để tận dụng Docker cache)
# Nếu chỉ thay đổi code mà không thay dependencies,
# Docker sẽ dùng lại layer cài package đã cache → build nhanh hơn
COPY pyproject.toml ./

# Bước 5: Cài dependencies bằng uv (không cần .venv, cài thẳng vào hệ thống)
# --no-cache: không lưu cache để image nhỏ hơn
# --system: cài vào Python system, không tạo venv riêng
RUN uv pip install --system --no-cache -r pyproject.toml

# Bước 6: Copy toàn bộ source code vào container
COPY src/ ./src/

# Bước 7: Tạo các thư mục data/ và logs/ trong container
# (Các thư mục này sẽ được mount ra ngoài qua Docker volume)
RUN mkdir -p data/outbox data/success data/failure data/rejected data/checkpoint logs

# Bước 8: Biến môi trường mặc định (có thể ghi đè qua docker-compose hoặc .env)
ENV PYTHONUNBUFFERED=1
# PYTHONUNBUFFERED=1: buộc Python flush stdout/stderr ngay lập tức
# (quan trọng để log hiện ra trong docker logs -f)

# Bước 9: Lệnh chạy khi container khởi động
CMD ["python", "-m", "src.main"]
