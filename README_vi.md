# Telecom CDR Auto Loader — ETL Service

> **Học viên:** pknguyen  
> **Phần mềm quản lý:** uv  
> **Ngôn ngữ văn bản:** [English](README.md) | [Tiếng Việt](README_vi.md)

---

## 🎯 Mục tiêu bài toán

Xây dựng một ETL service chạy định kỳ, tự động:
1. Kết nối vào **PostgreSQL** (nguồn dữ liệu cuộc gọi CDR).
2. Quét **bản ghi mới** một cách linh hoạt dựa trên Checkpoint theo dõi (ngăn load lại file cũ).
3. **Biến đổi dữ liệu gốc** chuẩn hoá (chuyển múi giờ Timestamp, map call_type, tính toán duration).
4. Viết file nén xuất dưới dạng **file CSV** an toàn tuyệt đối.
5. Gửi thẳng file CSV vào thư mục gốc `auto_loader` để nền tảng **Auto Loader** plugin bên ngoài quét tự động và kéo dữ liệu vào kho truy vấn MariaDB.
6. Thiết lập **SQLite Checkpoint** bền vững và chống chết nhằm bảo vệ ngăn chặn trùng bản ghi kể cả sau System Restart.

---

## 🏗️ Kiến trúc thiết kế

```text
┌─────────────────────────────────────────────────────────────┐
│                        ETL SERVICE                          │
│                                                             │
│  scheduler.py → etl_job.py (run_etl_job)                    │
│        │                                                    │
│        ▼                                                    │
│  extract.py ──── đọc từ PostgreSQL (chỉ id > last_id)       │
│        │                                                    │
│        ▼                                                    │
│  transform.py ── biến đổi, validate, tách valid/rejected    │
│        │                                                    │
│        ├──► csv_writer.py ──► auto_loader/*.csv             │
│        │                           │                        │
│        │                    (Auto Loader nhận)              │
│        │                      ↓         ↓                   │
│        │                     s/        f/                   │
│        │                                                    │
│        └──► csv_writer.py ──► auto_loader/rejected/*.csv    │
│                                                             │
│  checkpoint.py ─── auto_loader/checkpoint/checkpoint.db     │
└─────────────────────────────────────────────────────────────┘
```

Vòng lặp thông qua quy trình:
1. `checkpoint.py` tra soát DB SQLite nội tại để lôi ra con số `last_id`.
2. `db.py` mở port kết dính với hệ thống PostgreSQL (đã fix loop dự phòng ngắt nhánh).
3. `extract.py` truy xuất query với khối lệnh `id > last_id` chia bằng `BATCH_SIZE`.
4. `transform.py` soi lọc, thanh lý rác và format lại thông số.
5. `csv_writer.py` nhồi file nguyên tử kết xuất ra File System System.
6. `checkpoint.py` update và nhả mũi chốt ID tiến trình *chỉ duy nhất khi và chỉ khi* CSV báo file hoàn tất để bảo vệ chống trùng lặp dữ liệu mất mát.

---

## 📂 Tổ chức tài nguyên

```text
telecom_cdr_auto_loader/
├── README.md                   ← Cấu hình tài liệu chuẩn hóa Tiếng Anh
├── README_vi.md                ← Cấu hình tài liệu Tiếng Việt
├── pyproject.toml              ← Khai báo Project, Thư viện UV Manager
├── build.bat                   ← Batch build gọn docker local
├── .env.example                ← Template khai báo hệ thống Env
├── .gitignore                  
│
├── src/                        ← Source mã nguồn Python
│   ├── __init__.py             
│   ├── main.py                 ← Entry point Bootstrap chạy Service
│   ├── etl_job.py              ← Tách riêng module thao tác Run Process Core
│   ├── config.py               ← Biến Configuration tập trung
│   ├── db.py                   ← Cổng nối trực diện DB & module Auto Retry
│   ├── extract.py              ← Rút lõi Data DB
│   ├── transform.py            ← Lõi chỉnh sửa cấu trúc Data (Format, map, v.v..)
│   ├── csv_writer.py           ← Logic Rename Nguyên tử bóc tách File
│   ├── checkpoint.py           ← Quản trị SQLite Persistent Data File 
│   ├── scheduler.py            ← Vòng xoáy bộ đếm giờ mili-giây Tick
│   ├── logger.py               ← Cung cấp bộ máy Debug Log tự động
│   └── utils.py                ← Auto Generate Tree Pattern Folder
│
├── docker/
│   ├── Dockerfile              
│   ├── docker-compose.yml      
│   └── docker_images/          ← Nơi lưu Image Archive Build
│
├── sql/
│   └── sample_loader_config.sql← Lệnh nạp hệ thống Data Configuration cho SQL Setup
│
└── auto_loader/                ← Directory Mount Data cực kì an toàn
    ├── *.csv                   ← ✅ Kết xuất bốc File chuẩn chờ quét
    ├── s/                      ← Folder chứa file "Success" của Plugin đẩy vô
    ├── f/                      ← Folder chứa file rác lỗi kẹt cứng
    ├── rejected/               ← Các line records không trọn vẹn bị ETL vứt bỏ
    ├── checkpoint/             ← Chứa DB Storage Cache SQLite
    └── logs/                   ← Text xoắn vòng Terminal Watch Logs
```

---

## 🚀 Khởi chạy (Môi trường máy ảo Local trực tiếp)

### 1. Nền tảng cấu thành
- Python 3.11+
- Package Manager: `uv`

### 2. Thiết đặt Hệ thống Biến Môi trường (Environment Variables)

```bash
uv sync # Gom mọi yêu cầu vào một không gian .venv riêng rẽ tách gộp an toàn
cp .env.example .env
```
Các tham số Core điều hướng toàn app:
| Tham biến | Chú thích | Căn bản |
|----------|-------------|---------|
| `POSTGRES_HOST` | IP Dữ liệu nguồn trỏ đến Database Server PG | `mariadb.emerald...` |
| `SCHEDULE_INTERVAL_SECONDS` | Nhịp thời gian gọi Service lặp vòng tính theo giây | `10` |
| `BATCH_SIZE` | Ném ra tối đa Rows giới hạn để chia mỏng dung lượng | `1000` |
| `AUTO_LOADER_DIR` | Khai báo điểm tiếp nhận Root Loader Folder | `auto_loader` |

### 3. Vận Hành Run Application
```bash
uv run python -m src.main
```

---

## 🐳 Vận Hành qua Container (Docker Compose)

Hệ thống được thiết kế nhúng cực mạnh mẽ liên kết System Host nhằm giữ được khả năng atomic đổi tên và tính lưu trữ Database Checkpoint bất khả xâm phạm.

### Execute Lệnh Khởi Động
```bash
cd docker
docker compose up -d --build
docker compose logs -f etl
```

**Khuyến cáo Bảo Vệ Đường Ống Output Mount:**
Thư mục `/opt/data/loader/auto_loader_nguyenphung` (hoặc mount thẳng vô `auto_loader` tại Windows Host) là điểm bám của chùm Checkpoint SQLite, CSV và Output System Log. 
Nhờ cơ chế Mapping Volume này, những lần tái sinh Container qua lệnh down/run/stop hoàn toàn không bị trượt Data, Database SQLite sẽ không bị chém mất.

---

## 🛡️ Thiết Kế Chống Dữ Liệu Bẩn & Nát Cấu Trúc Khối Nền File

### 1. Phép Đổi Tên Nguyên Tử (Atomic Replace Lock)
Một hệ thống Auto Loader độc lập trên server luôn khoái cắn nhầm file cực nhanh trong khi ETL còn đang ngắc ngoải ghi chép từng Byte. 
Dự án cản đứng rủi ro này qua chuỗi phản ứng liên kết:
- Xả file sang hệ thống mạo danh đuôi rác tránh máy đọc rà quét: `telecom_cdr_...csv.tmp`.
- Buộc hệ điều hành tống khứ bộ nhớ ghi dở văng thẳng vật lý về ổ cứng phần vỏ `os.fsync(f.fileno())`. Tránh việc HĐH Cache Data ảo lươn lẹo.
- Tung lệnh đổi tên sấm sét `os.replace` thành file `.csv` sáng rực. Auto Loader nhảy bén chộp ngay đúng 1 khối file hoàn hảo cực kỳ sắc bén, hoàn toàn vô hiệu hoá các lỗi Read/Partial Data nhầm lẫn của Plugin quét ngoài.

### 2. Sự Lì Lợm Của Checkpoint SQLite Memory Cache
Thói quen dùng file .JSON rất dễ bị xé xác và Corrupt nếu như máy vi tính sập điện ngẫu nhiên lúc System đang ghi chồng dữ liệu file JSON Checkpoint (khiến hệ mã trở nên rỗng). 
Sự thiết đặt kết hợp Database nội bộ **SQLite File** khép kín chặn đứng điều này qua Atomic Transaction Write Database. Nó chỉ Commit khóa `id` *bắt buộc sau khi quá trình ghi đĩa File CSV báo kết quả Success Rate 100%*. Dù ngắt mạng, sập nguồn giữa chừng thì SQLite sẽ Rollback an toàn toàn vẹn dòng Checkpoint và Load lại đúng khúc Pipeline cũ một cách tinh khiết cực phẩm!
