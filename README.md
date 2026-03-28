# Telecom CDR Auto Loader — ETL Service

> **Tên sinh viên / Trainee:** pknguyen  
> **Ngày hoàn thành:** 2026-03-27  
> **Ngôn ngữ:** Python 3.11  
> **Quản lý môi trường:** uv  

---

## Mục tiêu bài toán

Xây dựng một ETL service chạy định kỳ, tự động:

1. Kết nối vào **PostgreSQL** (nguồn dữ liệu CDR viễn thông)
2. Lấy **chỉ những bản ghi mới** dựa trên checkpoint (không load lại dữ liệu cũ)
3. Áp dụng **các biến đổi dữ liệu** (chuyển timestamp, map call_type, tính duration_minutes, ...)
4. Ghi dữ liệu ra **file CSV** một cách an toàn
5. Đặt file CSV vào **thư mục outbox** để **Auto Loader** tự động đọc và nạp vào MariaDB
6. **Checkpoint** được lưu bền vững để tránh load trùng lặp khi service restart

---

## Kiến trúc tổng quan

```
┌─────────────────────────────────────────────────────────────┐
│                        ETL SERVICE                          │
│                                                             │
│  scheduler.py → main.py (run_etl_job)                      │
│        │                                                    │
│        ▼                                                    │
│  extract.py ──── đọc từ PostgreSQL (chỉ id > last_id)      │
│        │                                                    │
│        ▼                                                    │
│  transform.py ── biến đổi, validate, tách valid/rejected   │
│        │                                                    │
│        ├──► csv_writer.py ──► data/outbox/*.csv            │
│        │                           │                        │
│        │                    (Auto Loader đọc)               │
│        │                      ↓         ↓                   │
│        │               success/      failure/               │
│        │                                                    │
│        └──► csv_writer.py ──► data/rejected/*.csv          │
│                                                             │
│  checkpoint.py ─── data/checkpoint/checkpoint.db           │
└─────────────────────────────────────────────────────────────┘
```

Luồng dữ liệu từng bước:

| Bước | Module | Mô tả |
|------|--------|--------|
| 1 | `checkpoint.py` | Đọc checkpoint — biết phải lấy dữ liệu từ `id` nào |
| 2 | `db.py` | Mở kết nối đến PostgreSQL |
| 3 | `extract.py` | Lấy bản ghi có `id > last_id`, tối đa `BATCH_SIZE` dòng |
| 4 | `transform.py` | Biến đổi và validate từng bản ghi |
| 5 | `csv_writer.py` | Ghi CSV an toàn (temp file → atomic rename) |
| 6 | `checkpoint.py` | Cập nhật checkpoint sau khi ghi CSV thành công |

---

## Cấu trúc thư mục

```
telecom_cdr_auto_loader/
├── README.md                   ← File này
├── pyproject.toml              ← Cấu hình project và dependencies cho uv
├── .env.example                ← Mẫu file cấu hình môi trường
├── .gitignore                  ← Các file/thư mục không commit lên Git
│
├── src/                        ← Toàn bộ source code Python
│   ├── __init__.py             ← Đánh dấu src/ là Python package
│   ├── main.py                 ← Điểm khởi động, hàm run_etl_job()
│   ├── config.py               ← Đọc cấu hình từ biến môi trường
│   ├── db.py                   ← Kết nối PostgreSQL, retry logic
│   ├── extract.py              ← Lấy dữ liệu mới từ PostgreSQL
│   ├── transform.py            ← Biến đổi và validate dữ liệu
│   ├── csv_writer.py           ← Ghi CSV an toàn (temp + rename)
│   ├── checkpoint.py           ← Lưu/đọc checkpoint bằng SQLite
│   ├── scheduler.py            ← Lập lịch chạy ETL định kỳ
│   ├── logger.py               ← Cấu hình logging (console + file)
│   └── utils.py                ← Tạo thư mục, in banner
│
├── docker/
│   ├── Dockerfile              ← Cấu hình đóng gói Docker
│   └── docker-compose.yml      ← Cấu hình chạy Docker Compose
│
├── sql/
│   ├── create_target_table.sql ← DDL tạo bảng đích MariaDB
│   └── sample_loader_config.sql← Mẫu SQL cấu hình Auto Loader
│
├── data/                       ← Dữ liệu runtime (gitignore CSV, giữ cấu trúc)
│   ├── outbox/                 ← ✅ Auto Loader đọc file từ đây
│   ├── success/                ← Auto Loader chuyển file thành công vào đây
│   ├── failure/                ← Auto Loader chuyển file lỗi vào đây
│   ├── rejected/               ← ETL ghi bản ghi không hợp lệ vào đây
│   └── checkpoint/             ← File SQLite checkpoint.db
│
└── logs/                       ← File log ETL (etl.log, xoay vòng)
```

---

## Cách cài đặt bằng uv

### Yêu cầu hệ thống

- Python 3.11 trở lên
- `uv` (cài bằng lệnh bên dưới nếu chưa có)

### Bước 1: Cài uv (nếu chưa có)

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Kiểm tra cài thành công
uv --version
```

### Bước 2: Clone project và vào thư mục

```bash
git clone <url-repo>
cd telecom_cdr_auto_loader
```

### Bước 3: Tạo môi trường ảo và cài dependencies

```bash
# uv tự tạo .venv và cài đúng phiên bản Python + packages
uv sync
```

> Lệnh `uv sync` đọc `pyproject.toml`, tạo môi trường ảo `.venv/`,
> và cài tất cả dependencies. Tất cả chỉ trong một lệnh!

---

## Cách cấu hình `.env`

```bash
# Copy file mẫu
cp .env.example .env

# Mở file .env và chỉnh sửa
nano .env   # hoặc dùng trình soạn thảo yêu thích
```

Các biến quan trọng cần chỉnh:

| Biến | Mô tả | Giá trị mặc định |
|------|--------|-----------------|
| `POSTGRES_HOST` | Host PostgreSQL nguồn | `mariadb.emerald.dataplatformsolution.com` |
| `POSTGRES_PASSWORD` | Mật khẩu PostgreSQL | Xem trong assignment |
| `SCHEDULE_INTERVAL_MINUTES` | Chạy ETL mỗi bao nhiêu phút | `5` |
| `BATCH_SIZE` | Số bản ghi tối đa mỗi lần | `10000` |
| `OUTBOX_DIR` | Thư mục đặt CSV cho Auto Loader | `data/outbox` |

---

## Cách chạy local

```bash
# Kích hoạt môi trường ảo
source .venv/bin/activate     # macOS/Linux
# hoặc: .venv\Scripts\activate  # Windows

# Chạy ETL service
python -m src.main
```

Hoặc dùng uv run (không cần kích hoạt thủ công):

```bash
uv run python -m src.main
```

Bạn sẽ thấy log như sau khi chạy thành công:

```
╔══════════════════════════════════════════════════════╗
║         TELECOM CDR AUTO LOADER — ETL SERVICE        ║
╠══════════════════════════════════════════════════════╣
║  PostgreSQL : mariadb.emerald.dataplatformsolution.com:5432
║  Database   : ps_db
║  Bảng nguồn : public.telecom_cdr
║  Batch size : 10000 bản ghi
║  Lịch chạy  : mỗi 5 phút
╚══════════════════════════════════════════════════════╝

[2026-03-27 15:30:00] [INFO    ] [src.main] Checkpoint đã sẵn sàng tại: data/checkpoint/checkpoint.db
[2026-03-27 15:30:00] [INFO    ] [src.main] ETL Service đã khởi động thành công.
[2026-03-27 15:30:00] [INFO    ] [src.scheduler] Chạy ETL lần đầu ngay bây giờ...
[2026-03-27 15:30:00] [INFO    ] [src.main] ============================================================
[2026-03-27 15:30:00] [INFO    ] [src.main] 🚀 BẮT ĐẦU LẦN CHẠY ETL MỚI
[2026-03-27 15:30:00] [INFO    ] [src.main] Checkpoint hiện tại: last_id=0
```

---

## Cách chạy bằng Docker

### Bước 1: Build và chạy

```bash
# Vào thư mục docker/
cd docker

# Build image và chạy container nền
docker compose up -d --build

# Xem log real-time
docker compose logs -f etl
```

### Bước 2: Kiểm tra trạng thái

```bash
docker compose ps          # Xem container có đang chạy không
docker compose logs etl    # Xem log
```

### Bước 3: Dừng service

```bash
docker compose down        # Dừng và xóa container (DATA VẪN CÒN vì dùng bind mounts)
docker compose stop        # Chỉ dừng, không xóa container
```

### Lưu ý về dữ liệu bền vững khi dùng Docker

Thư mục `data/` và `logs/` đã được cấu hình là bind mounts trong `docker-compose.yml`:

```yaml
volumes:
  - ../data:/app/data    # thư mục data/ trên host ↔ /app/data trong container
  - ../logs:/app/logs    # thư mục logs/ trên host ↔ /app/logs trong container
```

→ Khi container bị xóa (`docker compose down`) và tạo lại (`docker compose up`),
  file checkpoint.db vẫn còn nguyên → ETL tiếp tục từ đúng vị trí cũ, không load lại dữ liệu.

---

## Cách deploy lên PROC server

### Option 1: Chạy trực tiếp (không Docker)

```bash
# 1. Upload code lên PROC server
scp -r . user@proc-server:/opt/etl/telecom_cdr_auto_loader/

# 2. Kết nối vào server
ssh user@proc-server

# 3. Cài uv
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc

# 4. Vào thư mục project
cd /opt/etl/telecom_cdr_auto_loader

# 5. Cài dependencies
uv sync

# 6. Copy và sửa .env
cp .env.example .env
vi .env   # Sửa các đường dẫn thành đường dẫn tuyệt đối:
           # OUTBOX_DIR=/opt/etl/data/outbox
           # SUCCESS_DIR=/opt/etl/data/success
           # ...

# 7. Chạy với nohup để không bị tắt khi đóng SSH
nohup uv run python -m src.main > logs/nohup.log 2>&1 &

# Lưu PID để có thể kill sau này
echo $! > etl.pid

# 8. Kiểm tra xem có chạy không
tail -f logs/etl.log
```

### Option 2: Dùng Docker trên PROC server (khuyến nghị)

```bash
# 1. Upload code
scp -r . user@proc-server:/opt/etl/telecom_cdr_auto_loader/

# 2. Kết nối server, vào thư mục docker
ssh user@proc-server
cd /opt/etl/telecom_cdr_auto_loader

# 3. Tạo .env
cp .env.example .env
# Sửa OUTBOX_DIR, SUCCESS_DIR, ... thành đường dẫn tuyệt đối trên server

# 4. Build và chạy
cd docker
docker compose up -d --build

# 5. Kiểm tra
docker compose logs -f etl
```

### Chú ý quan trọng khi deploy

- Đường dẫn trong `.env` trên server phải là **đường dẫn tuyệt đối**.
- **Không restart loader service** nếu người khác trong team đã làm rồi.
  Loader service là dùng chung, chỉ cần cấu hình `data_loader` entry là đủ.
- Thư mục `OUTBOX_DIR` trên server phải trùng với thư mục mà Auto Loader đang quét.

---

## Cách hoạt động của checkpoint

### Khái niệm

Checkpoint là "dấu đánh dấu" mà ETL đã đọc đến, được lưu trong file SQLite:

```
data/checkpoint/checkpoint.db
```

Bảng `checkpoint` trong SQLite chứa một dòng duy nhất:

| Cột | Ý nghĩa | Ví dụ |
|-----|---------|-------|
| `last_id` | id lớn nhất đã xử lý | `5000` |
| `last_event_time` | event_time UTC tương ứng | `2024-01-15 08:30:00` |
| `updated_at` | Thời điểm checkpoint được cập nhật | `2026-03-27 10:00:00` |

### Cách hoạt động từng bước

```
Lần chạy 1 (lần đầu, checkpoint = 0):
  SELECT * FROM telecom_cdr WHERE id > 0 ORDER BY id LIMIT 10000
  → Lấy được id 1 đến 10000
  → Ghi CSV thành công
  → Cập nhật checkpoint: last_id = 10000

Lần chạy 2 (sau 5 phút):
  SELECT * FROM telecom_cdr WHERE id > 10000 ORDER BY id LIMIT 10000
  → Lấy được id 10001 đến 20000 (hoặc ít hơn nếu chưa có dữ liệu mới)
  → Chỉ lấy bản ghi MỚI, không lặp lại dữ liệu cũ
```

### Tại sao dùng SQLite thay vì file JSON?

- **JSON**: đơn giản nhưng nếu chương trình crash khi đang ghi, file JSON có thể hỏng.
- **SQLite**: ghi nguyên tử — hoặc ghi xong hoàn toàn, hoặc không ghi gì cả.
  Không bao giờ bị corrupt dở dang.
- SQLite dễ kiểm tra bằng DB Browser for SQLite (ứng dụng desktop miễn phí).

---

## Cách chống duplicate

Có 3 lớp bảo vệ chống load trùng dữ liệu:

### Lớp 1: Checkpoint (ETL không fetch lại dữ liệu cũ)

```python
# Chỉ lấy bản ghi có id lớn hơn last_id đã lưu trong checkpoint
WHERE id > last_id ORDER BY id LIMIT batch_size
```

### Lớp 2: Checkpoint chỉ cập nhật sau khi ghi CSV thành công

```python
# Trong main.py — thứ tự QUAN TRỌNG:
output_path = write_csv(valid_records)   # 1. Ghi CSV trước
checkpoint.save_checkpoint(new_last_id)  # 2. Cập nhật checkpoint SAU
```

→ Nếu ghi CSV thất bại, checkpoint không đổi → lần sau fetch lại đúng batch đó.

### Lớp 3: Khóa chính (PRIMARY KEY) trong MariaDB

```sql
PRIMARY KEY (id)
```

→ Dù ETL load trùng một file, MariaDB sẽ từ chối bản ghi đã tồn tại (duplicate key error).
→ Đây là lớp bảo vệ cuối cùng.

### Hạn chế của chiến lược này

- Nếu bảng nguồn có bản ghi cũ được **insert với id nhỏ hơn** last_id đã checkpoint, chúng sẽ bị bỏ qua vĩnh viễn. Với CDR event table thì id luôn tăng dần nên không xảy ra.
- Nếu `id` là UUID ngẫu nhiên (không tăng dần), cần dùng `created_at` làm checkpoint thay thế.

---

## Cách chống file ghi dở bị Auto Loader đọc nhầm

### Vấn đề

Nếu ghi thẳng vào `telecom_cdr_20240315.csv` và chương trình crash giữa chừng,
Auto Loader sẽ thấy một file **không đầy đủ** và load dữ liệu thiếu vào MariaDB.

### Giải pháp: Ghi file tạm + đổi tên nguyên tử

```
Bước 1: Ghi ra file TẠM:
  data/outbox/telecom_cdr_20240315_143022.csv.tmp   ← Auto Loader KHÔNG thấy file này

Bước 2: flush() + fsync():
  Đảm bảo toàn bộ dữ liệu đã được ghi xuống ổ đĩa vật lý

Bước 3: os.replace() — đổi tên nguyên tử:
  data/outbox/telecom_cdr_20240315_143022.csv.tmp
       → data/outbox/telecom_cdr_20240315_143022.csv   ← Auto Loader THẤY file này
```

Lệnh `os.replace()` trên Linux/macOS là **atomic** (nguyên tử):
- Auto Loader chỉ thấy file sau khi đổi tên hoàn toàn xong.
- Không bao giờ thấy file đang ghi dở.
- Nếu crash trước khi rename, chỉ còn file `.tmp` vô hại, không ảnh hưởng Auto Loader.

---

## Mô tả các bước ETL

```
┌─────────────────────────────────────────────────────────────┐
│ 1. EXTRACT (extract.py)                                     │
│    - Đọc checkpoint: last_id = 5000                        │
│    - Query: SELECT * FROM telecom_cdr WHERE id > 5000       │
│              ORDER BY id ASC LIMIT 10000                    │
│    - Nhận về 3000 bản ghi mới                              │
├─────────────────────────────────────────────────────────────│
│ 2. TRANSFORM (transform.py)                                 │
│    Với mỗi bản ghi:                                        │
│    - Kiểm tra trường bắt buộc (id, caller, receiver, ...)  │
│    - event_time: 1700000000 → "2023-11-14 22:13:20"       │
│    - call_type: "MO" → "Outgoing", "MT" → "Incoming"       │
│    - duration_minutes: 120 / 60 = 2.00                     │
│    - tower_lat, tower_lng: "10.823" → 10.823 (float)       │
│    - Bản ghi hợp lệ → valid_records (2980 bản ghi)        │
│    - Bản ghi lỗi → rejected_records (20 bản ghi)          │
├─────────────────────────────────────────────────────────────│
│ 3. LOAD — GHI CSV (csv_writer.py)                          │
│    - Ghi valid_records → data/outbox/telecom_cdr_*.csv     │
│    - Ghi rejected_records → data/rejected/rejected_*.csv   │
│    - Dùng temp file + atomic rename                        │
├─────────────────────────────────────────────────────────────│
│ 4. CHECKPOINT (checkpoint.py)                              │
│    - Cập nhật: last_id = max(id trong valid_records) = 8000│
└─────────────────────────────────────────────────────────────┘
```

---

## Mô tả transform

| Field | Trước transform | Sau transform |
|-------|----------------|---------------|
| `event_time_unix` | `1700000000` (int) | `1700000000` (giữ nguyên) |
| `event_time_utc` | — (không có) | `"2023-11-14 22:13:20"` (string UTC) |
| `call_type_code` | `"MO"` | `"MO"` (giữ nguyên) |
| `call_type_name` | — (không có) | `"Outgoing"` |
| `duration_seconds` | `120` | `120` |
| `duration_minutes` | — (không có) | `2.00` |
| `tower_lat` | `"10.823456"` (string) | `10.823456` (float) |
| `tower_lng` | `"106.629999"` (string) | `106.629999` (float) |
| `caller` | `" 0901234567 "` | `"0901234567"` (đã trim) |

---

## Mô tả log và rejected records

### File log: `logs/etl.log`

Mỗi lần chạy ETL sẽ in ra:

```log
[2026-03-27 15:30:00] [INFO    ] [src.main] ============================================================
[2026-03-27 15:30:00] [INFO    ] [src.main] 🚀 BẮT ĐẦU LẦN CHẠY ETL MỚI
[2026-03-27 15:30:00] [INFO    ] [src.checkpoint] Checkpoint hiện tại: last_id=5000
[2026-03-27 15:30:01] [INFO    ] [src.db] Kết nối PostgreSQL thành công!
[2026-03-27 15:30:02] [INFO    ] [src.extract] Lấy được 3000 bản ghi mới từ PostgreSQL
[2026-03-27 15:30:03] [WARNING ] [src.transform] Bỏ qua bản ghi id=5123: Thiếu trường 'caller'
[2026-03-27 15:30:04] [INFO    ] [src.csv_writer] ✅ File CSV đã sẵn sàng: data/outbox/telecom_cdr_20240315_153004.csv
[2026-03-27 15:30:04] [INFO    ] [src.checkpoint] Checkpoint đã cập nhật: last_id=8000
[2026-03-27 15:30:04] [INFO    ] [src.main] 📊 KẾT QUẢ LẦN CHẠY ETL:
[2026-03-27 15:30:04] [INFO    ] [src.main]    - Checkpoint trước : last_id=5000
[2026-03-27 15:30:04] [INFO    ] [src.main]    - Bản ghi lấy về   : 3000
[2026-03-27 15:30:04] [INFO    ] [src.main]    - Bản ghi hợp lệ   : 2980
[2026-03-27 15:30:04] [INFO    ] [src.main]    - Bản ghi bị loại  : 20
[2026-03-27 15:30:04] [INFO    ] [src.main]    - Checkpoint mới   : last_id=8000
```

### File rejected: `data/rejected/rejected_telecom_cdr_*.csv`

Chứa các bản ghi không qua được validate, kèm cột `reject_reason`:

```csv
id,caller,receiver,event_time,call_type,...,reject_reason
5123,,0901234568,1700000001,MO,...,Validation failed — xem log để biết chi tiết
```

→ Có thể tìm id trong file rejected → tìm log của id đó để biết lý do cụ thể.

---

## Các giả định của bài làm

1. **Bảng nguồn có cột `id` tăng dần (SERIAL/BIGINT)**: Đây là giả định quan trọng nhất.
   Chiến lược checkpoint `WHERE id > last_id` chỉ hoạt động đúng khi id không thay đổi.

2. **Bảng nguồn không xóa/cập nhật bản ghi đã có**: Nếu UPDATE xảy ra, ETL sẽ không detect được.
   Với CDR data thường là append-only nên OK.

3. **`event_time` là Unix timestamp dạng số nguyên (giây)**: Nếu là milliseconds cần chia thêm 1000.

4. **Auto Loader đã được cấu hình sẵn** để quét thư mục `outbox/` và biết bảng đích.
   Chỉ cần ETL đặt file vào đúng thư mục.

5. **Múi giờ**: Mọi datetime trong CSV đều là UTC. Auto Loader và MariaDB cần xử lý múi giờ ở tầng application.

---

## Hạn chế của solution

| Hạn chế | Giải thích |
|---------|------------|
| **Không detect UPDATE** | Nếu bản ghi cũ bị sửa, ETL không biết và không sync lại |
| **Checkpoint chỉ bảo vệ 1 chiều** | Nếu checkpoint bị xóa tay, ETL sẽ fetch lại từ đầu |
| **Không chạy song song** | Chỉ có một thread ETL, không scale horizontal |
| **Batch size cố định** | Không tự động điều chỉnh theo tốc độ của nguồn |
| **Không có alert** | Không gửi email/Slack khi ETL bị lỗi nhiều lần |

---

## Hướng cải tiến

1. **Thêm monitoring**: Gửi alert Slack/email khi ETL lỗi N lần liên tiếp.
2. **Dùng event-driven thay polling**: Nếu PostgreSQL hỗ trợ NOTIFY/LISTEN, phản ứng ngay khi có data mới.
3. **Deduplicate bằng UPSERT**: Thay INSERT bằng `INSERT ... ON DUPLICATE KEY UPDATE` để xử lý case load trùng.
4. **Cấu hình múi giờ**: Thêm config `OUTPUT_TIMEZONE` để chuyển datetime sang giờ địa phương nếu cần.
5. **Thêm unit tests**: Test hàm `transform_one()` với nhiều case edge.
6. **Distributed checkpoint**: Nếu scale ra nhiều instance ETL, dùng Redis hoặc database chung làm checkpoint.

---

## Hướng dẫn cấu hình Auto Loader (tóm tắt)

1. Chạy `sql/create_target_table.sql` trên MariaDB để tạo bảng đích.
2. Sửa `sql/sample_loader_config.sql` với đường dẫn và connection string thực tế.
3. Chạy SQL đó vào `sqlite_metadata.data_loader` (hỏi DBA nếu không có quyền).
4. Trigger restart loader service (chỉ 1 lần cho cả team).
5. Đặt một file CSV test vào thư mục `outbox/` và kiểm tra xem loader có pick up không.

---

## Hướng dẫn cấu hình DM

Tên DM: **Telecom CDR pknguyen**

Cấu hình recommended:
- **Type**: Managed DM
- **Enable file loading**: Bật
- **IsID field**: `id`
- **Searchable fields**: `caller`, `receiver`, `device_imei`, `country`, `call_type_name`
- **Field roles**: Tham khảo bảng ddl trong `sql/create_target_table.sql`

---

*Được tạo bởi pknguyen — Telecom CDR ETL Training Assignment*
