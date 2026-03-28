# Telecom CDR Auto Loader — ETL Service

> **Trainee:** pknguyen  
> **Environment Manager:** uv  
> **Languages:** [English](README.md) | [Tiếng Việt](README_vi.md)

---

## 🎯 Objective

Build a periodically running ETL service that automatically:
1. Connects to **PostgreSQL** (the telecom CDR data source).
2. Fetches **only new records** based on a persistent tracking checkpoint.
3. Applies **data transformations** (timestamp conversion, call_type mapping, duration calculation, etc.).
4. Writes data into a **CSV file** safely using atomic replace operations.
5. Places the file into the `auto_loader` root folder for the external **Auto Loader** service to dynamically pick up and insert into MariaDB.
6. Maintains a persistent **SQLite Checkpoint** to prevent duplicate loading across system restarts.

---

## 🏗️ Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                        ETL SERVICE                          │
│                                                             │
│  scheduler.py → etl_job.py (run_etl_job)                    │
│        │                                                    │
│        ▼                                                    │
│  extract.py ──── read from PostgreSQL (id > last_id)        │
│        │                                                    │
│        ▼                                                    │
│  transform.py ── transform, validate, split valid/rejected  │
│        │                                                    │
│        ├──► csv_writer.py ──► auto_loader/*.csv             │
│        │                           │                        │
│        │                     (Auto Loader)                  │
│        │                      ↓         ↓                   │
│        │                     s/        f/                   │
│        │                                                    │
│        └──► csv_writer.py ──► auto_loader/rejected/*.csv    │
│                                                             │
│  checkpoint.py ─── auto_loader/checkpoint/checkpoint.db     │
└─────────────────────────────────────────────────────────────┘
```

The flow executes in the following sequence:
1. `checkpoint.py` reads the local SQLite tracker to know the `last_id`.
2. `db.py` opens a connection to PostgreSQL (with retry mechanism).
3. `extract.py` queries for `id > last_id` up to a specified `BATCH_SIZE`.
4. `transform.py` validates and processes each record. 
5. `csv_writer.py` atomically writes the valid chunks to a CSV.
6. `checkpoint.py` updates the tracker ID *only if* the CSV was written successfully.

---

## 📂 Directory Structure

```text
telecom_cdr_auto_loader/
├── README.md                   ← English documentation
├── README_vi.md                ← Vietnamese documentation
├── pyproject.toml              ← Project configuration and dependencies for uv
├── build.bat                   ← Docker build script helper
├── .env.example                ← Environment variables template
├── .gitignore                  
│
├── src/                        ← Python source code
│   ├── __init__.py             
│   ├── main.py                 ← ETL loop entrypoint
│   ├── etl_job.py              ← Core run_etl_job() logic
│   ├── config.py               ← Config loader module
│   ├── db.py                   ← PostgreSQL connection & retries
│   ├── extract.py              ← Data extraction
│   ├── transform.py            ← Data transformations & validation
│   ├── csv_writer.py           ← Atomic CSV writing logic
│   ├── checkpoint.py           ← SQLite checkpoint management
│   ├── scheduler.py            ← Interval execution loop
│   ├── logger.py               ← Logging configuration
│   └── utils.py                ← Auto dir tree setup snippet
│
├── docker/
│   ├── Dockerfile              ← Docker image specs
│   ├── docker-compose.yml      ← Docker compose specs
│   └── docker_images/          ← Images stored here after build.bat
│
├── sql/
│   └── sample_loader_config.sql← Sample SQL config for Auto Loader
│
└── auto_loader/                ← Exported mounted folder (Bind Mount)
    ├── *.csv                   ← ✅ Valid CSVs directly dropped here
    ├── s/                      ← Success files moved by auto_loader
    ├── f/                      ← Failed files moved by auto_loader
    ├── rejected/               ← Invalid rows dumped here by ETL
    ├── checkpoint/             ← SQLite DB tracking checkpoint
    └── logs/                   ← Rotating service logs
```

---

## 🚀 Setup & Installation (Local)

### 1. Requirements
- Python 3.11+
- `uv` Package Manager

### 2. Environment Setup

```bash
uv sync # Generate standard .venv inside with all dependencies installed.
cp .env.example .env
```
Edit `.env` configurations:
| Variable | Description | Default |
|----------|-------------|---------|
| `POSTGRES_HOST` | Source PG host | `mariadb.emerald.dataplatformsolution.com` |
| `SCHEDULE_INTERVAL_SECONDS` | Run interval in seconds | `10` |
| `BATCH_SIZE` | Max rows per block | `1000` |
| `AUTO_LOADER_DIR` | Auto_Loader output dir | `auto_loader` |

### 3. Run the Service
```bash
uv run python -m src.main
```

---

## 🐳 Docker Deployment

For server deployments, a fully integrated Docker Compose approach is pre-configured. The `/opt/data/loader/auto_loader_nguyenphung` volume mapping secures atomic updates efficiently on Linux.

### Run Via Docker Compose
```bash
cd docker
docker compose up -d --build
docker compose logs -f etl
```

**Persistent Volume Important Details**:
The `auto_loader` folder is strongly coupled as a host bind mount directly mapping state persistence, SQLite, and generated logs natively without risking container recreation anomalies.

---

## 🛡️ Fault Tolerance & Atomicity

### 1. Avoiding Partial Reads (Atomic Replace)
External Data Loaders frequently risk reading corrupted entries if they scan a file currently being written. Our safety net:
- File is initially written utilizing a `.tmp` extension strictly ignored by loaders: `telecom_cdr_...143022.csv.tmp`.
- `os.fsync(f.fileno())` heavily pushes data down avoiding IO caching buffers.
- Fast `os.replace` atomically drops the final `.csv`, exposing a completely integrated file immediately readable to the Auto Loader without corruption risks.

### 2. Checkpoint Reliability
SQLite operates atomically. In case of unexpected shutdowns, JSON files can get corrupted mid-write, locking up the ETL instance. Using SQLite effectively ensures maximum stability. `id` tracking occurs purely AFTER the CSVs fully commit. Checkpoints NEVER shift on fail-events, preventing missing chunks.
