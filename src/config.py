import os
from dotenv import load_dotenv

load_dotenv()

def get_env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def get_env_int(key: str, default: int = 0) -> int:
    value = os.environ.get(key, str(default))
    try:
        return int(value)
    except ValueError:
        return default

POSTGRES_HOST = get_env("POSTGRES_HOST", "mariadb.emerald.dataplatformsolution.com")
POSTGRES_PORT = get_env_int("POSTGRES_PORT", 5432)
POSTGRES_DB   = get_env("POSTGRES_DB", "ps_db")
POSTGRES_USER = get_env("POSTGRES_USER", "ps_user")
POSTGRES_PASSWORD = get_env("POSTGRES_PASSWORD", "nGqEC3P009PSiKDe")
SOURCE_TABLE = get_env("SOURCE_TABLE", "public.telecom_cdr")
SCHEDULE_INTERVAL_SECONDS = get_env_int("SCHEDULE_INTERVAL_SECONDS", 10)
BATCH_SIZE = get_env_int("BATCH_SIZE", 1000)
AUTO_LOADER_DIR = get_env("AUTO_LOADER_DIR", "auto_loader")
REJECTED_DIR = get_env("REJECTED_DIR", "auto_loader/rejected")
CHECKPOINT_DIR = get_env("CHECKPOINT_DIR", "auto_loader/checkpoint")
LOG_DIR = get_env("LOG_DIR", "auto_loader/logs")
CHECKPOINT_DB_NAME = get_env("CHECKPOINT_DB_NAME", "checkpoint.db")
