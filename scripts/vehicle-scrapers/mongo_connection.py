import os
from pathlib import Path

import certifi
from pymongo import MongoClient


def _load_env_file() -> None:
    """
    Lightweight .env loader for standalone Python scripts.
    Keeps existing environment vars untouched.
    """
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue

        key, value = text.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            os.environ.setdefault(key, value)


_load_env_file()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in environment")

client = MongoClient(
    MONGO_URI,
    tls=True,
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=30000,
)

db_name = os.getenv("MONGO_DB_NAME")
if db_name:
    db = client[db_name]
else:
    try:
        db = client.get_default_database() or client["cdrive"]
    except Exception:
        db = client["cdrive"]

prices_collection = db["vehicles"]
features_collection = db["vehicle_features"]
colors_collection = db["vehicle_colors_v2"]
price_history_collection = db["price_history"]
offers_collection        = db["offers"]
service_costs_collection = db["service_costs"]
monthly_sales_collection = db["monthly_car_sales"]