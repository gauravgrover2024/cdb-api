import os

import certifi
from pymongo import MongoClient


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
colors_collection = db["vehicle_colors"]
price_history_collection = db["price_history"]
