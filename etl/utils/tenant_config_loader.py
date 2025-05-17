from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["roi_tracker"]

def get_all_tenant_configs():
    return list(db.tenants.find({}))
