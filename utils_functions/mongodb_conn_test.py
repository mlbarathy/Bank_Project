from pymongo import MongoClient
import urllib.parse

# Credentials and host
username = urllib.parse.quote_plus("lakshimi.mariappan")
password = urllib.parse.quote_plus("07vl79k4VfvIws9F")
host = "edl-mongodb.atgdmap.stage"
port = 27017
authSource = "admin"
database_name = "hotel_calendar_pricing"

# Connection URI
uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={authSource}"

# Connect and list collections
client = MongoClient(uri, serverSelectionTimeoutMS=5000)

try:
    db = client[database_name]
    collections = db.list_collection_names()
    print(f"📂 Collections in '{database_name}':")
    for coll in collections:
        print(f" - {coll}")
except Exception as e:
    print(f"❌ Error: {e}")
