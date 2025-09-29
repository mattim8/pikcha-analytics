# Импорт всех JSON из ./scripts/data/{stores,products,customers,purchases} в MongoDB.
# Требует: pip install pymongo

import os, json, glob
from pymongo import MongoClient
# from dotenv import load_dotenv

# Конфиг берём из переменных окружения (или дефолты), при желании можно load_dotenv(), подключаемся к
# MongoDB, Для каждой коллекции ('stores','products','customers','purchases') ищем все *.json в BASE_DIR/<coll>/, если файлов нет - прпускаем коллекцию;
# ordered=False позволяет операции не останавливаться в случае ошибки с одним из документов

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "piccha")
BASE_DIR  = os.getenv("DATA_DIR",  "scripts/data")  

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

for coll in ["stores", "products", "customers", "purchases"]:
    paths = glob.glob(f"{BASE_DIR}/{coll}/*.json")
    print(f"{coll}: файлов={len(paths)}")
    if not paths:
        continue

    # При CLEAR=1 коллекция предварительно очищается, полезно для повторных прогонов, чтобы не плодить дубли
    if os.getenv("CLEAR") == "1":
        db[coll].delete_many({})

    docs = []
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)
            docs.extend(obj if isinstance(obj, list) else [obj])

    if docs:
        db[coll].insert_many(docs, ordered=False)
        print(f"{coll}: вставлено={len(docs)}")

print("[DONE] импорт завершён")