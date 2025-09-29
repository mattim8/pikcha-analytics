# Mongo -> Kafka: шлём целый документ как JSON.
# Для PII: у customers и stores.manager удаляем email/phone и кладём *_hash.
# Требует: pip install kafka-python pymongo orjson
# re - используем один паттерн в регулярках, чтобы из телефона убрать всё, кроме цифр
# hashlib - для SHA-256 (хэш + соль)
# orjson - ускоряем сериализацию JSON(вернутся в bytes, для Kafka хорошо)

import os, re, hashlib
from pymongo import MongoClient
from kafka import KafkaProducer
import orjson

# код смотрит в окружение , если значения нет - берет дефолт
MONGO_URI    = os.getenv("MONGO_URI",   "mongodb://localhost:27017")
MONGO_DB     = os.getenv("MONGO_DB",    "piccha")
KAFKA_BROKER = os.getenv("KAFKA_BROKER","localhost:9093")  # с хоста: 9093; из контейнера: kafka:9092
PII_SALT     = os.getenv("PII_SALT",    "change_me")

TOPICS = {
    "stores":    "stores",
    "products":  "products",
    "customers": "customers",
    "purchases": "purchases",
}

# \D = non-digit, НЕцифры заменяются на пустое значение, останутся цифры
# по поводу соль здесь: мы удаляем email/phone и записываем только email_hash/phone_hash
_DIGITS = re.compile(r"\D+")
def _sha256_hex(s: str) -> str:
    h = hashlib.sha256()
    h.update((s + PII_SALT).encode("utf-8"))
    return h.hexdigest()

# нормализация мейла и телефона: убираем пробелы, всё лишнее кроме цифр и в нижний регистр и добавляем "+" в начало
def _norm_email(x) -> str:  return str(x or "").strip().lower()
def _norm_phone(x) -> str:
    d = _DIGITS.sub("", str(x or ""));  return ("+" + d) if d else ""

# читаем сырые мейлы/телефоны, удаляем оригиналы и добавляем хэши
def _sanitize_customer(d: dict) -> dict:
    email = _norm_email(d.get("email"))
    phone = _norm_phone(d.get("phone"))
    d.pop("email", None); d.pop("phone", None)
    d["email_hash"] = _sha256_hex(email) if email else ""
    d["phone_hash"] = _sha256_hex(phone) if phone else ""
    return d

def _sanitize_store(d: dict) -> dict:
    m = d.get("manager")
    if isinstance(m, dict):
        email = _norm_email(m.get("email"))
        phone = _norm_phone(m.get("phone"))
        d["manager"] = {
            "name": m.get("name", ""),
            "email_hash": _sha256_hex(email) if email else "",
            "phone_hash": _sha256_hex(phone) if phone else "",
        }
    return d

# acks="all" минимизирует потерю сообщений(подтверждения от лидера и ISR)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    acks="all",
    retries=3,
    linger_ms=10,
    value_serializer=lambda v: orjson.dumps(v),
    key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
    request_timeout_ms=10000,
    max_block_ms=10000,
)

# подключаемся
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
client.admin.command("ping")
db = client[MONGO_DB]

# читаем коллекции и шлем в Kafka
for coll, topic in TOPICS.items():
    print(f"[INFO] {coll} -> {topic}")
    sent = 0
    for doc in db[coll].find({}, projection=None):
        doc.pop("_id", None)

        if coll == "customers":
            doc = _sanitize_customer(doc)
        elif coll == "stores":
            doc = _sanitize_store(doc)

        key = str(doc.get(f"{coll[:-1]}_id", ""))  # store_id/product_id/customer_id/purchase_id
        future = producer.send(topic, value=doc, key=key)
        future.get(timeout=10)  # быстрый фэйл, если что-то не так

        sent += 1
        if sent % 50 == 0:
            print(f"[..] {coll}: {sent}")

    producer.flush()
    print(f"[OK] {coll}: sent={sent}")

print("[DONE] all topics sent")


# Скрипт читает документы из 4 коллекций Mongo и шлёт каждую запись целиком в соответствующий топик Kafka (JSON).
# Перед отправкой для чувствительных полей (email/phone) делает: нормализацию (строка/цифры), солевой SHA-256, удаляет исходные значения.
# Ключ сообщения (message key): <entity>_id (store_id / product_id / customer_id / purchase_id).
# Параметры подключения берутся из переменных окружения (MONGO_URI, KAFKA_BROKER, PII_SALT), есть умолчания.