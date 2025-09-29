import os, json, random, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from faker import Faker

# фиксируем seed для random и Faker - на каждом запуске один и тот же набор данных, это удобно для отладки пайплайна и сравнения результатов в CH/Grafana.
random.seed(42)
Faker.seed(42)
fake = Faker("ru_RU")

# куда писать(структура согласована с импортёром в Mongo, один JSON = один файл в 4 подкаталогах)
BASE = Path("scripts/data")
for sub in ["stores", "products", "customers", "purchases"]:
    (BASE / sub).mkdir(parents=True, exist_ok=True)

# Категории: читаемый ярлык(для витрин) + машинный код(проще строить фильтры в MART/дашбордах)
category_map = {
    "🥖🥖 Зерновые и хлебобулочные изделия": "grains",
    "🥩🥩 Мясо, рыба, яйца и бобовые":       "meat_eggs_fish",
    "🥛🥛 Молочные продукты":                "milk",
    "🍏🍏 Фрукты и ягоды":                   "fruits",
    "🥦🥦 Овощи и зелень":                  "vegetables",
}
labels = list(category_map.keys())

store_networks = [("Большая Пикча", 30), ("Маленькая Пикча", 15)]
stores = []

# === 1. Магазины (45) ===
for network, count in store_networks:
    for _ in range(count):
        store_id = f"store-{len(stores)+1:03}"
        city = fake.city()
        store = {
            "store_id": store_id,
            "store_name": f"{network} — Магазин на {fake.street_name()}",
            "store_network": network,
            "store_type_description": (
                "Супермаркет более 200 кв.м."
                if network == "Большая Пикча"
                else "Магазин у дома менее 100 кв.м."
            ) + f" Входит в сеть из {count} магазинов.",
            "type": "offline",
            # пусть магазин «поддерживает» весь ассортимент
            "categories_labels": labels,
            "categories_codes": [category_map[l] for l in labels],
            # PII (email/phone) присутствуют тут, продюсер в Kafka их захэширует и удалит
            "manager": {
                "name": fake.name(),
                "phone": fake.phone_number(),
                "email": fake.email().lower(),
            },
            "location": {
                "country": "Россия",
                "city": city,
                "street": fake.street_name(),
                "house": str(fake.building_number()),
                "postal_code": fake.postcode(),
                "coordinates": {
                    "latitude": float(fake.latitude()),
                    "longitude": float(fake.longitude())
                }
            },
            "opening_hours": {"mon_fri": "09:00-21:00", "sat": "10:00-20:00", "sun": "10:00-18:00"},
            "accepts_online_orders": True,
            "delivery_available": True,
            "warehouse_connected": random.choice([True, False]),
            "last_inventory_date": datetime.now(timezone.utc).date().isoformat(),
        }
        stores.append(store)
        (BASE / "stores" / f"{store_id}.json").write_text(
            json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8"
        )

# === 2. Товары (≥20) ===
products = []
for i in range(20):
    label = random.choice(labels)
    code = category_map[label]
    product = {
        "product_id": f"prd-{1000+i}",
        "product_name": fake.word().capitalize(),
        "group": label,
        "category_code": code,
        "description": fake.sentence(),
        "kbju": {
            "calories": round(random.uniform(50, 300), 1),
            "protein": round(random.uniform(0.5, 20), 1),
            "fat": round(random.uniform(0.1, 15), 1),
            "carbohydrates": round(random.uniform(0.5, 50), 1)
        },
        "price": round(random.uniform(30, 300), 2),
        "unit": "шт",
        "origin_country": "Россия",
        "expiry_days": random.randint(5, 30),
        "is_organic": random.choice([True, False]),
        "barcode": fake.ean(length=13),
        "manufacturer": {
            "name": fake.company(),
            "country": "Россия",
            "website": f"https://{fake.domain_name()}",
            "inn": fake.bothify(text="##########")
        }
    }
    products.append(product)
    (BASE / "products" / f"{product['product_id']}.json").write_text(
        json.dumps(product, ensure_ascii=False, indent=2), encoding="utf-8"
    )

# === 3. Покупатели (>=1 на магазин) ===
customers = []
for store in stores:
    customer_id = f"cus-{1000 + len(customers)}"
    customer = {
        "customer_id": customer_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email().lower(),
        "phone": fake.phone_number(),
        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
        "gender": random.choice(["male", "female"]),
        "registration_date": datetime.now(timezone.utc).isoformat(),
        "is_loyalty_member": True,
        "loyalty_card_number": f"LOYAL-{uuid.uuid4().hex[:10].upper()}",
        "home_store_id": store["store_id"],        # простая ссылка
        "purchase_location": store["location"],    # оставим и полный объект
        "delivery_address": {
            "country": "Россия",
            "city": store["location"]["city"],
            "street": fake.street_name(),
            "house": str(fake.building_number()),
            "apartment": str(random.randint(1, 100)),
            "postal_code": fake.postcode()
        },
        "preferences": {
            "preferred_language": "ru",
            "preferred_payment_method": random.choice(["card", "cash"]),
            "receive_promotions": random.choice([True, False])
        }
    }
    customers.append(customer)
    (BASE / "customers" / f"{customer_id}.json").write_text(
        json.dumps(customer, ensure_ascii=False, indent=2), encoding="utf-8"
    )

# === 4. Покупки (>=200) ===
for i in range(200):
    customer = random.choice(customers)
    store = random.choice(stores)
    items = random.sample(products, k=random.randint(1, 3))

    purchase_items, total = [], 0.0
    for item in items:
        qty = random.randint(1, 5)
        total_price = round(item["price"] * qty, 2)
        total += total_price
        purchase_items.append({
            "product_id": item["product_id"],
            "product_name": item["product_name"],
            "category": item["group"],
            "category_code": item["category_code"],
            "quantity": qty,
            "unit": item["unit"],
            "price_per_unit": item["price"],
            "total_price": total_price,
            "kbju": item["kbju"],
            "manufacturer": item["manufacturer"]
        })

    cash = random.random() < 0.5
    purchase = {
        "purchase_id": f"ord-{i+1:05}",
        "customer": {
            "customer_id": customer["customer_id"],
            "first_name": customer["first_name"],
            "last_name": customer["last_name"]
        },
        "store": {
            "store_id": store["store_id"],
            "store_name": store["store_name"],
            "store_network": store["store_network"],
            "location": store["location"]
        },
        "items": purchase_items,
        "total_amount": round(total, 2),
        "paid_cash": int(cash),
        "paid_card": int(not cash),
        "delivery": int(random.random() < 0.3),
        "delivery_address": customer["delivery_address"],
        "purchase_datetime": (datetime.now(timezone.utc) - timedelta(days=random.randint(0, 90))).isoformat()
    }
    (BASE / "purchases" / f"{purchase['purchase_id']}.json").write_text(
        json.dumps(purchase, ensure_ascii=False, indent=2), encoding="utf-8"
    )

print("✅ Данные успешно сгенерированы в папке scripts/data/")
