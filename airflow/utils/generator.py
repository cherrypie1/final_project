import json
import random
import socket
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import requests
from faker import Faker
from kafka import KafkaProducer

faker = Faker()

# Kafka
KAFKA_BROKER = "kafka:29092"
TOPIC = "user_events"

# API валют
CURRENCY_API = "https://open.er-api.com/v6/latest/USD"

# API погоды
WEATHER_API = "https://api.openweathermap.org/data/2.5/weather"
WEATHER_API_KEY = "1c65bcbc810738b26c1cbf7ead663376"

# API для поиска городов по координатам
POSITIONSTACK_KEY = "7cb0abf85dc261e923fd4900bf0b3a5a"
POSITIONSTACK_URL = "https://api.positionstack.com/v1/reverse"

PRODUCT_CATEGORIES = [
    "electronics", "clothing", "books", "home", "sports",
    "beauty", "toys", "food", "jewelry", "automotive"
]

PRODUCTS = [
    {"id": 1, "name": "Redmond Фен", "category": "electronics", "price": 99.0},
    {"id": 2, "name": "Apple Планшет", "category": "electronics", "price": 1200.99},
    {"id": 3, "name": "Nike Кросовки", "category": "clothing", "price": 260.90},
    {"id": 4, "name": "Samsung TV", "category": "electronics", "price": 1999.99},
    {"id": 5, "name": "Levi's Jeans", "category": "clothing", "price": 89.60},
    {"id": 6, "name": "Мастер и Маргарита книга", "category": "books", "price": 10.12},
    {"id": 7, "name": "Плед", "category": "home", "price": 79.99},
    {"id": 8, "name": "Мяч волейбольный", "category": "sports", "price": 32.40},
    {"id": 9, "name": "Помада", "category": "beauty", "price": 5.96},
    {"id": 10, "name": "Дженга настольная игра", "category": "toys", "price": 43.59},
    {"id": 11, "name": "Чипсы", "category": "food", "price": 3.65},
    {"id": 12, "name": "Подвеска с искусственным камнем", "category": "jewelry", "price": 12.40},
    {"id": 13, "name": "Ваза", "category": "home", "price": 18.38},
    {"id": 14, "name": "Детейлинг средство", "category": "automotive", "price": 73.13}
]

# Кампании для маркетинговых событий
CAMPAIGNS = [
    {"id": "camp1", "name": "Summer Sale", "channel": "email", "base_cost": 500},
    {"id": "camp2", "name": "Black Friday", "channel": "social", "base_cost": 800},
    {"id": "camp3", "name": "New Year", "channel": "search", "base_cost": 300},
    {"id": "camp4", "name": "Spring Collection", "channel": "email", "base_cost": 400},
    {"id": "camp5", "name": "Winter Discount", "channel": "social", "base_cost": 600}
]


def generate_campaign_event(user_id: int):
    # Генерирует событие взаимодействия с маркетинговой кампанией
    campaign = random.choice(CAMPAIGNS)
    cost = round(campaign["base_cost"] * random.uniform(0.8, 1.2), 2)
    return {
        "type": "campaign_event",
        "user_id": user_id,
        "campaign_id": campaign["id"],
        "campaign_name": campaign["name"],
        "channel": campaign["channel"],
        "action": random.choice(["view", "click", "conversion"]),
        "cost": cost,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


def generate_order_status_event(order_id: str):
    # Генерирует событие смены статуса заказа

    statuses = [
        ("pending", 0.5),  # 50% — заказ создан, ожидает оплаты/обработки
        ("success", 0.3),  # 30% — успешно оплачен и обработан
        ("failed", 0.15),  # 15% — оплата не прошла
        ("cancelled", 0.05)  # 5% — отменён пользователем
    ]

    # Выбираем статус на основе весов
    status = random.choices(
        population=[s[0] for s in statuses],
        weights=[s[1] for s in statuses]
    )[0]

    return {
        "type": "order_status",
        "order_id": order_id,
        "status": status,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


def is_kafka_available(broker: str = KAFKA_BROKER) -> bool:
    # Проверяет доступность Kafka брокера через socket соединение"
    try:
        host, port = broker.split(":")
        with socket.create_connection((host, int(port)), timeout=5):
            return True
    except Exception as e:
        print(f"[DEBUG] Kafka connection error: {e}")
        return False


def get_currency_rates():
    # Получает актуальные курсы валют от API или генерирует случайные значения при ошибке
    try:
        r = requests.get(CURRENCY_API, timeout=5)
        data = r.json()
        rates = data.get("rates", {})
        usd_price = round(random.uniform(1, 1000), 2)
        eur_price = round(usd_price * rates.get("EUR", 0), 2) if "EUR" in rates else None
        rub_price = round(usd_price * rates.get("RUB", 0), 2) if "RUB" in rates else None
        return usd_price, eur_price, rub_price
    except Exception:
        return round(random.uniform(1, 1000), 2), None, None


def get_random_city(lat: float, lon: float, max_retries: int = 5) -> Dict[str, Optional[float]]:
    # Генерирует координаты и город по ним через PositionStack API с повторными попытками
    for attempt in range(max_retries):
        params = {"access_key": POSITIONSTACK_KEY, "query": f"{lat},{lon}", "limit": 1}
        try:
            r = requests.get(POSITIONSTACK_URL, params=params, timeout=5)
            data = r.json().get("data")
            if data:
                result = data[0]
                city = result.get("name")
                country = result.get("country")

                # Проверяем, что это не океан/море
                if city and not any(word in city for word in ["Ocean", "Sea", "Gulf", "Bay"]):
                    return {
                        "city": city,
                        "country": country,
                        "lat": lat,
                        "lon": lon
                    }

        except Exception:
            pass
        # пробуем новые случайные
        lat = random.uniform(-90, 90)
        lon = random.uniform(-180, 180)
    return {"city": "Unknown", "country": None, "lat": lat, "lon": lon}


def get_weather(lat: float, lon: float) -> Dict[str, Optional[float]]:
    # Получает данные о погоде по координатам через OpenWeatherMap API
    try:
        params = {"lat": lat, "lon": lon, "appid": WEATHER_API_KEY, "units": "metric"}
        r = requests.get(WEATHER_API, params=params, timeout=5)
        if r.status_code == 200:
            data = r.json()
            return {"temp": data["main"]["temp"], "weather": data["weather"][0]["description"]}
        return {"temp": None, "weather": None}
    except Exception:
        return {"temp": None, "weather": None}


def generate_user(user_id: int):
    # Генерирует набор событий профиля пользователя: профиль, устройство и местоположение
    lat = float(faker.latitude())
    lon = float(faker.longitude())
    geo = get_random_city(lat, lon)
    weather = get_weather(lat, lon)

    profile = {
        "type": "user_profile",
        "user_id": user_id,
        "name": faker.name(),
        "email": faker.email(),
        "birth_date": faker.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        "gender": random.choice(["male", "female", "other"]),
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    device = {
        "type": "user_device",
        "user_id": user_id,
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "os": random.choice(["iOS", "Android", "Windows", "Linux", "MacOS"]),
        "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    location = {
        "type": "user_location",
        "user_id": user_id,
        "city": geo.get("city"),
        "country": geo.get("country"),
        "lat": lat,
        "lon": lon,
        "weather_temp": weather.get("temp"),
        "weather_desc": weather.get("weather"),
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    return [profile, device, location]


def generate_order(order_id: str, user_id: int, product: Dict, quantity: int):
    # Генерирует события заказа и позиции заказа с расчетом общей стоимости
    order_ts = datetime.now(timezone.utc).isoformat()
    total_usd = product["price"] * quantity

    order_msg = {
        "type": "order",
        "order_id": order_id,
        "user_id": user_id,
        "order_ts": order_ts,
        "total_usd": total_usd,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    order_item_msg = {
        "type": "order_item",
        "order_id": order_id,
        "product_id": product["id"],
        "quantity": quantity,
        "price_usd": product["price"],
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    return [order_msg, order_item_msg]


def generate_event(user_id: int) -> Dict[str, Any]:
    # Генерирует одно событие пользовательской активности с различными типами действий
    session_id = str(uuid.uuid4())
    event_type = random.choice(["session_start", "session_end", "view_page", "view_product",
                                "add_to_cart", "remove_from_cart", "purchase", "payment_success",
                                "search", "filter", "login", "logout", "signup"])
    lat = float(faker.latitude())
    lon = float(faker.longitude())
    geo = get_random_city(lat, lon)
    weather = get_weather(lat, lon)
    usd, eur, rub = get_currency_rates()

    product = random.choice(PRODUCTS) if event_type in ("view_product", "add_to_cart", "purchase") else None
    order_id = str(uuid.uuid4()) if event_type in ("add_to_cart", "purchase") else None
    quantity = random.randint(1, 5) if event_type in ("add_to_cart", "purchase") else None

    event = {
        "type": "event",
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "product_id": product["id"] if product else None,
        "product_name": product["name"] if product else None,
        "product_category": product["category"] if product else None,
        "order_id": order_id,
        "quantity": quantity,
        "city": geo.get("city"),
        "country": geo.get("country"),
        "lat": lat,
        "lon": lon,
        "cost_usd": usd,
        "cost_eur": eur,
        "cost_rub": rub,
        "weather_temp": weather.get("temp"),
        "weather_desc": weather.get("weather"),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return event


def produce_batch(send_count_users: int = 1, events_per_user: int = 1, broker: str = KAFKA_BROKER) -> int:
    if not is_kafka_available(broker):
        raise RuntimeError(f"Kafka broker {broker} недоступен")

    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10
    )

    sent = 0
    for _ in range(send_count_users):
        user_id = random.randint(1, 5000)
        for msg in generate_user(user_id):
            producer.send(TOPIC, value=msg)
            sent += 1

        # Генерация кампаний (20% вероятность)
        if random.random() < 0.2:
            campaign_event = generate_campaign_event(user_id)
            producer.send(TOPIC, value=campaign_event)
            sent += 1

        for _ in range(events_per_user):
            ev = generate_event(user_id)
            producer.send(TOPIC, value=ev)
            sent += 1

            if ev["event_type"] == "purchase" and ev["order_id"]:
                product = next((p for p in PRODUCTS if p["id"] == ev["product_id"]), None)
                if product:
                    order_msgs = generate_order(ev["order_id"], user_id, product, ev["quantity"])
                    for order_msg in order_msgs:
                        producer.send(TOPIC, value=order_msg)
                        sent += 1

                    if random.random() < 0.8:  # 80% заказов получают статус (не все сразу)
                        status_event = generate_order_status_event(ev["order_id"])
                        producer.send(TOPIC, value=status_event)
                        sent += 1

    producer.flush()
    producer.close()
    return sent