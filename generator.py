import json
import time
import random
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable, KafkaConnectionError

# Конфигурация Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC = "user_events"

# API валют
CURRENCY_API = "https://open.er-api.com/v6/latest/USD"

# API погоды
WEATHER_API = "https://api.openweathermap.org/data/2.5/weather"
WEATHER_API_KEY = "1c65bcbc810738b26c1cbf7ead663376"

# API для поиска городов по координатам
POSITIONSTACK_KEY = "3dfdbeefb41c4742c2a307e68219a82e"
POSITIONSTACK_URL = "https://api.positionstack.com/v1/reverse"


def get_currency_rates():
    # Генерирем сумму корзины и переводим в валюту
    try:
        r = requests.get(CURRENCY_API, timeout=5)
        data = r.json()
        rates = data.get("rates", {})
        usd_price = round(random.uniform(1, 1000), 2)

        eur_price = round(usd_price * rates.get("EUR", 0), 2) if rates.get("EUR") else None
        rub_price = round(usd_price * rates.get("RUB", 0), 2) if rates.get("RUB") else None

        return {
            "USD": usd_price,
            "EUR": eur_price,
            "RUB": rub_price
        }
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Currency API request failed: {e}")
        return {"USD": None, "EUR": None, "RUB": None}
    except Exception as e:
        print(f"[ERROR] Currency API: {e}")
        return {"USD": None, "EUR": None, "RUB": None}


def get_random_city(max_retries=5):
    # Возвращает случайный город. Проверка если попали в океан - повторяем попытку
    for attempt in range(max_retries):
        lat = random.uniform(-90, 90)
        lon = random.uniform(-180, 180)

        params = {
            "access_key": POSITIONSTACK_KEY,
            "query": f"{lat},{lon}",
            "limit": 1
        }

        try:
            r = requests.get(POSITIONSTACK_URL, params=params, timeout=5)
            r.raise_for_status()
            data = r.json().get("data")
            if data and len(data) > 0:
                result = data[0]
                city = result.get("name") or result.get("label", "Unknown")
                country = result.get("country")

                if country and country != "Unknown":  # нормальная страна
                    return {
                        "city": city,
                        "country": country,
                        "lat": lat,
                        "lon": lon
                    }
                else:
                    print(f"[INFO] Попали в океан/неизвестное место (попытка {attempt+1}), пробуем снова...")
        except Exception as e:
            print(f"[WARNING] Positionstack API error (попытка {attempt+1}): {e}")

    # если все попытки неудачные
    return {
        "city": "Unknown",
        "country": None,
        "lat": lat,
        "lon": lon
    }


def get_weather(lat, lon):
    if not lat or not lon:
        return {"temp": None, "weather": None}

    try:
        params = {"lat": lat, "lon": lon, "appid": WEATHER_API_KEY, "units": "metric"}
        r = requests.get(WEATHER_API, params=params, timeout=5)

        if r.status_code == 200:
            data = r.json()
            return {
                "temp": data["main"]["temp"],
                "weather": data["weather"][0]["description"]
            }
        else:
            print(f"[ERROR] Weather API status: {r.status_code}")
            return {"temp": None, "weather": None}

    except Exception as e:
        print(f"[ERROR] Weather API: {e}")
        return {"temp": None, "weather": None}


def generate_event():
    try:
        user_id = random.randint(1, 1000)
        event_type = random.choice(["session_start", "view_product", "add_to_cart", "purchase"])
        city = get_random_city()

        currency = get_currency_rates()
        weather = get_weather(city["lat"], city["lon"])

        event = {
            "user_id": user_id,
            "event_type": event_type,
            "city": city["city"],
            "country": city["country"],
            "lat": city["lat"],
            "lon": city["lon"],
            "cart sum": currency,
            "weather": weather,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        return event
    except Exception as e:
        print(f"[ERROR] Failed to generate event: {e}")
        return None


def is_kafka_available():
    # Проверка Kafka
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            request_timeout_ms=5000,
            api_version=(2, 0, 2)
        )
        admin.close()
        return True
    except (NoBrokersAvailable, KafkaConnectionError) as e:
        print(f"[DEBUG] Kafka connection error: {e}")
        return False
    except Exception as e:
        print(f"[DEBUG] Kafka check error: {e}")
        return False


def main():
    print(f"[INFO] Checking Kafka connection to {KAFKA_BROKER}...")

    if is_kafka_available():
        try:
            # Создаем producer
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(2, 0, 2),
                request_timeout_ms=30000,
                retry_backoff_ms=500,
                security_protocol="PLAINTEXT"
            )

            print(f"[SUCCESS] Connected to Kafka broker at {KAFKA_BROKER}")
            print("[INFO] Generator started. Sending events every 60 seconds...")

            while True:
                try:
                    event = generate_event()
                    if event:  # Проверяем, что event не None
                        producer.send(TOPIC, value=event)
                        producer.flush()
                        print(f"[SENT] {event}")
                    else:
                        print("[WARNING] Skipping None event")
                    time.sleep(60)
                except Exception as e:
                    print(f"[ERROR] Failed to send event: {e}")
                    time.sleep(2)

        except Exception as e:
            print(f"[FATAL ERROR] Failed to create producer: {e}")
    else:
        print(f"[ERROR] Kafka broker not available at {KAFKA_BROKER}")


if __name__ == "__main__":
    main()