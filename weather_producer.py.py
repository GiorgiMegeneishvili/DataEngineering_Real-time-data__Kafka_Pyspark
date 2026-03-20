import json
import time
import requests
import sys
from datetime import datetime
from kafka import KafkaProducer

# კონფიგი
API_KEY = "52f5b9b2f30ac7ee5020df9a740aa2da"
#CITIES = ["Tbilisi", "Baku"]
CITIES = [
    'Tbilisi', 'Baku', 'Yerevan', 'Ankara', 'Athens',
    'Sofia', 'Bucharest', 'Budapest', 'Vienna', 'Berlin',
    'Paris', 'Madrid', 'Rome', 'Warsaw', 'Prague',
    'Bratislava', 'Zagreb', 'Ljubljana', 'Belgrade', 'Skopje',
    'Podgorica', 'Sarajevo', 'Oslo', 'Helsinki', 'Copenhagen',
    'Tallinn', 'Riga', 'Vilnius', 'Luxembourg', 'Monaco',
    'Chisinau', 'Valletta', 'Andorra la Vella', 'Vaduz', 'San Marino',
    'Bern', 'Kyiv', 'London', 'Dublin', 'Reykjavik'
]
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "weather-stream"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

print("Weather Producer started... (Press Ctrl+C to stop)")

while True:
    for city in CITIES:
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            event = {
                "city": city,
                "temperature": float(data["main"]["temp"]),
                "humidity": int(data["main"]["humidity"]),
                "weather": data["weather"][0]["description"],
                "event_time": datetime.utcnow().isoformat(),
                "pressure": int(data["main"]["pressure"]),
                "wind_speed": float(data.get("wind", {}).get("speed", 0.0)),
                "source": "openweathermap"
            }

            producer.send(KAFKA_TOPIC, value=event)
            print(f"→ Sent to Kafka: {city} | {event['temperature']}°C | {event['weather']}")

        except Exception as e:
            print(f"Error {city}: {e}")

    time.sleep(60)  # 1 minute — safe for rate limiting
