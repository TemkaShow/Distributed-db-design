#!/usr/bin/env python3
"""
Простий Producer для енергетичних даних з Kafka
"""
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

TOPIC_NAME = 'power-station-data-hlyviak'

def create_producer():
    print(" Підключаємося до Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all',
            retries=3,
            request_timeout_ms=30000,
            retry_backoff_ms=500
        )
        print(" Підключення до Kafka успішне!")
        return producer
    except Exception as e:
        print(f" Помилка підключення: {e}")
        return None

def generate_power_data():
    stations = [
        {"name": "Київська ТЕС", "type": "thermal", "max_power": 1200},
        {"name": "Дніпровська ГЕС", "type": "hydro", "max_power": 800},
        {"name": "Сонячна ферма", "type": "solar", "max_power": 150},
        {"name": "Вітряна ферма", "type": "wind", "max_power": 200}
    ]
    station = random.choice(stations)
    power_factor = random.uniform(0.75, 0.95)
    current_power = station["max_power"] * power_factor

    return {
        "student": "hlyviak",
        "station_name": station["name"],
        "station_type": station["type"],
        "timestamp": datetime.now().isoformat(),
        "power_output_mw": round(current_power, 2),
        "voltage_kv": round(random.uniform(218, 222), 1),
        "frequency_hz": round(random.uniform(49.9, 50.1), 2),
        "efficiency_percent": round(random.uniform(82, 88), 1),
        "kafka_version": "3.7.1"
    }

def main():
    producer = create_producer()
    if not producer:
        return

    print(" Починаємо відправку даних через Kafka...")
    print(" Натисніть Ctrl+C для зупинки\n")

    message_count = 0
    try:
        while True:
            power_data = generate_power_data()
            future = producer.send(TOPIC_NAME, power_data)
            record_metadata = future.get(timeout=10)
            message_count += 1

            print(f" [{message_count}] {power_data['station_name']} - {power_data['power_output_mw']} МВт")
            print(f"   Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

            time.sleep(3)
    except KeyboardInterrupt:
        print(f"\n Зупинено. Всього відправлено {message_count} повідомлень")
    finally:
        producer.flush()
        producer.close()
        print(" З'єднання з Kafka 3.7.1 закрито")

if __name__ == "__main__":
    main()
