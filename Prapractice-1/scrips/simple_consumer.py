#!/usr/bin/env python3
"""
Простий Consumer для енергетичних даних з Kafka
"""
import json
import time
from kafka import KafkaConsumer

TOPIC_NAME = 'power-station-data-hlyviak'

def create_consumer():
    print(" Підключаємося до Kafka як Consumer...")
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            group_id=f'energy-monitor-hlyviak-{int(time.time())}',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(" Consumer для Kafka готовий до роботи!")
        return consumer
    except Exception as e:
        print(f" Помилка підключення: {e}")
        return None

def analyze_power_data(data):
    try:
        power = data.get('power_output_mw', 0)
        voltage = data.get('voltage_kv', 0)
        frequency = data.get('frequency_hz', 0)
        efficiency = data.get('efficiency_percent', 0)

        power_status = "🟢 ВИСОКА ПОТУЖНІСТЬ" if power > 500 else "🟢 НОРМАЛЬНА ПОТУЖНІСТЬ"
        voltage_status = "🟢 НОРМА" if 219 <= voltage <= 221 else "🟠 ВІДХИЛЕННЯ"
        frequency_status = "🟢 НОРМА" if 49.9 <= frequency <= 50.1 else "🔴 КРИТИЧНО"
        efficiency_status = "🟢 ВІДМІННО" if efficiency >= 85 else "🟡 ДОБРЕ"

        return {
            'power_status': power_status,
            'voltage_status': voltage_status,
            'frequency_status': frequency_status,
            'efficiency_status': efficiency_status
        }
    except Exception as e:
        print(f"❌ Помилка аналізу: {e}")
        return None

def process_power_data(data):
    try:
        analysis = analyze_power_data(data)
        print("\n === МОНІТОРИНГ ЕНЕРГОСИСТЕМИ ===")
        print(f" Студент: {data.get('student', 'hlyviak')}")
        print(f" Станція: {data.get('station_name')} ({data.get('station_type', '').upper()})")
        print(f" Час: {data.get('timestamp')}")
        print(f" Потужність: {data.get('power_output_mw')} МВт - {analysis['power_status']}")
        print(f" Напруга: {data.get('voltage_kv')} кВ - {analysis['voltage_status']}")
        print(f" Частота: {data.get('frequency_hz')} Гц - {analysis['frequency_status']}")
        print(f" ККД: {data.get('efficiency_percent')}% - {analysis['efficiency_status']}")
        print("-" * 50)
    except Exception as e:
        print(f" Помилка обробки: {e}")

def main():
    consumer = create_consumer()
    if not consumer:
        return

    print(" Очікуємо дані від електростанцій...")
    print(" Натисніть Ctrl+C для зупинки\n")

    message_count = 0
    try:
        for message in consumer:
            message_count += 1
            print(f"\n Отримано повідомлення #{message_count}")
            print(f" Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
            process_power_data(message.value)
    except KeyboardInterrupt:
        print(f"\n Consumer зупинено. Оброблено {message_count} повідомлень")
    finally:
        consumer.close()
        print(" З'єднання закрито")

if __name__ == "__main__":
    main()
