from kafka import KafkaProducer
import sys

BROKER = "iot.redesuvg.cloud:9092"

try:
    print(f"Intentando conectar a {BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        request_timeout_ms=5000,
        api_version_auto_timeout_ms=5000
    )
    print("Conexión exitosa!")
    producer.close()
except Exception as e:
    print(f"Error de conexión: {e}")
    sys.exit(1)