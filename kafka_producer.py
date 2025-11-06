from kafka import KafkaProducer
import json
import time
import random
from sensor_sim import generar_datos

TOPIC = "21231"  # numero de carnet
BROKER = "lab9.alumchat.lol:9092"

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    while True:
        data = json.loads(generar_datos())
        producer.send(TOPIC, value=data)
        print(f"Enviado: {data}")
        time.sleep(random.randint(15, 30))
except KeyboardInterrupt:
    print("\nEnvío detenido por el usuario.")
finally:
    producer.close()