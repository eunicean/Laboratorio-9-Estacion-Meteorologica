import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka import KafkaProducer
from sensor_sim import generar_datos
from encode import encode
import time, json, random



TOPIC = "21231"
BROKER = "iot.redesuvg.cloud:9092"

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: v,   #envia bytes
)


print("Producer con payload de 3 bytes...\n")

contador = 0
try:
    while True:
        data = json.loads(generar_datos())
        encoded = encode(data)

        future = producer.send(TOPIC, key="s1", value=encoded)
        meta = future.get(timeout=10)

        contador += 1
        print(f"[{contador}] enviado: {encoded}  (size={len(encoded)} bytes)")
        print(f"  Original: {data}")

        time.sleep(random.randint(15, 30))

except KeyboardInterrupt:
    print("\nDetenido.")
finally:
    producer.flush()
    producer.close()