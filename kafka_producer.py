from kafka import KafkaProducer
import json
import time
import random
from sensor_sim import generar_datos

TOPIC = "21231"  # numero de carnet
BROKER = "iot.redesuvg.cloud:9092"

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    request_timeout_ms=5000,  # Timeout de 5 segundos
    retries=3,  
    acks='all' 
)

print(f"Productor conectado al broker: {BROKER}")
print(f"Enviando datos al topic: {TOPIC}")

try:
    contador = 0
    while True:
        data = json.loads(generar_datos())
        
        # Envía el mensaje y obtiene el futuro
        future = producer.send(TOPIC, value=data, key=b'sensor1')
        
        # Espera confirmación, esto lo pusimos porque la conexion no funcionaba la primera vez jeje
        record_metadata = future.get(timeout=10)
        
        contador += 1
        print(f"[{contador}] Enviado a partición {record_metadata.partition}, offset {record_metadata.offset}")
        print(f"    Datos: {data}")
        
        tiempo_espera = random.randint(15, 30)
        print(f"    Esperando {tiempo_espera} segundos...\n")
        time.sleep(tiempo_espera)
        
except KeyboardInterrupt:
    print("\n\n")
    print("Envío detenido por el usuario.")
    print(f"Total de mensajes enviados: {contador}")
except Exception as e:
    print(f"\n Error: {e}")
finally:
    producer.flush()
    producer.close()
    print("Productor cerrado correctamente.")