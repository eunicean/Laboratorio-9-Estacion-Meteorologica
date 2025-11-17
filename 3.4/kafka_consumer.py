from kafka import KafkaConsumer
from encode import decode
import matplotlib.pyplot as plt

TOPIC = "21231"
BROKER = "iot.redesuvg.cloud:9092"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    value_deserializer=lambda m: m,  # recibe bytes crudos
    auto_offset_reset='latest',
    group_id='group3bytes'
)

temperaturas = []
humedades = []
direcciones = {"N":0, "NE":45, "E":90, "SE":135,"S":180,"SO":225,"O":270,"NO":315}
dir_valores = []

plt.ion()
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(6,4))

print("Consumer (3 bytes)...\n")

try:
    for msg in consumer:
        payload = decode(msg.value)

        temperaturas.append(payload["temperatura"])
        humedades.append(payload["humedad"])
        dir_valores.append(direcciones[payload["direccion_viento"]])

        print(payload)

        ax1.clear(); ax2.clear(); ax3.clear()
        ax1.plot(temperaturas, 'r-o'); ax1.set_ylabel("Temp")
        ax2.plot(humedades, 'b-s'); ax2.set_ylabel("Humedad")
        ax3.plot(dir_valores, 'g-^'); ax3.set_ylabel("Viento")

        plt.pause(0.1)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    plt.ioff()
    plt.show()