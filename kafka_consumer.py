from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt

TOPIC = "21231"
BROKER = "iot.redesuvg.cloud:9092"

temperaturas = []
humedades = []
direcciones_viento = []

direccion_map = {"N": 0, "NE": 45, "E": 90, "SE": 135, 
                 "S": 180, "SO": 225, "O": 270, "NO": 315}

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='weather-consumer-group'
)

print(f"Consumer escuchando en {TOPIC}...")
print("Presiona Ctrl+C para detener\n")

plt.ion()  # Modo interactivo
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(6, 4))

try:
    for mensaje in consumer:
        payload = mensaje.value
        
        # Agregar datos
        temperaturas.append(payload['temperatura'])
        humedades.append(payload['humedad'])
        direcciones_viento.append(direccion_map[payload['direccion_viento']])
        
        print(f"[{len(temperaturas)}] {payload}")
        
        # Actualizar gráficos
        ax1.clear()
        ax2.clear()
        ax3.clear()
        
        ax1.plot(temperaturas, 'r-o')
        ax1.set_ylabel('Temperatura (°C)')
        ax1.set_title(f'Temperatura - Última: {temperaturas[-1]:.2f}°C')
        ax1.grid(True)
        
        ax2.plot(humedades, 'b-s')
        ax2.set_ylabel('Humedad (%)')
        ax2.set_title(f'Humedad - Última: {humedades[-1]}%')
        ax2.grid(True)
        
        ax3.plot(direcciones_viento, 'g-^')
        ax3.set_ylabel('Dirección del viento (°)')
        ax3.set_xlabel('Muestras')
        ax3.grid(True)
        
        plt.tight_layout()
        plt.pause(0.1)
        
except KeyboardInterrupt:
    print(f"\n\nTotal recibidos: {len(temperaturas)}")
finally:
    consumer.close()
    plt.ioff()
    plt.show()