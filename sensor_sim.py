import random
import json
import time
import sys

def generar_datos():
    temperatura = round(random.gauss(55, 15), 2)
    temperatura = max(0, min(temperatura, 110))
    humedad = int(random.gauss(50, 20))
    humedad = max(0, min(humedad, 100))
    direccion_viento = random.choice(["N", "NO", "O", "SO", "S", "SE", "E", "NE"])
    
    data = {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": direccion_viento
    }
    return json.dumps(data)

if __name__ == "__main__":
    try:
        while True:
            print(generar_datos())
            time.sleep(random.randint(15, 30))
    except KeyboardInterrupt:
        print("\nAdios!")
        sys.exit(0) # Exit the program cleanly