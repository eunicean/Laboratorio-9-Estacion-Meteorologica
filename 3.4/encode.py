import json

direccion_bits = {
    "N": 0, "NE": 1, "E": 2, "SE": 3,
    "S": 4, "SO": 5, "O": 6, "NO": 7
}

bits_direccion_inv = {v: k for k, v in direccion_bits.items()}


def encode(data):
    """
    JSON → 3 bytes (24 bits)
    """
    temp = int(data["temperatura"] * 10)   # escala temperatura
    hum = int(data["humedad"])             # 0–100
    wind = direccion_bits[data["direccion_viento"]]  # 0–7

    # Empaquetar en 24 bits
    packed = (temp << 10) | (hum << 3) | wind
    return packed.to_bytes(3, byteorder="big")


def decode(payload_bytes):
    value = int.from_bytes(payload_bytes, "big")

    wind = value & 0b111
    hum = (value >> 3) & 0b1111111
    temp = (value >> 10)

    return {
        "temperatura": temp / 10.0,
        "humedad": hum,
        "direccion_viento": bits_direccion_inv[wind]
    }