# test_referencia.py
import zmq
import json
import time

SERVER_ADDRESS = "tcp://referencia:6000"
TIMEOUT = 3000  # ms

print(f"🔌 Conectando ao servidor de referência em {SERVER_ADDRESS}...")

context = zmq.Context()
sock = context.socket(zmq.REQ)
sock.connect(SERVER_ADDRESS)

# Configura socket para timeout
sock.RCVTIMEO = TIMEOUT
sock.SNDTIMEO = TIMEOUT

def send_request(service_name, data):
    try:
        print(f"📤 Enviando {service_name}...")
        sock.send_json({
            "service": service_name,
            "data": data
        })
        reply = sock.recv_json()
        print(f"✅ Resposta do {service_name}: {json.dumps(reply, indent=2)}\n")
    except zmq.error.Again:
        print(f"❌ Timeout: sem resposta do servidor de referência para {service_name}\n")
    except Exception as e:
        print(f"❌ Erro enviando {service_name}: {e}\n")

# --- Teste 1: rank ---
send_request("rank", {"user": "ServidorTeste", "clock": 0})

# --- Teste 2: list ---
send_request("list", {"clock": 0})

# --- Teste 3: heartbeat ---
send_request("heartbeat", {"user": "ServidorTeste", "clock": 0})
