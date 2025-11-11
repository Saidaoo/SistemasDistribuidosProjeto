import zmq
import json
import time
from pathlib import Path

# --- CONFIGURAÇÃO DE ARQUIVOS ---
DATA_DIR = Path("data")
USERS_FILE = DATA_DIR / "users.json"
CHANNELS_FILE = DATA_DIR / "channels.json"
MESSAGES_FILE = DATA_DIR / "messages.json"

DATA_DIR.mkdir(exist_ok=True)
for f in [USERS_FILE, CHANNELS_FILE, MESSAGES_FILE]:
    if not f.exists():
        f.write_text(json.dumps([]))  # inicializa como lista

# --- FUNÇÕES AUXILIARES ---
def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

def save_json(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=2)

# --- SERVIÇOS ---
def handle_login(data):
    users = load_json(USERS_FILE)
    if data["user"] in users:
        return {"status": "erro", "description": "Usuário já logado"}
    users.append(data["user"])
    save_json(USERS_FILE, users)
    return {"status": "sucesso"}

# --- MAIN ---
def main():
    context = zmq.Context()

    # REQ-REP para comunicação com clientes
    rep_socket = context.socket(zmq.REP)
    rep_socket.bind("tcp://*:5555")

    # PUB para publicar mensagens no Proxy
    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect("tcp://proxy:5557")  # proxy Pub/Sub

    print("🟢 Servidor iniciado (REQ/REP na 5555, PUB → proxy:5557)...", flush=True)

    while True:
        message = rep_socket.recv_json()
        print(f"📩 Mensagem recebida: {message}", flush=True)

        service = message.get("service")
        data = message.get("data", {})
        timestamp = time.time()

        if service == "login":
            users = load_json(USERS_FILE)
            user = data.get("user")
            if user in users:
                reply = {"service": "login", "data": {"status": "erro", "description": "Usuário já logado.", "timestamp": timestamp}}
            else:
                users.append(user)
                save_json(USERS_FILE, users)
                reply = {"service": "login", "data": {"status": "sucesso", "timestamp": timestamp}}

        elif service == "users":
            users = load_json(USERS_FILE)
            reply = {"service": "users", "data": {"users": users, "timestamp": timestamp}}

        elif service == "channel":
            channels = load_json(CHANNELS_FILE)
            channel = data.get("channel")
            if channel in channels:
                reply = {"service": "channel", "data": {"status": "erro", "description": "Canal já existe.", "timestamp": timestamp}}
            else:
                channels.append(channel)
                save_json(CHANNELS_FILE, channels)
                reply = {"service": "channel", "data": {"status": "sucesso", "timestamp": timestamp}}

        elif service == "channels":
            channels = load_json(CHANNELS_FILE)
            reply = {"service": "channels", "data": {"channels": channels, "timestamp": timestamp}}

        elif service == "publish":  # publicação em canal
            channels = load_json(CHANNELS_FILE)
            messages = load_json(MESSAGES_FILE)
            channel = data.get("channel")
            user = data.get("user")
            msg_text = data.get("message")

            if channel not in channels:
                reply = {"service": "publish", "data": {"status": "erro", "message": "Canal não existe.", "timestamp": timestamp}}
            else:
                msg_data = {"type": "canal", "channel": channel, "user": user, "message": msg_text, "timestamp": timestamp}
                messages.append(msg_data)
                save_json(MESSAGES_FILE, messages)

                pub_socket.send_string(f"{channel} {user}: {msg_text}")  # publicando no tópico do canal
                reply = {"service": "publish", "data": {"status": "OK", "timestamp": timestamp}}

        elif service == "message":  # mensagem direta entre usuários
            users = load_json(USERS_FILE)
            messages = load_json(MESSAGES_FILE)
            src = data.get("src")
            dst = data.get("dst")
            msg_text = data.get("message")

            if dst not in users:
                reply = {"service": "message", "data": {"status": "erro", "message": "Usuário de destino não existe.", "timestamp": timestamp}}
            else:
                msg_data = {"type": "privada", "from": src, "to": dst, "message": msg_text, "timestamp": timestamp}
                messages.append(msg_data)
                save_json(MESSAGES_FILE, messages)

                pub_socket.send_string(f"{dst} {src}: {msg_text}")  # publicando no tópico do usuário de destino
                reply = {"service": "message", "data": {"status": "OK", "timestamp": timestamp}}

        else:
            reply = {"service": "erro", "data": {"message": "Serviço desconhecido.", "timestamp": timestamp}}

        rep_socket.send_json(reply)

    rep_socket.close()
    pub_socket.close()
    context.term()

if __name__ == "__main__":
    main()
