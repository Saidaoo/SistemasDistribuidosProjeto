import zmq
import json
import os
import time
import random

DATA_DIR = "../reqrep/data"
USERS_FILE = os.path.join(DATA_DIR, "users.json")
CHANNELS_FILE = os.path.join(DATA_DIR, "channels.json")

def load_json(file):
    if os.path.exists(file):
        with open(file, "r") as f:
            return json.load(f)
    return []

users = load_json(USERS_FILE)
channels = load_json(CHANNELS_FILE)

context = zmq.Context()
pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5555")  # conecta no XSUB do proxy

print(f"Publicador ativo. Usuários={users}, Canais={channels}")

while True:
    # Escolhe aleatoriamente usuário ou canal
    if random.random() < 0.5 and channels:
        topic = random.choice(channels)
        message = f"{topic} | Mensagem do canal {topic} às {time.time()}"
    elif users:
        topic = random.choice(users)
        message = f"{topic} | Mensagem privada para {topic} às {time.time()}"
    else:
        time.sleep(1)
        continue

    print(f"Enviando: {message}", flush=True)
    pub.send_string(message)
    time.sleep(2)

pub.close()
context.close()
