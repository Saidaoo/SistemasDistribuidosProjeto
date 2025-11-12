# reqrep/servidor.py
import zmq
import json
import time
from datetime import datetime
import os

class ChatServer:
    def __init__(self):
        self.context = zmq.Context()
        
        # Socket REQ-REP para comunicação com clientes
        self.rep_socket = self.context.socket(zmq.REP)
        self.rep_socket.bind("tcp://*:5555")
        
        # Socket PUB para publicar mensagens (conecta no proxy)
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect("tcp://proxy:5557")
        time.sleep(2)
        
        # Inicializa arquivos de persistência
        self.init_storage()
        
        # Dados em memória
        self.users = self.load_users()
        self.channels = self.load_channels()
        self.user_logins = self.load_user_logins()
        
        print("✅ Servidor inicializado com sucesso!")
        print(f"👥 Usuários: {len(self.users)}")
        print(f"📋 Canais: {self.channels}")

    def init_storage(self):
        """Inicializa arquivos JSON para persistência"""
        os.makedirs("/app/data", exist_ok=True)
        
        files = ['users.json', 'channels.json', 'user_logins.json', 'messages.json']
        for file in files:
            filepath = f"/app/data/{file}"
            if not os.path.exists(filepath):
                with open(filepath, 'w') as f:
                    json.dump([], f)

    # === MÉTODOS DE CARREGAMENTO ===
    def load_users(self):
        try:
            with open('/app/data/users.json', 'r') as f:
                return set(json.load(f))
        except:
            return set()

    def load_channels(self):
        try:
            with open('/app/data/channels.json', 'r') as f:
                channels = json.load(f)
                if not channels:
                    default_channels = ["geral", "musica", "filmes"]
                    self.save_channels(default_channels)
                    return default_channels
                return channels
        except:
            default_channels = ["geral", "musica", "filmes"]
            self.save_channels(default_channels)
            return default_channels

    def load_user_logins(self):
        try:
            with open('/app/data/user_logins.json', 'r') as f:
                return json.load(f)
        except:
            return []

    # === MÉTODOS DE PERSISTÊNCIA ===
    def save_users(self):
        with open('/app/data/users.json', 'w') as f:
            json.dump(list(self.users), f)

    def save_channels(self, channels):
        with open('/app/data/channels.json', 'w') as f:
            json.dump(channels, f)

    def save_user_logins(self):
        with open('/app/data/user_logins.json', 'w') as f:
            json.dump(self.user_logins, f, indent=2)

    def save_message(self, message_data):
        try:
            with open('/app/data/messages.json', 'r') as f:
                messages = json.load(f)
        except:
            messages = []
        
        messages.append(message_data)
        
        with open('/app/data/messages.json', 'w') as f:
            json.dump(messages, f, indent=2)

    # === SERVIÇOS REQ-REP (PARTE 1) ===
    def handle_login(self, data):
        """Serviço de login do usuário"""
        user = data.get("user")
        timestamp = data.get("timestamp", datetime.now().isoformat())
        
        if not user:
            return {
                "status": "erro",
                "timestamp": datetime.now().isoformat(),
                "description": "Nome de usuário é obrigatório"
            }
        
        # Registra usuário se não existir
        if user not in self.users:
            self.users.add(user)
            self.save_users()
            print(f"👤 Novo usuário registrado: {user}")
        
        # Registra login
        login_record = {
            "user": user,
            "timestamp": timestamp,
            "type": "login"
        }
        self.user_logins.append(login_record)
        self.save_user_logins()
        
        print(f"🔐 Login realizado: {user}")
        
        return {
            "status": "sucesso",
            "timestamp": datetime.now().isoformat()
        }

    def handle_users(self, data):
        """Serviço de listagem de usuários"""
        timestamp = data.get("timestamp", datetime.now().isoformat())
        
        return {
            "timestamp": timestamp,
            "users": list(self.users)
        }

    def handle_channel_create(self, data):
        """Serviço de criação de canal"""
        channel = data.get("channel")
        timestamp = data.get("timestamp", datetime.now().isoformat())
        
        if not channel:
            return {
                "status": "erro",
                "timestamp": timestamp,
                "description": "Nome do canal é obrigatório"
            }
        
        if channel in self.channels:
            return {
                "status": "erro", 
                "timestamp": timestamp,
                "description": f"Canal '{channel}' já existe"
            }
        
        # Adiciona canal
        self.channels.append(channel)
        self.save_channels(self.channels)
        
        print(f"📺 Novo canal criado: {channel}")
        
        return {
            "status": "sucesso",
            "timestamp": timestamp
        }

    def handle_channels(self, data):
        """Serviço de listagem de canais"""
        timestamp = data.get("timestamp", datetime.now().isoformat())
        
        return {
            "timestamp": timestamp,
            "channels": self.channels
        }

    # === SERVIÇOS PUB-SUB (PARTE 2) ===
    def handle_publish(self, data):
        """Publicação em canal"""
        user = data.get("user")
        channel = data.get("channel")
        message = data.get("message")
        timestamp = data.get("timestamp", datetime.now().isoformat())
        
        print(f"📤 Publicação: {user} -> {channel}")
        
        if channel not in self.channels:
            return {
                "status": "erro",
                "message": f"Canal '{channel}' não existe",
                "timestamp": datetime.now().isoformat()
            }
        
        # Publica no canal
        pub_message = {
            "type": "channel_message",
            "channel": channel,
            "user": user,
            "message": message,
            "timestamp": timestamp
        }
        
        self.pub_socket.send_string(f"{channel} {json.dumps(pub_message)}")
        
        # Salva mensagem
        self.save_message({
            "type": "channel",
            "user": user,
            "channel": channel,
            "message": message,
            "timestamp": timestamp
        })
        
        return {
            "status": "OK",
            "message": "Mensagem publicada com sucesso",
            "timestamp": datetime.now().isoformat()
        }

    def handle_direct_message(self, data):
        """Mensagem direta entre usuários"""
        src_user = data.get("src")
        dst_user = data.get("dst")
        message = data.get("message")
        timestamp = data.get("timestamp", datetime.now().isoformat())
        
        print(f"📩 Mensagem direta: {src_user} -> {dst_user}")
        
        if dst_user not in self.users:
            return {
                "status": "erro",
                "message": f"Usuário '{dst_user}' não encontrado",
                "timestamp": datetime.now().isoformat()
            }
        
        # Publica no tópico do usuário destino
        pub_message = {
            "type": "direct_message",
            "from": src_user,
            "to": dst_user,
            "message": message,
            "timestamp": timestamp
        }
        
        self.pub_socket.send_string(f"{dst_user} {json.dumps(pub_message)}")
        
        # Salva mensagem
        self.save_message({
            "type": "direct",
            "from": src_user,
            "to": dst_user,
            "message": message,
            "timestamp": timestamp
        })
        
        return {
            "status": "OK",
            "message": "Mensagem enviada com sucesso",
            "timestamp": datetime.now().isoformat()
        }

    def run(self):
        print("🚀 Servidor BBS/IRC iniciado na porta 5555")
        print("📡 Conectado ao proxy PUB/SUB")
        print("⏳ Aguardando requisições...")
        
        while True:
            try:
                # Aguarda requisição
                message = self.rep_socket.recv_string()
                request = json.loads(message)
                
                service = request.get("service")
                data = request.get("data", {})
                
                print(f"📨 Serviço: {service}")
                
                response_data = {}
                
                # Roteamento de serviços
                if service == "login":
                    response_data = self.handle_login(data)
                elif service == "users":
                    response_data = self.handle_users(data)
                elif service == "channel":  # Criação
                    response_data = self.handle_channel_create(data)
                elif service == "channels":  # Listagem
                    response_data = self.handle_channels(data)
                elif service == "publish":
                    response_data = self.handle_publish(data)
                elif service == "message":
                    response_data = self.handle_direct_message(data)
                else:
                    response_data = {
                        "status": "erro",
                        "message": f"Serviço '{service}' não encontrado",
                        "timestamp": datetime.now().isoformat()
                    }
                
                # Envia resposta
                response = {
                    "service": service,
                    "data": response_data
                }
                
                self.rep_socket.send_string(json.dumps(response))
                print(f"📤 Resposta: {response_data.get('status', 'OK')}")
                
            except Exception as e:
                error_response = {
                    "service": "error",
                    "data": {
                        "status": "erro",
                        "message": f"Erro interno: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    }
                }
                self.rep_socket.send_string(json.dumps(error_response))

if __name__ == "__main__":
    server = ChatServer()
    server.run()