# reqrep/cliente.py
import zmq
import json
import time
from datetime import datetime
import threading

class BBSClient:
    def __init__(self, server_host="servidor", server_port="5555"):
        self.context = zmq.Context()
        
        # Socket REQ para servidor
        self.req_socket = self.context.socket(zmq.REQ)
        self.req_socket.connect(f"tcp://{server_host}:{server_port}")
        
        # Socket SUB para receber mensagens
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect("tcp://proxy:5558")
        
        self.current_user = None
        self.listening = False
        
    def send_request(self, service, data):
        """Envia requisição ao servidor"""
        request = {
            "service": service,
            "data": data
        }
        
        self.req_socket.send_string(json.dumps(request))
        response = self.req_socket.recv_string()
        return json.loads(response)
    
    def login(self, username):
        """Faz login no sistema"""
        data = {
            "user": username,
            "timestamp": datetime.now().isoformat()
        }
        
        response = self.send_request("login", data)
        
        if response["data"]["status"] == "sucesso":
            self.current_user = username
            # Inscreve no próprio tópico para receber mensagens diretas
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, username)
            print(f"✅ Login realizado como: {username}")
            return True
        else:
            print(f"❌ Erro no login: {response['data']['description']}")
            return False
    
    def list_users(self):
        """Lista todos os usuários"""
        data = {"timestamp": datetime.now().isoformat()}
        response = self.send_request("users", data)
        
        if "users" in response["data"]:
            print("\n👥 Usuários cadastrados:")
            for user in response["data"]["users"]:
                print(f"  - {user}")
            return response["data"]["users"]
        return []
    
    def list_channels(self):
        """Lista todos os canais"""
        data = {"timestamp": datetime.now().isoformat()}
        response = self.send_request("channels", data)
        
        if "channels" in response["data"]:
            print("\n📺 Canais disponíveis:")
            for i, channel in enumerate(response["data"]["channels"], 1):
                print(f"  {i}. #{channel}")
            return response["data"]["channels"]
        return []
    
    def create_channel(self, channel_name):
        """Cria um novo canal"""
        data = {
            "channel": channel_name,
            "timestamp": datetime.now().isoformat()
        }
        
        response = self.send_request("channel", data)
        
        if response["data"]["status"] == "sucesso":
            print(f"✅ Canal '#{channel_name}' criado com sucesso!")
            # Inscreve no novo canal
            self.subscribe_channel(channel_name)
            return True
        else:
            print(f"❌ Erro ao criar canal: {response['data']['description']}")
            return False
    
    def subscribe_channel(self, channel_name):
        """Inscreve em um canal"""
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, channel_name)
        print(f"📡 Inscrito no canal: #{channel_name}")
    
    def publish_message(self, channel, message):
        """Publica mensagem em canal"""
        data = {
            "user": self.current_user,
            "channel": channel,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        
        response = self.send_request("publish", data)
        
        if response["data"]["status"] == "OK":
            print(f"✅ Mensagem publicada em #{channel}")
            return True
        else:
            print(f"❌ Erro: {response['data']['message']}")
            return False
    
    def send_direct_message(self, target_user, message):
        """Envia mensagem direta"""
        data = {
            "src": self.current_user,
            "dst": target_user,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        
        response = self.send_request("message", data)
        
        if response["data"]["status"] == "OK":
            print(f"✅ Mensagem enviada para {target_user}")
            return True
        else:
            print(f"❌ Erro: {response['data']['message']}")
            return False
    
    def listen_messages_background(self):
        """Escuta mensagens em background (thread)"""
        while self.listening:
            try:
                message = self.sub_socket.recv_string(zmq.NOBLOCK)
                topic, data = message.split(' ', 1)
                msg_data = json.loads(data)
            
                if msg_data.get("type") == "direct_message":
                    print(f"\n💌 [MENSAGEM PRIVADA] De {msg_data['from']}: {msg_data['message']}")
                    print(f"BBS {self.current_user} > ", end="", flush=True)
                elif msg_data.get("type") == "channel_message":
                    if msg_data['user'] != self.current_user:  # Não mostrar próprias mensagens
                        print(f"\n📢 [#{msg_data['channel']}] {msg_data['user']}: {msg_data['message']}")
                        print(f"BBS {self.current_user} > ", end="", flush=True)
                    
            except zmq.Again:
                time.sleep(0.1)
            except Exception as e:
                if self.listening:  # Só mostra erro se ainda estiver escutando
                    print(f"\nErro ao escutar mensagens: {e}")
                    print(f"BBS {self.current_user} > ", end="", flush=True)
    
    def start_listening(self):
        """Inicia a escuta em background"""
        self.listening = True
        listener_thread = threading.Thread(target=self.listen_messages_background)
        listener_thread.daemon = True
        listener_thread.start()
        print("🔊 Escutando mensagens em background...")
    
    def stop_listening(self):
        """Para a escuta em background"""
        self.listening = False
    
    def show_welcome(self):
        """Mostra tela de boas-vindas"""
        print("\n" + "="*50)
        print("          🚀 SISTEMA BBS/IRC")
        print("="*50)
    
    def show_menu(self):
        """Mostra menu principal"""
        print(f"\n--- BBS User: {self.current_user} ---")
        print("1. 📋 Listar usuários")
        print("2. 📺 Listar canais") 
        print("3. ➕ Criar canal")
        print("4. 📢 Publicar em canal")
        print("5. 💌 Enviar mensagem direta")
        print("6. 📡 Inscrever em canal")
        print("7. 🚪 Sair")
    
    def interactive_mode(self):
        """Modo interativo para usuário real"""
        self.show_welcome()
        
        # Login
        while not self.current_user:
            username = input("\nDigite seu nome de usuário: ").strip()
            if username:
                if self.login(username):
                    break
            else:
                print("❌ Nome de usuário não pode estar vazio")
        
        # Inscreve em canal geral por padrão
        self.subscribe_channel("geral")
        print("✅ Inscrito automaticamente no canal #geral")
        
        # Inicia escuta em background
        self.start_listening()
        
        # Menu principal
        try:
            while True:
                print(f"\n" + "="*40)
                print(f"BBS User: {self.current_user}")
                print("="*40)
                self.show_menu()
                print("-"*40)
                
                try:
                    option = input("Escolha uma opção: ").strip()
                    
                    if option == "1":
                        self.list_users()
                        
                    elif option == "2":
                        channels = self.list_channels()
                        if channels:
                            sub_option = input("\nDeseja se inscrever em algum canal? (s/n): ").strip().lower()
                            if sub_option == 's':
                                channel_name = input("Nome do canal: ").strip()
                                if channel_name in channels:
                                    self.subscribe_channel(channel_name)
                                else:
                                    print("❌ Canal não encontrado")
                    
                    elif option == "3":
                        channel_name = input("Nome do novo canal: ").strip()
                        if channel_name:
                            self.create_channel(channel_name)
                        else:
                            print("❌ Nome do canal não pode estar vazio")
                    
                    elif option == "4":
                        channel = input("Canal: ").strip()
                        if channel:
                            message = input("Mensagem: ").strip()
                            if message:
                                self.publish_message(channel, message)
                            else:
                                print("❌ Mensagem não pode estar vazia")
                        else:
                            print("❌ Canal não pode estar vazio")
                    
                    elif option == "5":
                        users = self.list_users()
                        if users:
                            target_user = input("\nUsuário destino: ").strip()
                            if target_user:
                                if target_user in users:
                                    message = input("Mensagem: ").strip()
                                    if message:
                                        self.send_direct_message(target_user, message)
                                    else:
                                        print("❌ Mensagem não pode estar vazia")
                                else:
                                    print("❌ Usuário não encontrado")
                    
                    elif option == "6":
                        channels = self.list_channels()
                        if channels:
                            channel_name = input("\nNome do canal para se inscrever: ").strip()
                            if channel_name in channels:
                                self.subscribe_channel(channel_name)
                            else:
                                print("❌ Canal não encontrado")
                    
                    elif option == "7":
                        print("\n👋 Saindo do sistema...")
                        break
                    
                    else:
                        print("❌ Opção inválida. Digite um número de 1 a 7.")
                    
                    # Pequena pausa para visualização
                    time.sleep(1)
                        
                except KeyboardInterrupt:
                    print("\n\n👋 Voltando ao menu...")
                    continue
                    
        except KeyboardInterrupt:
            print("\n\n👋 Saindo do sistema...")
        finally:
            self.stop_listening()

if __name__ == "__main__":
    client = BBSClient()
    client.interactive_mode()