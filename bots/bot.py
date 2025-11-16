#!/usr/bin/env python3
import zmq
import msgpack
import time
import random
from datetime import datetime

# Canais disponíveis
AVAILABLE_CHANNELS = [
    "geral", "random", "tech", "games", "music",
    "movies", "sports", "programming", "help", "offtopic"
]

# Mensagens pré-definidas
MESSAGE_TEMPLATES = [
    "Olá pessoal! Como vocês estão?",
    "Alguém online para conversar?",
    "Que dia interessante hoje!",
    "Alguém quer discutir sobre tecnologia?",
    "Alguém joga algum jogo interessante?",
    "Recomendações de filmes ou séries?",
    "Preciso de ajuda com programação...",
    "Que música vocês estão ouvindo?",
    "Alguém trabalha com desenvolvimento?",
    "Bom dia/tarde/noite a todos!",
    "Alguma novidade no mundo da tech?",
    "Estou estudando sistemas distribuídos!",
    "Os bots estão ficando inteligentes! 😄",
    "Como está o clima na cidade de vocês?",
    "Alguém quer formar um grupo de estudo?"
]

# Respostas para interações
RESPONSE_TEMPLATES = [
    "Interessante! Conte-me mais sobre isso.",
    "Eu também acho isso fascinante!",
    "Você já tentou resolver dessa forma?",
    "Isso me lembra uma experiência similar...",
    "Ótima pergunta! Vamos discutir isso.",
    "Eu concordo com você!",
    "Que ponto de vista interessante!",
    "Você tem experiência com isso?",
    "Isso é muito relevante para o tópico!",
    "Obrigado por compartilhar isso!"
]

class Bot:
    def __init__(self, bot_id, username, proxy_host="proxy", proxy_pub_port=5557, proxy_sub_port=5558):
        self.bot_id = bot_id
        self.username = username
        self.current_channel = "geral"
        self.is_active = True
        
        print(f"🤖 Inicializando {self.username}...")
        
        # Contexto ZeroMQ
        self.context = zmq.Context()
        
        # Socket PUB → Proxy (para enviar mensagens)
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect(f"tcp://{proxy_host}:{proxy_pub_port}")
        print(f"✅ PUB conectado ao proxy: tcp://{proxy_host}:{proxy_pub_port}")
        
        # Socket SUB ← Proxy (para receber mensagens de usuários)
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(f"tcp://{proxy_host}:{proxy_sub_port}")
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b"")  # Inscreve em todos os canais
        self.sub_socket.setsockopt(zmq.RCVTIMEO, 100)  # Timeout de 100ms
        print(f"✅ SUB conectado ao proxy: tcp://{proxy_host}:{proxy_sub_port}")
        
        time.sleep(1)  # Aguarda conexão estabilizar
        
        print(f"🎉 {self.username} pronto!\n")
    
    def get_timestamp(self):
        """Retorna timestamp formatado"""
        return datetime.now().strftime("%H:%M:%S")
    
    def send_message(self, message):
        """Envia mensagem MessagePack para o proxy"""
        timestamp = self.get_timestamp()
        
        formatted_message = f"[{timestamp}] {self.username}: {message}"
        
        # ⭐ Formato MessagePack igual ao servidor
        msg_data = {
            "service": "publish",
            "data": {
                "channel": self.current_channel,
                "message": formatted_message,
                "user": self.username
            }
        }
        
        # Envia como multipart: [canal, dados_msgpack]
        self.pub_socket.send_multipart([
            self.current_channel.encode(),
            msgpack.packb(msg_data)
        ])
        
        print(f"📨 {self.username} enviou para '{self.current_channel}': {formatted_message}")
    
    def listen_for_messages(self):
        """Escuta mensagens de usuários (não-bloqueante)"""
        try:
            # Recebe tópico/canal
            topic = self.sub_socket.recv()
            # Recebe mensagem
            raw_message = self.sub_socket.recv()
            
            # Verifica se não é mensagem de bot
            message_str = raw_message.decode('utf-8', errors='ignore')
            if "Bot_" not in message_str and random.random() < 0.8:  # 80% chance de responder
                print(f"🤖 {self.username} recebeu mensagem de usuário: {message_str[:50]}...")
                
                # Gera resposta
                response = random.choice(RESPONSE_TEMPLATES)
                self.send_message(response)
        
        except zmq.Again:
            pass  # Timeout, sem mensagens
        except Exception as e:
            pass  # Ignora erros de parse
    
    def change_channel(self):
        """Muda de canal aleatoriamente"""
        if random.random() < 0.15:  # 15% de chance
            new_channel = random.choice(AVAILABLE_CHANNELS)
            self.current_channel = new_channel
            print(f"🤖 {self.username} mudou para o canal: {new_channel}")
    
    def run(self):
        """Loop principal do bot"""
        cycle = 0
        
        while self.is_active:
            cycle += 1
            
            # Escuta mensagens de usuários
            self.listen_for_messages()
            
            # Comportamento do bot
            self.change_channel()
            
            # 60% de chance de enviar mensagem
            if random.random() < 0.6:
                message = random.choice(MESSAGE_TEMPLATES)
                self.send_message(message)
            
            # Aguarda intervalo aleatório (3-10 segundos)
            interval = random.randint(3, 10)
            time.sleep(interval)
            
            # A cada 10 ciclos, mostra status
            if cycle % 10 == 0:
                print(f"\n=== STATUS {self.username} ===")
                print(f"Canal: {self.current_channel}")
                print(f"Ciclo: {cycle}")
                print("=" * 30 + "\n")
    
    def cleanup(self):
        """Limpa recursos"""
        self.pub_socket.close()
        self.sub_socket.close()
        self.context.term()


if __name__ == "__main__":
    import sys
    import os
    
    bot_name = os.getenv("BOT_NAME", "Bot_Default")
    bot_id = 1 if "Alice" in bot_name else 2
    
    print("=" * 50)
    print(f"🚀 Iniciando {bot_name}")
    print("=" * 50)
    
    bot = Bot(bot_id, bot_name)
    
    try:
        bot.run()
    except KeyboardInterrupt:
        print(f"\n⚠️  {bot_name} encerrado pelo usuário")
    finally:
        bot.cleanup()