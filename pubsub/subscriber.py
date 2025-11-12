# pubsub/subscriber.py
import zmq
import json

def test_subscriber():
    context = zmq.Context()
    
    # Conecta no proxy XPUB
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://proxy:5558")
    
    # Inscreve em todos os tópicos
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print("👂 Subscriber de teste conectado ao proxy")
    print("Aguardando mensagens...")
    
    try:
        while True:
            message = socket.recv_string()
            topic, data = message.split(' ', 1)
            message_data = json.loads(data)
            
            print(f"\n📨 Nova mensagem recebida:")
            print(f"   Tópico: {topic}")
            print(f"   Conteúdo: {message_data}")
            
    except KeyboardInterrupt:
        print("\nSubscriber finalizado")

if __name__ == "__main__":
    test_subscriber()