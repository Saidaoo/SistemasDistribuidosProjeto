import zmq
import time
import json
from datetime import datetime

def test_publisher():
    context = zmq.Context()
    
    socket = context.socket(zmq.PUB)
    socket.connect("tcp://proxy:5557")
    
    print("🚀 Publisher de teste conectado ao proxy")
    time.sleep(1)
    
    topics = ["general", "tech", "news"]
    
    for i in range(10):
        topic = topics[i % len(topics)]
        message = {
            "type": "test_message",
            "content": f"Mensagem de teste #{i}",
            "timestamp": datetime.now().isoformat(),
            "publisher": "test_publisher"
        }
        
        socket.send_string(f"{topic} {json.dumps(message)}")
        print(f"📤 Publicado no tópico '{topic}': Mensagem #{i}")
        time.sleep(2)

if __name__ == "__main__":
    test_publisher()