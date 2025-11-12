# proxy/proxy.py
import zmq

def run_proxy():
    """Proxy ZeroMQ para PUB/SUB"""
    context = zmq.Context()
    
    # Socket para receber mensagens de publishers
    xsub_socket = context.socket(zmq.XSUB)
    xsub_socket.bind("tcp://*:5557")
    
    # Socket para enviar mensagens para subscribers
    xpub_socket = context.socket(zmq.XPUB)
    xpub_socket.bind("tcp://*:5558")
    
    print("🚀 Proxy PUB/SUB iniciado:")
    print("XSUB (Publishers) na porta 5557")
    print("XPUB (Subscribers) na porta 5558")
    
    # Proxy que conecta XSUB e XPUB
    zmq.proxy(xsub_socket, xpub_socket)

if __name__ == "__main__":
    run_proxy()