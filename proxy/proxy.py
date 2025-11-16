import zmq

def run_proxy():
    context = zmq.Context()

    xsub_socket = context.socket(zmq.XSUB)
    xsub_socket.bind("tcp://*:5557")
    
    xpub_socket = context.socket(zmq.XPUB)
    xpub_socket.bind("tcp://*:5558")
    
    print("🚀 Proxy PUB/SUB iniciado:")
    print("XSUB (Publishers) na porta 5557")
    print("XPUB (Subscribers) na porta 5558")
    
    zmq.proxy(xsub_socket, xpub_socket)

if __name__ == "__main__":
    run_proxy()