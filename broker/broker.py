import zmq

def run_broker():
    context = zmq.Context()
    
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5550")
    
    print("=" * 60)
    print("🚀 BROKER INICIADO")
    print("=" * 60)
    print("✅ Frontend (ROUTER) escutando em: tcp://*:5550")
    print("   → Clientes e bots conectam aqui via REQ")
    print()
    
    backend = context.socket(zmq.DEALER)
    
    servers = [
        "tcp://server_1:5555",
        "tcp://server_2:5556", 
        "tcp://server_3:5559"
    ]
    
    print("✅ Backend (DEALER) conectando aos servidores:")
    for server in servers:
        backend.connect(server)
        print(f"   → {server}")
    
    print()
    print("🔄 Balanceamento de carga: ROUND-ROBIN automático")
    print("📊 Cada requisição vai para um servidor diferente")
    print()
    print("=" * 60)
    print("Aguardando requisições...")
    print("=" * 60)
    print()
    
    try:
        zmq.proxy(frontend, backend)
    except KeyboardInterrupt:
        print("\n⚠️  Broker encerrado pelo usuário")
    except Exception as e:
        print(f"\n❌ Erro no broker: {e}")
    finally:
        frontend.close()
        backend.close()
        context.term()

if __name__ == "__main__":
    run_broker()