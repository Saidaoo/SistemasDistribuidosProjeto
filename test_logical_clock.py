import zmq
import msgpack
import time

def test_clock_increment():    
    print("🕐 Testando incremento de relógio lógico...")
    print("-" * 60)
    
    clocks_sent = []
    clocks_received = []
    
    server_ports = [5555, 5556, 5559]
    
    for port in server_ports:
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.RCVTIMEO, 10000)
        socket.setsockopt(zmq.LINGER, 0)
        
        try:
            socket.connect(f"tcp://localhost:{port}")
            time.sleep(0.5)
            
            print(f"\n📡 Testando servidor na porta {port}:")
            print("-" * 40)
            
            for i in range(1, 4):
                clock_value = i + (port - 5555) * 10
                
                msg = {
                    "service": "login",
                    "data": {
                        "user": f"test_user_{port}_{i}",
                        "timestamp": time.time(),
                        "clock": clock_value
                    }
                }
                
                print(f"\n📤 Mensagem {i}:")
                print(f"   Clock enviado: {clock_value}")
                socket.send(msgpack.packb(msg))
                
                resp = msgpack.unpackb(socket.recv(), raw=False)
                received_clock = resp.get("data", {}).get("clock", 0)
                
                print(f"   Clock recebido: {received_clock}")
                
                if received_clock > clock_value:
                    print(f"   ✅ Correto: {received_clock} > {clock_value}")
                else:
                    print(f"   ❌ Erro: {received_clock} não é maior que {clock_value}")
                
                clocks_sent.append(clock_value)
                clocks_received.append(received_clock)
                
                time.sleep(0.3)
            
        except zmq.error.Again:
            print(f"   ⏱️  Timeout: Servidor porta {port} não respondeu")
        except Exception as e:
            print(f"   ❌ Erro: {e}")
            import traceback
            traceback.print_exc()
        finally:
            socket.close()
            context.term()
    
    print("\n" + "=" * 60)
    print("📊 RESUMO GERAL:")
    print(f"   Total de mensagens: {len(clocks_sent)}")
    print(f"   Clocks enviados: {clocks_sent}")
    print(f"   Clocks recebidos: {clocks_received}")
    
    if clocks_received:
        all_correct = all(r > s for r, s in zip(clocks_received, clocks_sent))
        print(f"   ✅ Todos incrementaram corretamente? {all_correct}")
    
    print("\n✅ TESTE CONCLUÍDO!")
    print("🔍 O relógio lógico segue a regra: max(local, received) + 1")

if __name__ == "__main__":
    print("=" * 60)
    print("TESTE DE RELÓGIO LÓGICO (Algoritmo de Lamport)")
    print("=" * 60)
    time.sleep(2)
    test_clock_increment()