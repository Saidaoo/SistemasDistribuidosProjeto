#!/usr/bin/env python3
"""
Teste de Eleição de Coordenador (Algoritmo Bully)
Valida critério 4: eleição de coordenador
"""

import zmq
import msgpack
import time

def trigger_election(server_port):
    """Força eleição em um servidor específico"""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 10000)
    socket.setsockopt(zmq.LINGER, 0)
    
    try:
        endpoint = f"tcp://localhost:{server_port}"
        print(f"🔌 Conectando em: {endpoint}")
        socket.connect(endpoint)
        time.sleep(0.5)
        
        msg = {
            "service": "trigger_election",
            "data": {
                "timestamp": time.time(),
                "clock": 1
            }
        }
        
        print(f"🗳️  Disparando eleição no servidor porta {server_port}")
        socket.send(msgpack.packb(msg))
        
        resp = msgpack.unpackb(socket.recv(), raw=False)
        print(f"✅ Resposta: {resp}")
        return True
        
    except zmq.error.Again:
        print(f"⏱️  Timeout: Servidor não respondeu em 10 segundos")
        return False
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        socket.close()
        context.term()

def check_coordinator(server_port):
    """Verifica qual é o coordenador atual"""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 10000)
    socket.setsockopt(zmq.LINGER, 0)
    
    try:
        endpoint = f"tcp://localhost:{server_port}"
        print(f"🔌 Conectando em: {endpoint}")
        socket.connect(endpoint)
        time.sleep(0.5)
        
        msg = {
            "service": "status",
            "data": {}
        }
        
        socket.send(msgpack.packb(msg))
        resp = msgpack.unpackb(socket.recv(), raw=False)
        
        data = resp.get("data", {})
        print(f"📊 Status da porta {server_port}:")
        print(f"   Servidor: {data.get('server_name', 'N/A')}")
        print(f"   Rank: {data.get('rank', 'N/A')}")
        print(f"   Coordenador: {data.get('coordinator', 'N/A')}")
        is_coord = data.get('coordinator') == data.get('server_name')
        print(f"   É coordenador: {is_coord}")
        
        return data
        
    except zmq.error.Again:
        print(f"⏱️  Timeout: Servidor porta {server_port} não respondeu")
        return None
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    print("=" * 60)
    print("TESTE DE ELEIÇÃO DE COORDENADOR (Algoritmo Bully)")
    print("=" * 60)
    
    server_ports = [5555, 5556, 5559]
    
    print("\n⏳ Aguardando servidores ficarem prontos (3 segundos)...")
    time.sleep(3)
    
    # 1. Verifica estado inicial
    print("\n📋 ETAPA 1: Estado inicial dos servidores")
    print("-" * 60)
    
    for port in server_ports:
        check_coordinator(port)
        print()
    
    time.sleep(2)
    
    # 2. Dispara eleição no server_3 (porta 5559, rank mais alto)
    print("\n🗳️  ETAPA 2: Disparando eleição no server_3 (porta 5559, rank 3)")
    print("-" * 60)
    success = trigger_election(5559)
    
    if success:
        print("\n⏳ Aguardando eleição completar (5 segundos)...")
        time.sleep(5)
        
        # 3. Verifica novo coordenador
        print("\n👑 ETAPA 3: Verificando novo coordenador")
        print("-" * 60)
        for port in server_ports:
            check_coordinator(port)
            print()
        
        print("\n✅ TESTE CONCLUÍDO!")
        print("🔍 Verifique se o servidor com MENOR rank foi eleito coordenador")
        print("   (Esperado: server_1 com rank 1)")
    else:
        print("\n❌ Eleição não pôde ser disparada")
        print("💡 Verifique os logs dos servidores:")
        print("   docker compose logs server_1")
        print("   docker compose logs server_2")
        print("   docker compose logs server_3")