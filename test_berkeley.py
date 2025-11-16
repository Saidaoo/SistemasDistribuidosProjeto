#!/usr/bin/env python3
"""
Teste de Sincronização Berkeley
Valida critério 4: sincronização de relógio
"""

import zmq
import msgpack
import time

def get_cluster_status(server_port):
    """Obtém status completo do cluster"""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 15000)  # 15 segundos
    socket.setsockopt(zmq.LINGER, 0)
    
    try:
        socket.connect(f"tcp://localhost:{server_port}")
        time.sleep(1)  # Aguarda conexão
        
        msg = {
            "service": "cluster_status",
            "data": {}
        }
        
        print(f"   🔌 Conectando porta {server_port}...")
        socket.send(msgpack.packb(msg))
        
        resp = msgpack.unpackb(socket.recv(), raw=False)
        return resp.get("data", {})
        
    except zmq.error.Again:
        print(f"   ⏱️  Timeout na porta {server_port}")
        return None
    except Exception as e:
        print(f"   ❌ Erro na porta {server_port}: {e}")
        return None
    finally:
        socket.close()
        context.term()

def send_messages_to_trigger_sync(server_port, count=10):
    """Envia mensagens diretamente ao servidor para disparar sincronização"""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 10000)
    socket.setsockopt(zmq.LINGER, 0)
    
    try:
        socket.connect(f"tcp://localhost:{server_port}")
        time.sleep(0.5)
        
        print(f"📨 Enviando {count} mensagens para porta {server_port}...")
        
        for i in range(count):
            msg = {
                "service": "channels",
                "data": {
                    "clock": i + 1
                }
            }
            
            socket.send(msgpack.packb(msg))
            socket.recv()
            
            if (i + 1) == 10:
                print(f"   ✅ 10ª mensagem enviada - Sincronização Berkeley disparada!")
            elif (i + 1) % 3 == 0:
                print(f"   📤 {i + 1}ª mensagem enviada")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    print("=" * 60)
    print("TESTE DE SINCRONIZAÇÃO BERKELEY")
    print("=" * 60)
    
    server_ports = [5555, 5556, 5559]
    
    # 1. Estado inicial
    print("\n📊 ETAPA 1: Estado inicial dos servidores")
    print("-" * 60)
    
    initial_states = {}
    for port in server_ports:
        status = get_cluster_status(port)
        if status:
            initial_states[port] = status
            print(f"\n   Porta {port} ({status['current_server']}):")
            print(f"      Relógio lógico: {status['logical_clock']}")
            print(f"      Offset físico: {status['physical_offset']}")
            print(f"      Contador msgs: {status['message_count']}")
            print(f"      É coordenador: {status['is_coordinator']}")
    
    if not initial_states:
        print("\n❌ Nenhum servidor respondeu. Verifique se estão rodando:")
        print("   docker compose ps")
        exit(1)
    
    time.sleep(2)
    
    # 2. Envia 10 mensagens para o coordenador
    print("\n📨 ETAPA 2: Enviando 10 mensagens para disparar sincronização")
    print("-" * 60)
    
    # Encontra o coordenador
    coordinator_port = None
    for port, status in initial_states.items():
        if status.get('is_coordinator'):
            coordinator_port = port
            print(f"👑 Coordenador encontrado na porta {port}")
            break
    
    if not coordinator_port:
        print("⚠️  Nenhum coordenador encontrado, usando server_1 (porta 5555)")
        coordinator_port = 5555
    
    success = send_messages_to_trigger_sync(coordinator_port, 10)
    
    if success:
        print("\n⏳ Aguardando sincronização Berkeley completar (5 segundos)...")
        time.sleep(5)
        
        # 3. Verifica estado após sincronização
        print("\n📊 ETAPA 3: Estado após sincronização Berkeley")
        print("-" * 60)
        
        for port in server_ports:
            status = get_cluster_status(port)
            if status:
                print(f"\n   Porta {port} ({status['current_server']}):")
                print(f"      Relógio lógico: {status['logical_clock']}")
                print(f"      Offset físico: {status['physical_offset']}")
                print(f"      Contador msgs: {status['message_count']}")
                
                # Compara com estado inicial
                if port in initial_states:
                    old_offset = initial_states[port]['physical_offset']
                    new_offset = status['physical_offset']
                    if old_offset != new_offset:
                        print(f"      🔄 Offset mudou: {old_offset} → {new_offset}")
        
        print("\n✅ TESTE CONCLUÍDO!")
        print("\n💡 Para ver detalhes da sincronização Berkeley, execute:")
        print("   docker compose logs server_1 | Select-String 'Berkeley'")
        print("   docker compose logs server_2 | Select-String 'Berkeley'")
        print("   docker compose logs server_3 | Select-String 'Berkeley'")
    else:
        print("\n❌ Falha ao enviar mensagens")