#!/usr/bin/env python3
"""
Teste de Sincronização de Dados entre Servidores
Valida critério 4: sincronização de dados
"""

import zmq
import msgpack
import time

def get_data_stats(server_port):
    """Obtém estatísticas de dados de um servidor"""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 15000)  # 15 segundos
    socket.setsockopt(zmq.LINGER, 0)
    
    try:
        socket.connect(f"tcp://localhost:{server_port}")
        time.sleep(1)
        
        msg = {
            "service": "cluster_status",
            "data": {}
        }
        
        print(f"   🔌 Conectando porta {server_port}...")
        socket.send(msgpack.packb(msg))
        
        resp = msgpack.unpackb(socket.recv(), raw=False)
        data = resp.get("data", {})
        return data.get("data_stats", {})
        
    except zmq.error.Again:
        print(f"   ⏱️  Timeout na porta {server_port}")
        return None
    except Exception as e:
        print(f"   ❌ Erro na porta {server_port}: {e}")
        return None
    finally:
        socket.close()
        context.term()

def create_user(server_port, username):
    """Cria usuário diretamente em um servidor"""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 10000)
    socket.setsockopt(zmq.LINGER, 0)
    
    try:
        socket.connect(f"tcp://localhost:{server_port}")
        time.sleep(0.5)
        
        msg = {
            "service": "login",
            "data": {
                "user": username,
                "timestamp": time.time(),
                "clock": 1
            }
        }
        
        socket.send(msgpack.packb(msg))
        resp = msgpack.unpackb(socket.recv(), raw=False)
        
        return resp.get("data", {}).get("status") == "sucesso"
        
    except Exception as e:
        print(f"❌ Erro ao criar usuário: {e}")
        return False
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    print("=" * 60)
    print("TESTE DE SINCRONIZAÇÃO DE DADOS ENTRE SERVIDORES")
    print("=" * 60)
    
    server_ports = [5555, 5556, 5559]
    
    # 1. Estado inicial
    print("\n📊 ETAPA 1: Contagem inicial de dados")
    print("-" * 60)
    
    initial_stats = {}
    for port in server_ports:
        stats = get_data_stats(port)
        if stats:
            initial_stats[port] = stats
            print(f"   Porta {port}: {stats.get('users', 0)} usuários, {stats.get('channels', 0)} canais")
    
    if not initial_stats:
        print("\n❌ Nenhum servidor respondeu")
        exit(1)
    
    time.sleep(2)
    
    # 2. Cria usuário no coordenador (server_1)
    print("\n👤 ETAPA 2: Criando usuário 'sync_test_user' no server_1 (coordenador)")
    print("-" * 60)
    success = create_user(5555, "sync_test_user_" + str(int(time.time())))
    print(f"   {'✅' if success else '❌'} Usuário criado: {success}")
    
    if not success:
        print("\n❌ Falha ao criar usuário")
        exit(1)
    
    # 3. Aguarda sincronização
    print("\n⏳ ETAPA 3: Aguardando sincronização automática (8 segundos)")
    print("-" * 60)
    print("   💡 O coordenador deve sincronizar dados após o login")
    time.sleep(8)
    
    # 4. Verifica sincronização
    print("\n📊 ETAPA 4: Verificando sincronização")
    print("-" * 60)
    
    final_stats = {}
    user_counts = []
    
    for port in server_ports:
        stats = get_data_stats(port)
        if stats:
            final_stats[port] = stats
            count = stats.get('users', 0)
            user_counts.append(count)
            
            # Mostra mudança
            old_count = initial_stats.get(port, {}).get('users', 0)
            change = count - old_count
            
            print(f"   Porta {port}:")
            print(f"      Antes: {old_count} usuários")
            print(f"      Depois: {count} usuários")
            print(f"      {'📈' if change > 0 else '➡️ '} Mudança: +{change}")
    
    # Verifica se todos têm o mesmo número
    if user_counts:
        all_synced = len(set(user_counts)) == 1
        
        print("\n" + "=" * 60)
        print(f"{'✅' if all_synced else '⚠️ '} Sincronização: ", end="")
        if all_synced:
            print("OK - Todos os servidores têm mesmos dados")
        else:
            print(f"Divergente - Counts: {user_counts}")
            print("\n💡 Possíveis causas:")
            print("   1. Sincronização ainda em andamento (aguarde mais)")
            print("   2. Coordenador não está disparando sync após login")
            print("   3. Rede/comunicação entre servidores com problema")
        
        print("\n✅ TESTE CONCLUÍDO!")
        
        # Sugestões de verificação
        print("\n💡 Para verificar sincronização nos logs:")
        print("   docker compose logs server_1 | Select-String 'sincronização'")
        print("   docker compose logs server_2 | Select-String 'sincronização'")
        print("   docker compose logs server_3 | Select-String 'sincronização'")