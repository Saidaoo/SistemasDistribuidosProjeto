import zmq
import time
import json
import os
import msgpack
from datetime import datetime
import threading
import sys
sys.stdout = sys.__stdout__

HEARTBEAT_INTERVAL = 3

class BerkeleyClockSync:
    def __init__(self, server):
        self.server = server
        self.physical_offset = 0
    
    def synchronize_physical_clock(self):
        if self.server.coordinator == self.server.server_name:
            self.start_berkeley_sync()
        else:
            self.sync_with_coordinator()
    
    def sync_with_coordinator(self):
        if not self.server.coordinator:
            print("❌ Nenhum coordenador definido para sincronização")
            return
        
        coordinator_info = self.server.get_server_info(self.server.coordinator)
        
        if not coordinator_info:
            print(f"❌ Não foi possível obter info do coordenador {self.server.coordinator}")
            return
        
        try:
            coord_socket = self.server.context.socket(zmq.REQ)
            coord_socket.setsockopt(zmq.RCVTIMEO, 5000)
            coord_socket.connect(f"tcp://{coordinator_info['host']}:{coordinator_info['port']}")
            

            t1 = time.time()
            sync_msg = {
                "service": "clock_sync",
                "data": {
                    "t1": t1,
                    "clock": self.server.logical_clock
                }
            }
            
            print(f"🕐 Solicitando sincronização Berkeley com coordenador {self.server.coordinator}")
            coord_socket.send(msgpack.packb(sync_msg))
            
            try:
                resp = msgpack.unpackb(coord_socket.recv(), raw=False)
                t4 = time.time()
                
                t2 = resp["data"]["t2"]
                t3 = resp["data"]["t3"]
                coord_clock = resp["data"]["clock"]
                

                round_trip_time = (t4 - t1) - (t3 - t2)
                offset = ((t2 - t1) + (t3 - t4)) / 2
                
                self.physical_offset = offset
                new_time = time.time() + offset
                
                print(f"✅ Sincronização Berkeley completa:")
                print(f"   📊 Offset calculado: {offset:.6f} segundos")
                print(f"   ⏱️  Tempo ajustado: {datetime.fromtimestamp(new_time)}")
                print(f"   🔄 Round-trip time: {round_trip_time:.6f} segundos")
                

                self.server.increment_clock(coord_clock)
                
            except zmq.error.Again:
                print("⏱️ Timeout ao aguardar resposta do coordenador")
            
            coord_socket.close()
            
        except Exception as e:
            print(f"❌ Erro na sincronização Berkeley: {e}")
    
    def start_berkeley_sync(self):
        print("👑 Coordenador iniciando sincronização Berkeley com cluster...")
        
        try:
            server_list = self.server.get_server_list()
            if not server_list:
                print("❌ Nenhum servidor disponível para sincronização")
                return
            
            offsets = []
            coordinator_time = time.time()
            
            for server_info in server_list:
                if server_info["name"] != self.server.server_name:
                    try:
                        sync_socket = self.server.context.socket(zmq.REQ)
                        sync_socket.setsockopt(zmq.RCVTIMEO, 3000)
                        sync_socket.connect(f"tcp://{server_info['host']}:{server_info['port']}")
                        
                        time_msg = {
                            "service": "get_time",
                            "data": {
                                "clock": self.server.logical_clock
                            }
                        }
                        
                        sync_socket.send(msgpack.packb(time_msg))
                        resp = msgpack.unpackb(sync_socket.recv(), raw=False)
                        
                        server_time = resp["data"]["time"]
                        offset = server_time - coordinator_time
                        offsets.append(offset)
                        
                        print(f"   ⏰ {server_info['name']}: offset {offset:.6f}s")
                        sync_socket.close()
                        
                    except Exception as e:
                        print(f"   ❌ Erro ao sincronizar com {server_info['name']}: {e}")
            
            if offsets:
                avg_offset = sum(offsets) / len(offsets)
                print(f"📊 Offset médio calculado: {avg_offset:.6f} segundos")
                print("✅ Sincronização Berkeley completada")
            else:
                print("⚠️  Nenhum offset coletado")
                
        except Exception as e:
            print(f"💥 Erro na sincronização Berkeley: {e}")

    def handle_get_time(self, data):
        received_clock = data.get("clock", 0)
        current_clock = self.increment_clock(received_clock)
        
        return {
            "time": time.time(),
            "clock": current_clock
        }
    
    def start_election(self):
        try:
            print("\n🚨 INICIANDO ELEIÇÃO", flush=True)
            
            time.sleep(0.2)
            
            server_list = self.server.get_server_list()
            
            if not server_list:
                print("❌ Não foi possível obter lista de servidores", flush=True)
                return
            
            my_rank = int(self.server.rank) if self.server.rank else 999
            election_responses = []
            
            print(f"🗳️ Meu rank: {my_rank}", flush=True)
            print(f"📋 Servidores disponíveis: {len(server_list)}", flush=True)
            
            lower_rank_servers = [s for s in server_list 
                                if s["rank"] < my_rank and s["name"] != self.server.server_name]
            
            print(f"📊 Servidores com rank menor que {my_rank}: {len(lower_rank_servers)}", flush=True)
            for srv in lower_rank_servers:
                print(f"   - {srv['name']} (rank {srv['rank']})", flush=True)
            
            if not lower_rank_servers:
                print("🎉 Nenhum servidor com rank menor - EU SOU O COORDENADOR!", flush=True)
                self.server.declare_coordinator()
                return
            
            for server_info in lower_rank_servers:
                try:
                    election_socket = self.server.context.socket(zmq.REQ)
                    election_socket.setsockopt(zmq.RCVTIMEO, 2000)
                    election_socket.connect(f"tcp://{server_info['host']}:{server_info['port']}")
                    
                    election_msg = {
                        "service": "election",
                        "data": {
                            "timestamp": time.time(),
                            "clock": self.server.increment_clock(),
                            "from_rank": my_rank
                        }
                    }
                    
                    print(f"   📤 Enviando ELECTION para {server_info['name']} (rank {server_info['rank']})...", flush=True)
                    election_socket.send(msgpack.packb(election_msg))
                    
                    try:
                        resp = msgpack.unpackb(election_socket.recv(), raw=False)
                        election_responses.append({
                            "server": server_info["name"],
                            "rank": server_info["rank"],
                            "response": resp
                        })
                        print(f"   ✅ {server_info['name']} respondeu OK", flush=True)
                    except zmq.error.Again:
                        print(f"   ⏱️ {server_info['name']} não respondeu (timeout)", flush=True)
                    
                    election_socket.close()
                    
                except Exception as e:
                    print(f"   ❌ Erro ao enviar eleição para {server_info['name']}: {e}", flush=True)
            

            if election_responses:
                print(f"\n⏳ {len(election_responses)} servidor(es) com rank menor responderam", flush=True)
                print(f"👀 Aguardando que o servidor com menor rank se declare coordenador...", flush=True)
                
                election_responses.sort(key=lambda x: x["rank"])
                winner = election_responses[0]
                
                print(f"🏆 Servidor {winner['server']} (rank {winner['rank']}) deve assumir como coordenador", flush=True)
                
            else:
                print("\n🎉 Nenhum servidor com rank menor respondeu - EU SOU O COORDENADOR!", flush=True)
                self.server.declare_coordinator()
        
        except Exception as e:
            print(f"\n💥 ERRO em start_election: {e}", flush=True)
            import traceback
            traceback.print_exc()

class ClusteredServer:
    def __init__(self):
        self.server_name = os.getenv("SERVER_NAME", "server_x")
        self.reference_host = os.getenv("REFERENCE_HOST", "referencia")
        self.proxy_host = os.getenv("PROXY_HOST", "proxy")
        threading.Thread(target=self.periodic_sync, daemon=True).start()
        
        self.server_ports = {
            "server_1": 5555,
            "server_2": 5556,
            "server_3": 5559
        }
        self.server_port = self.server_ports.get(self.server_name, 5555)

        print(f"🚀 Iniciando servidor {self.server_name}")
        print(f"   Porta REP: {self.server_port}")
        print(f"   Referência: {self.reference_host}:6000")
        print(f"   Proxy: {self.proxy_host}:5557")

        self.context = zmq.Context()
        
        self.logical_clock = 0
        self.message_count = 0
        self.clock_sync = BerkeleyClockSync(self)

        self.rep_socket = self.context.socket(zmq.REP)
        self.rep_socket.bind(f"tcp://*:{self.server_port}")
        print(f"✅ Socket REP escutando em tcp://*:{self.server_port}")

        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect(f"tcp://{self.proxy_host}:5557")
        print(f"✅ Socket PUB conectado ao {self.proxy_host}:5557")

        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(f"tcp://{self.proxy_host}:5558")
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "servers")
        print(f"✅ Socket SUB conectado ao {self.proxy_host}:5558")
        print(f"   📻 Inscrito no tópico: servers")

        time.sleep(1)

        self.ref_socket = self.context.socket(zmq.REQ)
        self.ref_socket.connect(f"tcp://{self.reference_host}:6000")
        print(f"✅ Socket REQ conectado ao {self.reference_host}:6000")

        self.init_storage()
        self.users = self.load("/app/data/users.json", default=[])
        self.channels = self.load("/app/data/channels.json", default=["geral"])
        self.user_logins = self.load("/app/data/user_logins.json", default=[])
        self.messages = self.load("/app/data/messages.json", default=[])

        self.rank = None
        self.coordinator = None
        self.data_lock = threading.Lock()

        self.register_in_reference()
        
        threading.Thread(target=self.listen_coordinator_announcements, daemon=True).start()
        
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

        print(f"🔥 Servidor {self.server_name} iniciado e pronto!", flush=True)
        print(f"🕐 Relógio lógico inicializado: {self.logical_clock}")
        print()

    def listen_coordinator_announcements(self):
        print("👂 Thread de escuta de anúncios de coordenador iniciada...")
        
        while True:
            try:
                topic, payload = self.sub_socket.recv_multipart()
                topic_str = topic.decode()
                msg = msgpack.unpackb(payload, raw=False)
                
                print(f"\n📡 ANÚNCIO recebido no tópico '{topic_str}'", flush=True)
                
                service = msg.get("service")
                data = msg.get("data", {})
                
                if service == "election":
                    coordinator = data.get("coordinator")
                    if coordinator:
                        received_clock = data.get("clock", 0)
                        self.increment_clock(received_clock)
                        
                        print(f"👑 NOVO COORDENADOR ANUNCIADO: {coordinator}", flush=True)
                        self.coordinator = coordinator
                        
                        if coordinator == self.server_name:
                            print("   🎉 EU SOU O COORDENADOR!", flush=True)
                        else:
                            print(f"   ✅ Reconheço {coordinator} como coordenador", flush=True)
                    
            except Exception as e:
                print(f"❌ Erro ao processar anúncio: {e}", flush=True)

    def get_server_list(self):
        try:
            temp_context = zmq.Context()
            temp_socket = temp_context.socket(zmq.REQ)
            temp_socket.setsockopt(zmq.RCVTIMEO, 5000)
            temp_socket.setsockopt(zmq.LINGER, 0)
            temp_socket.connect(f"tcp://{self.reference_host}:6000")
            
            time.sleep(0.1)
            
            list_msg = {
                "service": "list",
                "data": {
                    "timestamp": time.time(),
                    "clock": self.logical_clock
                }
            }
            
            temp_socket.send(msgpack.packb(list_msg))
            resp = msgpack.unpackb(temp_socket.recv(), raw=False)
            
            temp_socket.close()
            temp_context.term()
            
            server_list = resp["data"]["list"]
            
            enhanced_list = []
            for server in server_list:
                enhanced_list.append({
                    "name": server["name"],
                    "rank": server["rank"],
                    "host": server["name"],
                    "port": self.server_ports.get(server["name"], 5555)
                })
            
            return enhanced_list
            
        except zmq.error.Again:
            print(f"❌ Timeout ao buscar lista de servidores", flush=True)
            return []
        except Exception as e:
            print(f"❌ Erro ao buscar lista: {e}", flush=True)
            return []
    
    def get_server_info(self, server_name):
        server_list = self.get_server_list()
        for server in server_list:
            if server["name"] == server_name:
                return server
        return None

    def increment_clock(self, received_clock=None):
        if received_clock is not None:
            self.logical_clock = max(self.logical_clock, received_clock) + 1
        else:
            self.logical_clock += 1
        return self.logical_clock

    def load(self, path, default):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except:
            return default

    def save(self, path, data):
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def init_storage(self):
        os.makedirs("/app/data", exist_ok=True)
        for f in ["users.json", "channels.json", "user_logins.json", "messages.json"]:
            full = f"/app/data/{f}"
            if not os.path.exists(full):
                with open(full, "w") as fp:
                    json.dump([], fp)

    def register_in_reference(self):
        print("📡 Registrando no servidor de referência...")

        msg = {
            "service": "rank",
            "data": {
                "user": self.server_name, 
                "timestamp": time.time(),
                "clock": self.logical_clock
            }
        }
        self.ref_socket.send(msgpack.packb(msg))
        resp = msgpack.unpackb(self.ref_socket.recv(), raw=False)

        self.rank = resp["data"]["rank"]
        print(f"🏅 Rank recebido: {self.rank}")

    def send_heartbeat(self):
        while True:
            try:
                msg = {
                    "service": "heartbeat",
                    "data": {
                        "user": self.server_name,
                        "clock": self.logical_clock
                    }
                }
                self.ref_socket.send(msgpack.packb(msg))
                self.ref_socket.recv()
            except Exception as e:
                print(f"❌ Erro no heartbeat: {e}")
            
            time.sleep(HEARTBEAT_INTERVAL)

    def run(self):
        print(f"🖥️  {self.server_name} aguardando requisições...")
        print()

        while True:
            try:
                print(f"⏳ [{self.server_name}] Aguardando mensagem...")
                raw = self.rep_socket.recv()
                print(f"📨 [{self.server_name}] Mensagem recebida! Tamanho: {len(raw)} bytes")
                
                try:
                    req = msgpack.unpackb(raw, raw=False)
                    print(f"🔍 [{self.server_name}] Serviço: '{req.get('service')}'")
                except Exception as e:
                    print(f"❌ Erro na desserialização: {e}")
                    continue
                
                service = req.get("service")
                data = req.get("data", {})
                
                if not service:
                    error_resp = {"status": "erro", "message": "Serviço não especificado"}
                    self.rep_socket.send(msgpack.packb({"service": "error", "data": error_resp}))
                    continue

                handler = getattr(self, f"handle_{service}", None)
                
                if handler:
                    print(f"✅ [{self.server_name}] Chamando handle_{service}")
                    resp = handler(data)
                else:
                    print(f"❌ [{self.server_name}] Handler não encontrado: handle_{service}")
                    resp = {"status": "erro", "message": f"Handler {service} não encontrado"}
                
                response_msg = {"service": service, "data": resp}
                self.rep_socket.send(msgpack.packb(response_msg))
                print(f"📤 [{self.server_name}] Resposta enviada\n")
                
            except Exception as e:
                print(f"💥 Erro geral: {e}")
                import traceback
                traceback.print_exc()

    def handle_login(self, data):
        user = data.get("user")
        received_clock = data.get("clock", 0)
        current_clock = self.increment_clock(received_clock)
        
        self.message_count += 1
        if self.message_count >= 10:
            print("🎯 MENSAGEM 10 - Sincronização física!")
            self.clock_sync.synchronize_physical_clock()
            self.message_count = 0
        
        if self.coordinator == self.server_name:
            threading.Thread(target=self.sync_data_with_servers, daemon=True).start()
        
        with self.data_lock:
            if user not in self.users:
                self.users.append(user)
                self.save("/app/data/users.json", self.users)

            self.user_logins.append({
                "user": user,
                "timestamp": data.get("timestamp"),
                "type": "login", 
                "clock": current_clock
            })
            self.save("/app/data/user_logins.json", self.user_logins)

        return {"status": "sucesso", "clock": current_clock}
    
    def handle_cluster_status(self, data):
        server_list = self.get_server_list()
        
        return {
            "current_server": self.server_name,
            "rank": self.rank,
            "coordinator": self.coordinator,
            "logical_clock": self.logical_clock,
            "physical_offset": self.clock_sync.physical_offset,
            "message_count": self.message_count,
            "servers_online": len(server_list) if server_list else 0,
            "data_stats": {
                "users": len(self.users),
                "channels": len(self.channels),
                "logins": len(self.user_logins),
                "messages": len(self.messages)
            },
            "is_coordinator": self.coordinator == self.server_name
        }

    def handle_users(self, data):
        self.message_count += 1
        if self.message_count >= 10:
            print("🎯 MENSAGEM 10 - Sincronização física!")
            self.clock_sync.synchronize_physical_clock()
            self.message_count = 0
        
        received_clock = data.get("clock", 0)
        current_clock = self.increment_clock(received_clock)
        
        return {"users": self.users, "clock": current_clock}

    def handle_channels(self, data):
        self.message_count += 1
        if self.message_count >= 10:
            self.clock_sync.synchronize_physical_clock()
            self.message_count = 0
        
        current_clock = self.increment_clock()
        return {"channels": self.channels, "clock": current_clock}

    def handle_channel(self, data):
        self.message_count += 1
        if self.message_count >= 10:
            self.clock_sync.synchronize_physical_clock()
            self.message_count = 0
        
        received_clock = data.get("clock", 0)
        current_clock = self.increment_clock(received_clock)
        
        name = data.get("channel")
        if name in self.channels:
            return {"status": "erro", "message": "canal existe", "clock": current_clock}
        
        self.channels.append(name)
        self.save("/app/data/channels.json", self.channels)
        return {"status": "sucesso", "clock": current_clock}

    def handle_publish(self, data):
        self.message_count += 1
        if self.message_count >= 10:
            self.clock_sync.synchronize_physical_clock()
            self.message_count = 0
        
        received_clock = data.get("clock", 0)
        current_clock = self.increment_clock(received_clock)
        
        channel = data["channel"]
        
        message_record = {
            "channel": channel,
            "message": data.get("message", ""),
            "user": data.get("user", ""),
            "timestamp": time.time(),
            "clock": current_clock,
            "server": self.server_name
        }
        self.messages.append(message_record)
        self.save("/app/data/messages.json", self.messages)
        
        msg_out = {
            "service": "publish",
            "data": data,
            "clock": current_clock
        }
        self.pub_socket.send_multipart([channel.encode(), msgpack.packb(msg_out)])
        
        return {"status": "OK", "clock": current_clock}

    def handle_message(self, data):
        self.message_count += 1
        if self.message_count >= 10:
            self.clock_sync.synchronize_physical_clock()
            self.message_count = 0
        
        received_clock = data.get("clock", 0)
        current_clock = self.increment_clock(received_clock)
        
        dst = data["dst"]
        msg_out = {
            "service": "message", 
            "data": data,
            "clock": current_clock
        }
        self.pub_socket.send_multipart([dst.encode(), msgpack.packb(msg_out)])
        
        return {"status": "OK", "clock": current_clock}
    
    def handle_election(self, data):
        print(f"\n🗳️ REQUISIÇÃO DE ELEIÇÃO recebida em {self.server_name}", flush=True)
        
        received_clock = data.get("clock", 0)
        current_clock = self.increment_clock(received_clock)
        
        from_rank = data.get("from_rank", 999)
        my_rank = int(self.rank) if self.rank else 999
        
        print(f"   📊 Rank do solicitante: {from_rank}, Meu rank: {my_rank}", flush=True)
        
        if from_rank > my_rank:
            print(f"   🚀 Rank {from_rank} > {my_rank}, iniciando minha própria eleição!", flush=True)
            threading.Thread(target=self.clock_sync.start_election, daemon=True).start()
        
        return {
            "election": "OK",
            "timestamp": time.time(),
            "clock": current_clock
        }
    
    def handle_clock(self, data):
        print(f"🕐 Requisição de clock recebida")
        
        received_clock = data.get("clock", 0)
        current_clock = self.increment_clock(received_clock)
        
        return {
            "time": time.time(),
            "timestamp": time.time(),
            "clock": current_clock
        }
    
    def handle_trigger_election(self, data):
        print(f"\n🎯 TRIGGER DE ELEIÇÃO recebido em {self.server_name}", flush=True)
        
        threading.Thread(target=self.clock_sync.start_election, daemon=True).start()
        
        return {
            "status": "election_triggered",
            "clock": self.increment_clock()
        }

    def declare_coordinator(self):
        self.coordinator = self.server_name
        
        coord_msg = {
            "service": "election",
            "data": {
                "coordinator": self.server_name,
                "timestamp": time.time(),
                "clock": self.increment_clock()
            }
        }
        
        print(f"\n📢 ANUNCIANDO: {self.server_name} é o novo coordenador!", flush=True)
        
        self.pub_socket.send_multipart([
            "servers".encode(),
            msgpack.packb(coord_msg)
        ])
        
        print(f"👑 {self.server_name} assumiu como coordenador!", flush=True)
    
    def handle_status(self, data):
        return {
            "server_name": self.server_name,
            "rank": self.rank,
            "coordinator": self.coordinator,
            "clock": self.logical_clock,
            "status": "online"
        }
    
    def sync_data_with_servers(self):
        try:
            print("🔄 Iniciando sincronização de dados com cluster...")
            
            server_list = self.get_server_list()
            if not server_list:
                print("❌ Nenhum servidor disponível para sincronização")
                return
            
            sync_data = {
                "users": self.users,
                "channels": self.channels, 
                "user_logins": self.user_logins[-50:],
                "messages": self.messages[-100:]
            }
            
            sync_msg = {
                "service": "data_sync",
                "data": {
                    "sync_data": sync_data,
                    "timestamp": time.time(),
                    "clock": self.increment_clock(),
                    "source": self.server_name
                }
            }
            
            for server_info in server_list:
                if server_info["name"] != self.server_name:
                    try:
                        sync_socket = self.context.socket(zmq.REQ)
                        sync_socket.setsockopt(zmq.RCVTIMEO, 3000)
                        sync_socket.connect(f"tcp://{server_info['host']}:{server_info['port']}")
                        
                        sync_socket.send(msgpack.packb(sync_msg))
                        resp = msgpack.unpackb(sync_socket.recv(), raw=False)
                        
                        print(f"✅ Dados sincronizados com {server_info['name']}")
                        sync_socket.close()
                        
                    except Exception as e:
                        print(f"❌ Erro ao sincronizar com {server_info['name']}: {e}")
        
        except Exception as e:
            print(f"💥 Erro na sincronização de dados: {e}")

    
    def handle_data_sync(self, data):
        print("📥 Recebendo sincronização de dados...")
        
        received_clock = data.get("clock", 0)
        current_clock = self.increment_clock(received_clock)
        sync_data = data.get("sync_data", {})
        source = data.get("source", "")
        
        with self.data_lock:
            for user in sync_data.get("users", []):
                if user not in self.users:
                    self.users.append(user)
            
            for channel in sync_data.get("channels", []):
                if channel not in self.channels:
                    self.channels.append(channel)
            
            for login in sync_data.get("user_logins", []):
                if login not in self.user_logins:
                    self.user_logins.append(login)
            
            for msg in sync_data.get("messages", []):
                if msg not in self.messages:
                    self.messages.append(msg)
            
            self.save("/app/data/users.json", self.users)
            self.save("/app/data/channels.json", self.channels)
            self.save("/app/data/user_logins.json", self.user_logins)
            self.save("/app/data/messages.json", self.messages)
        
        print(f"✅ Dados sincronizados de {source}")
        
        return {
            "status": "sync_ok", 
            "clock": current_clock,
            "received_items": {
                "users": len(sync_data.get("users", [])),
                "channels": len(sync_data.get("channels", [])),
                "logins": len(sync_data.get("user_logins", [])),
                "messages": len(sync_data.get("messages", []))
            }
        }
    
    def periodic_sync(self):
        while True:
            time.sleep(30)
            if self.coordinator == self.server_name:
                print("🔄 Coordenador iniciando sincronização periódica...")
                self.sync_data_with_servers()

    def handle_clock_sync(self, data):
        print("🕐 Coordenador: Processando requisição de sincronização Berkeley")
        
        received_clock = data.get("clock", 0)
        current_clock = self.increment_clock(received_clock)
        
        t1 = data["data"]["t1"]
        t2 = time.time()
        
        time.sleep(0.001)
        
        t3 = time.time()
        
        return {
            "t2": t2,
            "t3": t3,
            "clock": current_clock
        }


if __name__ == "__main__":
    server = ClusteredServer()
    server.run()