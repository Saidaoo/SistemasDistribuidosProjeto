const zmq = require("zeromq");
const msgpack = require("@msgpack/msgpack");

const PORT = process.env.REFERENCE_PORT || "6000";
const HEARTBEAT_INTERVAL = 5000;

let servers = [];
let coordinator = null;
let logicalClock = 0;

const FIXED_RANKS = {
  "server_1": 1,
  "server_2": 2,
  "server_3": 3
};

const sock = new zmq.Reply();

async function startReference() {
  await sock.bind(`tcp://0.0.0.0:${PORT}`);
  console.log(`🕒 Servidor de referência rodando na porta ${PORT}`);
  console.log(`📋 Ranks fixos configurados:`, FIXED_RANKS);

  for await (const [msg] of sock) {
    try {
      const message = msgpack.decode(msg);
      const service = message.service;
      const data = message.data;

      logicalClock = Math.max(logicalClock, data.clock || 0) + 1;

      console.log(`📨 Recebido serviço: ${service}, dados:`, data);

      switch (service) {
        case "rank":
          await handleRank(data, sock);
          break;
        case "list":
          await handleList(data, sock);
          break;
        case "heartbeat":
          await handleHeartbeat(data, sock);
          break;
        case "election":
          await handleElection(data, sock);
          break;
        default:
          await sock.send(msgpack.encode({
            service: "error",
            data: { message: "Serviço desconhecido", timestamp: Date.now(), clock: logicalClock }
          }));
      }
    } catch (err) {
      console.error("Erro processando mensagem:", err);
    }
  }
}

async function handleRank(data, sock) {
  const { user } = data;
  
  const fixedRank = FIXED_RANKS[user];
  
  if (!fixedRank) {
    console.log(`❌ Servidor ${user} não tem rank fixo definido!`);
    const reply = {
      service: "rank",
      data: { 
        rank: 999,
        timestamp: Date.now(), 
        clock: logicalClock,
        error: "Servidor não cadastrado"
      }
    };
    await sock.send(msgpack.encode(reply));
    return;
  }
  
  let server = servers.find(s => s.name === user);
  
  if (!server) {
    server = { name: user, rank: fixedRank, last_seen: Date.now() };
    servers.push(server);
    console.log(`➕ Novo servidor adicionado: ${user}, rank FIXO ${fixedRank}`);
  } else {
    server.last_seen = Date.now();
    server.rank = fixedRank;
    console.log(`♻️ Servidor existente atualizado: ${user}, rank ${fixedRank}`);
  }

  const reply = {
    service: "rank",
    data: { rank: server.rank, timestamp: Date.now(), clock: logicalClock }
  };
  console.log("📤 Enviando resposta rank:", reply);
  await sock.send(msgpack.encode(reply)); 
}

async function handleList(data, sock) {
  const reply = {
    service: "list",
    data: { 
      list: servers.map(s => ({ name: s.name, rank: s.rank })), 
      timestamp: Date.now(), 
      clock: logicalClock 
    }
  };
  console.log("📤 Enviando lista de servidores:", reply);
  await sock.send(msgpack.encode(reply));
}

async function handleHeartbeat(data, sock) {
  const { user } = data;
  const server = servers.find(s => s.name === user);
  
  if (server) {
    server.last_seen = Date.now();
    console.log(`💓 Heartbeat recebido de servidor existente: ${user} (rank ${server.rank})`);
  } else {
    const fixedRank = FIXED_RANKS[user] || 999;
    servers.push({ name: user, rank: fixedRank, last_seen: Date.now() });
    console.log(`➕ Heartbeat recebido de servidor desconhecido: ${user} (rank fixo ${fixedRank})`);
  }

  const reply = { 
    service: "heartbeat", 
    data: { timestamp: Date.now(), clock: logicalClock } 
  };
  await sock.send(msgpack.encode(reply));
}

async function handleElection(data, sock) {
  const { coordinator: newCoord } = data;
  if (newCoord) {
    coordinator = newCoord;
    console.log(`👑 Novo coordenador definido: ${coordinator}`);
    
    const coordServer = servers.find(s => s.name === coordinator);
    if (coordServer) {
      console.log(`   Rank do coordenador: ${coordServer.rank}`);
    }
  }

  const reply = { 
    service: "election", 
    data: { 
      election: "OK", 
      coordinator: coordinator,
      timestamp: Date.now(), 
      clock: logicalClock 
    } 
  };
  console.log("📤 Enviando resposta election:", reply);
  await sock.send(msgpack.encode(reply));
}

function cleanupServers() {
  const now = Date.now();
  const beforeCount = servers.length;
  servers = servers.filter(s => now - s.last_seen <= HEARTBEAT_INTERVAL * 2);
  
  if (servers.length < beforeCount) {
    console.log(`🧹 Removidos ${beforeCount - servers.length} servidor(es) inativo(s)`);
  }
  
  console.log("🧹 Servidores ativos:", servers.map(s => `${s.name}(rank ${s.rank})`).join(", "));
}

setInterval(cleanupServers, HEARTBEAT_INTERVAL);

startReference();