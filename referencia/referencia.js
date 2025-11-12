// referencia.js
const zmq = require("zeromq");
const msgpack = require("@msgpack/msgpack"); // <-- Import do msgpack

// Configurações
const PORT = process.env.REFERENCE_PORT || "6000"; // Porta para servidores
const HEARTBEAT_INTERVAL = 5000; // ms para checar servidores ativos

// Lista de servidores { name, rank, last_seen }
let servers = [];

// Coordenador atual
let coordinator = null;

// Relógio lógico
let logicalClock = 0;

// Criar REP socket
const sock = new zmq.Reply();

async function startReference() {
  await sock.bind(`tcp://0.0.0.0:${PORT}`);
  console.log(`🕒 Servidor de referência rodando na porta ${PORT}`);

  for await (const [msg] of sock) {
    try {
      const message = msgpack.decode(msg); // <-- decodifica messagepack
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

// Handler para rank
async function handleRank(data, sock) {
  const { user } = data;
  let server = servers.find(s => s.name === user);
  if (!server) {
    const rank = servers.length + 1;
    server = { name: user, rank, last_seen: Date.now() };
    servers.push(server);
    console.log(`➕ Novo servidor adicionado: ${user}, rank ${rank}`);
  } else {
    server.last_seen = Date.now();
    console.log(`♻️ Servidor existente atualizado: ${user}`);
  }

  const reply = {
    service: "rank",
    data: { rank: server.rank, timestamp: Date.now(), clock: logicalClock }
  };
  console.log("📤 Enviando resposta rank:", reply);
  await sock.send(msgpack.encode(reply)); // <-- envia como MessagePack
}

// Handler para list
async function handleList(data, sock) {
  const reply = {
    service: "list",
    data: { list: servers.map(s => ({ name: s.name, rank: s.rank })), timestamp: Date.now(), clock: logicalClock }
  };
  console.log("📤 Enviando lista de servidores:", reply);
  await sock.send(msgpack.encode(reply));
}

// Handler para heartbeat
async function handleHeartbeat(data, sock) {
  const { user } = data;
  const server = servers.find(s => s.name === user);
  if (server) {
    server.last_seen = Date.now();
    console.log(`♻️ Heartbeat recebido de servidor existente: ${user}`);
  } else {
    servers.push({ name: user, rank: servers.length + 1, last_seen: Date.now() });
    console.log(`➕ Heartbeat recebido de servidor desconhecido: ${user}`);
  }

  const reply = { service: "heartbeat", data: { timestamp: Date.now(), clock: logicalClock } };
  console.log("📤 Enviando resposta heartbeat:", reply);
  await sock.send(msgpack.encode(reply));
}

// Handler para election
async function handleElection(data, sock) {
  const { coordinator: newCoord } = data;
  if (newCoord) {
    coordinator = newCoord;
    console.log(`👑 Novo coordenador definido: ${coordinator}`);
  }

  const reply = { service: "election", data: { election: "OK", timestamp: Date.now(), clock: logicalClock } };
  console.log("📤 Enviando resposta election:", reply);
  await sock.send(msgpack.encode(reply));
}

// Função para remover servidores inativos
function cleanupServers() {
  const now = Date.now();
  servers = servers.filter(s => now - s.last_seen <= HEARTBEAT_INTERVAL * 2);
  console.log("🧹 Servidores ativos após limpeza:", servers.map(s => s.name));
}

// Roda a limpeza periodicamente
setInterval(cleanupServers, HEARTBEAT_INTERVAL);

// Inicia servidor
startReference();
