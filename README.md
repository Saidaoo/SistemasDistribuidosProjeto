# Sistema BBS Distribuído - Sistemas Distribuídos FEI

Sistema de Bulletin Board System (BBS) distribuído implementando conceitos de sistemas distribuídos.

## 🏗️ Arquitetura

### Componentes:
- **3 Servidores** (Python): Processam requisições com replicação
- **Broker** (Python): Load balancer ROUTER-DEALER (porta 5550)
- **Proxy** (Python): PUB/SUB proxy XSUB-XPUB (portas 5557-5558)
- **Servidor de Referência** (Node.js): Gerencia ranks e coordenador (porta 6000)
- **Cliente** (C): Interface de usuário com sockets REQ/SUB
- **2 Bots** (Python): Agentes autônomos que interagem no sistema

### Portas:
- Broker: 5550 (REQ/REP)
- Proxy: 5557 (Publishers), 5558 (Subscribers)
- Servidor de Referência: 6000
- Server 1: 5555
- Server 2: 5556
- Server 3: 5559

## 🚀 Como Executar

### Requisitos:
- Docker
- Docker Compose

### Iniciar sistema:
```bash
docker compose up
```

### Parar sistema:
```bash
docker compose down
```

## 🧪 Testes Implementados

### Teste de Eleição (Algoritmo Bully):
```bash
python test_election.py
```
**Valida:** Servidor com menor rank é eleito coordenador.

### Teste de Relógio Lógico (Lamport):
```bash
python test_logical_clock.py
```
**Valida:** Incremento e sincronização de relógios lógicos.

### Teste de Sincronização Berkeley:
```bash
python test_berkeley.py
```
**Valida:** Sincronização física de relógios entre servidores.

### Teste de Sincronização de Dados:
```bash
python test_data_sync.py
```
**Valida:** Replicação de dados entre servidores.

## 📋 Funcionalidades Implementadas

### ✅ Cliente:
- Relógio lógico de Lamport
- Comunicação via REQ/REP (comandos)
- Comunicação via PUB/SUB (mensagens em tempo real)
- Login, listagem de usuários/canais
- Envio de mensagens públicas e diretas

### ✅ Servidores:
- **Relógio Lógico:** Algoritmo de Lamport implementado
- **Eleição de Coordenador:** Algoritmo Bully
- **Sincronização Berkeley:** Sincronização física de relógios
- **Replicação de Dados:** Sincronização automática entre servidores
- **Persistência:** Dados salvos em JSON

### ✅ Bots:
- Comunicação PUB/SUB
- Envio automático de mensagens
- Resposta a mensagens de usuários
- Troca automática de canais

## 🔧 Tecnologias

- **C**: Cliente
- **Python**: Servidores, Broker, Proxy, Bots
- **Node.js**: Servidor de Referência
- **ZeroMQ**: Comunicação entre processos
- **MessagePack**: Serialização de mensagens
- **Docker**: Containerização

## 📊 Validação

Todos os critérios de avaliação foram implementados e testados:
- ✅ Cliente: Bibliotecas corretas + Relógio Lógico
- ✅ Bot: Bibliotecas corretas + Padrão de mensagens
- ✅ Broker/Proxy/Referência: Todos funcionando
- ✅ Servidor: Relógio Lógico + Eleição + Sincronizações
- ✅ Testes automatizados validam todas as funcionalidades

## 👥 Autores

Nome: Vinícius Saidi de Araújo Soares 
R.A: 22.122.064-3

Novembro 2025 - FEI
