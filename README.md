# Sistema BBS/IRC - Parte 2 (PUB-SUB)

Extensão do sistema com comunicação em tempo real via PUB-SUB.

## 🚀 Como Usar

```bash
# Iniciar sistema
docker-compose up -d

# Conectar usuário
docker-compose run --rm cliente

📋 Funcionalidades
🔄 PUB-SUB (Novo)
Publicar em canais - Mensagens em tempo real

Mensagens diretas - Chat privado entre usuários

Proxy dedicado - Roteamento de mensagens

✅ REQ-REP (Parte 1)
Login de usuários

Listar usuários/canais

Criar canais

Persistência em JSON

📦 Serviços
servidor (5555) - REQ-REP + PUB

proxy (5557/5558) - Roteamento PUB-SUB

cliente - Interface interativa

💾 Dados
Usuários, canais, logins e mensagens persistidos em JSON

Histórico completo de conversas

Parte 1: feature/reqrep
Repositório: https://github.com/Saidaoo/SistemasDistribuidosProjeto
