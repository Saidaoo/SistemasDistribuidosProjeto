# Sistema BBS/IRC - Parte 2: Comunicação PUB-SUB

## 📋 Funcionalidades Implementadas

### Serviços PUB-SUB:
- 📢 `publish` - Publicação de mensagens em canais
- 💌 `message` - Mensagens diretas entre usuários
- 🔄 Proxy dedicado - Roteamento de mensagens

### Serviços REQ-REP (mantidos):
- 🔐 `login` - Autenticação de usuários
- 👥 `users` - Listagem de usuários cadastrados  
- ➕ `channel` - Criação de novos canais
- 📺 `channels` - Listagem de canais disponíveis

### Persistência:
- 💾 Mensagens trocadas (novo)
- 💾 Usuários registrados
- 💾 Canais criados
- 💾 Histórico de logins
- 💾 Formato JSON

## 🚀 Como Executar

```bash
# 1. Iniciar servidor e proxy
docker-compose up -d --build

# 2. Conectar cliente (em outro terminal)
docker-compose run --rm cliente

# 3. Seguir menu interativo
