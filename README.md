# Sistema BBS/IRC - Parte 1: Comunicação REQ-REP

## 📋 Funcionalidades Implementadas

### Serviços REQ-REP:
- 🔐 `login` - Autenticação de usuários
- 👥 `users` - Listagem de usuários cadastrados  
- ➕ `channel` - Criação de novos canais
- 📺 `channels` - Listagem de canais disponíveis

### Persistência:
- 💾 Usuários registrados
- 💾 Canais criados
- 💾 Histórico de logins
- 💾 Formato JSON

## 🚀 Como Executar

```bash
# 1. Iniciar servidor
docker-compose up -d --build

# 2. Conectar cliente (em outro terminal)
docker-compose run --rm cliente

# 3. Seguir menu interativo
