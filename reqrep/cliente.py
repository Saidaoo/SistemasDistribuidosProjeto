import zmq
import json
import time

def send_request(socket, service, data):
    message = {"service": service, "data": data}
    socket.send_string(json.dumps(message))         # envia como string JSON
    reply_bytes = socket.recv()                      # recebe bytes
    reply = json.loads(reply_bytes.decode())         # decodifica para dict
    return reply

def main():
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://servidor:5555")

    print("🟡 Cliente conectado ao servidor.")

    while True:
        print("\n==== MENU ====")
        print("1 - Login")
        print("2 - Listar usuários")
        print("3 - Criar canal")
        print("4 - Listar canais")
        print("5 - Sair")
        choice = input("Escolha uma opção: ").strip()

        if choice == "1":
            user = input("Digite seu nome de usuário: ").strip()
            login_data = {"user": user, "timestamp": time.time()}
            reply = send_request(socket, "login", login_data)
            if reply["data"]["status"] == "sucesso":
                print(f"✅ Login bem-sucedido: {user}")
            else:
                print(f"⚠️ Login falhou: {reply['data']['description']}")

        elif choice == "2":
            reply = send_request(socket, "users", {"timestamp": time.time()})
            users_list = ", ".join(reply["data"]["users"])
            print(f"👥 Usuários cadastrados: {users_list}")

        elif choice == "3":
            canal = input("Digite o nome de um novo canal: ").strip()
            reply = send_request(socket, "channel", {"channel": canal, "timestamp": time.time()})
            if reply["data"]["status"] == "sucesso":
                print(f"✅ Canal criado: {canal}")
            else:
                print(f"⚠️ Channel falhou: {reply['data']['description']}")

        elif choice == "4":
            reply = send_request(socket, "channels", {"timestamp": time.time()})
            channels_list = ", ".join(reply["data"]["users"])
            print(f"#️⃣ Canais disponíveis: {channels_list}")

        elif choice == "5":
            print("👋 Saindo...")
            break
        else:
            print("❌ Opção inválida. Tente novamente.")

if __name__ == "__main__":
    main()
