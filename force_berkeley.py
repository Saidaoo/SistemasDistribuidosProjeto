import zmq, msgpack, time

print("🎯 Forçando Sincronização Berkeley")
print("=" * 60)

ctx = zmq.Context()
s = ctx.socket(zmq.REQ)
s.setsockopt(zmq.RCVTIMEO, 5000)
s.connect('tcp://localhost:5555')
time.sleep(0.5)

print("📨 Enviando 10 mensagens consecutivas...")
for i in range(1, 11):
    msg = {
        "service": "channels",
        "data": {"clock": i}
    }
    s.send(msgpack.packb(msg))
    resp = s.recv()
    print(f"   Mensagem {i}/10 enviada")
    
    if i == 10:
        print("\n🎯 MENSAGEM 10 ENVIADA - Berkeley deve disparar AGORA!")

s.close()
ctx.term()

print("\n⏳ Aguarde 3 segundos e verifique os logs...")
time.sleep(3)
print("✅ Execute agora:")
print("   docker compose logs server_1 --tail 50 | Select-String 'Berkeley'")
print("   docker compose logs server_2 --tail 50 | Select-String 'Berkeley'")
print("   docker compose logs server_3 --tail 50 | Select-String 'Berkeley'")