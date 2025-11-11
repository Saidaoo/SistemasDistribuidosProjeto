# Imagem base
FROM python:3.13-slim

# Diretório de trabalho dentro do container
WORKDIR /app

# Copia todos os arquivos da pasta pubsub para o container
COPY . .

# Instala a biblioteca necessária
RUN pip install pyzmq

# Comando padrão para rodar o publisher
# Troque para subscriber.py se quiser rodar o subscriber
CMD ["python", "publisher.py"]
