# Dockerfile do proxy
FROM python:3.13-slim

# Diretório de trabalho dentro do container
WORKDIR /app

# Copia todo o conteúdo da pasta proxy para o container
COPY . .

# Instala a biblioteca necessária
RUN pip install pyzmq

# Comando para rodar o proxy
CMD ["python", "proxy.py"]
