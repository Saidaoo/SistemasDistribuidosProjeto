# pubsub/Dockerfile
FROM python:3.9-slim

WORKDIR /app

RUN pip install pyzmq

COPY publisher.py .
COPY subscriber.py .

# O comando será definido no docker-compose.yml