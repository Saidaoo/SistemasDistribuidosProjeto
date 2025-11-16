FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    pyzmq \
    msgpack

COPY broker.py .

EXPOSE 5550

CMD ["python", "broker.py"]