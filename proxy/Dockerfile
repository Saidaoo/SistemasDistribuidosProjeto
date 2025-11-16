FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir pyzmq

COPY proxy.py .

EXPOSE 5557 5558

CMD ["python", "proxy.py"]