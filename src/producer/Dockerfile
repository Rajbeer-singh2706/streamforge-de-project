FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Shared library first (layer-cached independently of producer code)
COPY src/shared/ ./shared/

COPY src/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/producer/ ./producer/

CMD ["python", "producer/producer.py", "--mode", "simulate"]