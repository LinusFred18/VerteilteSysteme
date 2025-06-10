FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Generiere Python gRPC code aus der proto file
COPY proto/ /app/proto/
RUN python -m pip install grpcio-tools && \
    python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/taskgrid.proto

ENV PYTHONUNBUFFERED=1 