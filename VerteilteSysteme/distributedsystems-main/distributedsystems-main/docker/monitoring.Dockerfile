FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt flask

COPY proto/ /app/proto/
RUN python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/taskgrid.proto

COPY src/monitoring.py .

EXPOSE 50053
EXPOSE 8080

CMD ["python", "monitoring.py"] 