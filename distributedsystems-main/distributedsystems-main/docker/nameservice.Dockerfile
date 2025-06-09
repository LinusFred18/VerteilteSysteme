FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/ /app/proto/
RUN python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/taskgrid.proto

COPY src/nameservice.py .

EXPOSE 50051

CMD ["python", "nameservice.py"] 