FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/ /app/proto/
RUN python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/taskgrid.proto

COPY src/worker.py .

ARG WORKER_TYPE
ARG PORT

ENV WORKER_TYPE=${WORKER_TYPE}
ENV PORT=${PORT}

EXPOSE ${PORT}

CMD ["sh", "-c", "python worker.py ${WORKER_TYPE} ${PORT}"] 