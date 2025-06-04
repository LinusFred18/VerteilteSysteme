import grpc
from concurrent import futures
import taskgrid_pb2
import taskgrid_pb2_grpc
import time
import logging
import hashlib
import json

logging.basicConfig(level=logging.INFO)

class Worker(taskgrid_pb2_grpc.WorkerServiceServicer):
    def __init__(self, worker_type, nameservice_address):
        self.type = worker_type
        self.nameservice_address = nameservice_address
        self.logger = logging.getLogger(f"Worker-{worker_type}")
        self.task_processors = {
            "reverse": self._process_reverse,
            "sum": self._process_sum,
            "hash": self._process_hash,
            "upper": self._process_upper,
            "wait": self._process_wait
        }

    def ProcessTask(self, request, context):
        self.logger.info(f"Processing task {request.id} of type {request.type}")
        
        if request.type not in self.task_processors:
            return taskgrid_pb2.ProcessTaskResponse(
                success=False,
                result=f"Unsupported task type: {request.type}"
            )
            
        try:
            result = self.task_processors[request.type](request.payload)
            return taskgrid_pb2.ProcessTaskResponse(
                success=True,
                result=str(result)
            )
        except Exception as e:
            self.logger.error(f"Error processing task {request.id}: {e}")
            return taskgrid_pb2.ProcessTaskResponse(
                success=False,
                result=f"Error: {str(e)}"
            )

    def _process_reverse(self, payload):
        return payload[::-1]

    def _process_sum(self, payload):
        try:
            numbers = json.loads(payload)
            if not isinstance(numbers, list):
                raise ValueError("Payload must be a JSON array of numbers")
            return sum(float(n) for n in numbers)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON payload")

    def _process_hash(self, payload):
        return hashlib.sha256(payload.encode()).hexdigest()

    def _process_upper(self, payload):
        return payload.upper()

    def _process_wait(self, payload):
        try:
            seconds = float(payload)
            time.sleep(seconds)
            return f"Waited for {seconds} seconds"
        except ValueError:
            raise ValueError("Payload must be a number representing seconds to wait")

    def register_with_nameservice(self, address):
        while True:
            try:
                with grpc.insecure_channel(self.nameservice_address) as channel:
                    nameservice = taskgrid_pb2_grpc.NameServiceStub(channel)
                    response = nameservice.RegisterWorker(
                        taskgrid_pb2.RegisterWorkerRequest(
                            type=self.type,
                            address=address
                        )
                    )
                    if response.success:
                        self.logger.info(f"Successfully registered with nameservice")
                        break
            except grpc.RpcError as e:
                self.logger.error(f"Failed to register with nameservice: {e}")
                time.sleep(5)  # Retry after 5 seconds

def serve(worker_type, port, nameservice_address):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    worker = Worker(worker_type, nameservice_address)
    taskgrid_pb2_grpc.add_WorkerServiceServicer_to_server(worker, server)
    
    # Use container name for registration
    container_name = f"worker-{worker_type}"
    address = f'{container_name}:{port}'
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    # Register with nameservice using container name
    worker.register_with_nameservice(address)
    
    logging.info(f"Worker of type {worker_type} started on port {port}")
    server.wait_for_termination()

if __name__ == '__main__':
    import os
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python worker.py <worker_type> <port>")
        sys.exit(1)
        
    worker_type = sys.argv[1]
    port = int(sys.argv[2])
    nameservice_address = os.getenv('NAMESERVICE_ADDRESS', 'localhost:50051')
    
    serve(worker_type, port, nameservice_address) 