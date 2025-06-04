import grpc
from concurrent import futures
import taskgrid_pb2
import taskgrid_pb2_grpc
import time
import logging
from collections import defaultdict
from queue import Queue
import threading

logging.basicConfig(level=logging.INFO)

class TaskDispatcher(taskgrid_pb2_grpc.ClientServiceServicer):
    def __init__(self, nameservice_address):
        self.tasks = {}  # task_id -> Task
        self.task_queue = Queue()
        self.task_counter = 0
        self.nameservice_address = nameservice_address
        self.logger = logging.getLogger("Dispatcher")
        self.processing_times = defaultdict(list)
        
        # Start task dispatcher thread
        self.dispatcher_thread = threading.Thread(target=self._dispatch_tasks)
        self.dispatcher_thread.daemon = True
        self.dispatcher_thread.start()

    def SendTask(self, request, context):
        task_id = self.task_counter
        self.task_counter += 1
        
        task = taskgrid_pb2.Task(
            id=task_id,
            type=request.type,
            payload=request.payload,
            status="PENDING",
            timestamp_created=int(time.time())
        )
        
        self.tasks[task_id] = task
        self.task_queue.put(task)
        self.logger.info(f"Received task {task_id} of type {request.type}")
        
        return taskgrid_pb2.SendTaskResponse(task_id=task_id)

    def RequestResult(self, request, context):
        task_id = request.task_id
        if task_id not in self.tasks:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Task {task_id} not found")
            return taskgrid_pb2.RequestResultResponse()
            
        return taskgrid_pb2.RequestResultResponse(task=self.tasks[task_id])

    def _dispatch_tasks(self):
        while True:
            task = self.task_queue.get()
            try:
                # Lookup worker
                with grpc.insecure_channel(self.nameservice_address) as channel:
                    nameservice = taskgrid_pb2_grpc.NameServiceStub(channel)
                    lookup_response = nameservice.LookupWorker(
                        taskgrid_pb2.LookupWorkerRequest(type=task.type)
                    )
                    
                    # Send task to worker
                    with grpc.insecure_channel(lookup_response.address) as worker_channel:
                        worker = taskgrid_pb2_grpc.WorkerServiceStub(worker_channel)
                        response = worker.ProcessTask(task)
                        
                        # Update task with result
                        task.result = response.result
                        task.status = "COMPLETED" if response.success else "FAILED"
                        task.timestamp_completed = int(time.time())
                        
                        # Update processing time statistics
                        processing_time = task.timestamp_completed - task.timestamp_created
                        self.processing_times[task.type].append(processing_time)
                        
                        self.logger.info(f"Task {task.id} completed with status {task.status}")
                        
            except grpc.RpcError as e:
                task.status = "FAILED"
                task.result = f"Error: {str(e)}"
                self.logger.error(f"Failed to process task {task.id}: {e}")

def serve(nameservice_address):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dispatcher = TaskDispatcher(nameservice_address)
    taskgrid_pb2_grpc.add_ClientServiceServicer_to_server(dispatcher, server)
    server.add_insecure_port('[::]:50052')
    server.start()
    logging.info("Dispatcher started on port 50052")
    server.wait_for_termination()

if __name__ == '__main__':
    import os
    nameservice_address = os.getenv('NAMESERVICE_ADDRESS', 'localhost:50051')
    serve(nameservice_address) 