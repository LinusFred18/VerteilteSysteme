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

# Das ist die Dispatcher Klasse. 
# Sie verwaltet die task_queue, die speichert welche tasks zum ausführen an die worker verteilt werden müssen
# Sie verteilt die Aufgaben an die Worker.
# Sie kommuniziert mit dem Client indem sie Anfragen von ihm in die task_queue aufnimmt und die Ergebnisse an den Client zurück schickt.
# Sie läuft mit gRPC auf Port 50052
class TaskDispatcher(taskgrid_pb2_grpc.ClientServiceServicer):
    def __init__(self, nameservice_address):
        self.tasks = {}  # key: task_id | value: Task
        self.task_queue = Queue()
        self.pending_tasks = set()
        self.task_counter = 0
        self.nameservice_address = nameservice_address
        self.logger = logging.getLogger("Dispatcher")
        self.task_stats = defaultdict(lambda: {
            'times': [],  # Liste der letzten 100 process zeiten
            'max_time': 0.0,
            'last_time': 0.0
        })
        self.task_retries = {}  # key: task_id | value: retry_count
        
        # seperater thread für das dispatchen
        self.dispatcher_thread = threading.Thread(target=self._dispatch_tasks)
        self.dispatcher_thread.daemon = True
        self.dispatcher_thread.start()
    
    # Senden eines tasks
    def SendTask(self, request, context):
        # Überprüfe ob es einen worker für den task gibt
        try:
            with grpc.insecure_channel(self.nameservice_address) as channel:
                nameservice = taskgrid_pb2_grpc.NameServiceStub(channel)
                lookup_response = nameservice.LookupWorker(
                    taskgrid_pb2.LookupWorkerRequest(type=request.type)
                )
                if not lookup_response.address: # kein worker verfügbar
                    task_id = self.task_counter
                    self.task_counter += 1
                    
                    task = taskgrid_pb2.Task(
                        id=task_id,
                        type=request.type,
                        payload=request.payload,
                        status="FAILED",
                        result="No worker available for task type: " + request.type,
                        timestamp_created=int(time.time()),
                        timestamp_completed=int(time.time())
                    )
                    self.tasks[task_id] = task
                    self.logger.warning(f"No worker available for task {task_id} of type {request.type}")
                    return taskgrid_pb2.SendTaskResponse(task_id=task_id)
        except grpc.RpcError as e:
            # Nameservice error, wird wie kein worker gefuden behandelt
            task_id = self.task_counter
            self.task_counter += 1
            
            task = taskgrid_pb2.Task(
                id=task_id,
                type=request.type,
                payload=request.payload,
                status="FAILED",
                result="Service unavailable: Could not contact nameservice",
                timestamp_created=int(time.time()),
                timestamp_completed=int(time.time())
            )
            self.tasks[task_id] = task
            self.logger.error(f"Nameservice error for task type {request.type}: {e}")
            return taskgrid_pb2.SendTaskResponse(task_id=task_id)

        # worker verfügbar
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
        self.pending_tasks.add(task_id)  # task ist pending
        self.task_queue.put(task)
        self.logger.info(f"Received task {task_id} of type {request.type}")
        
        return taskgrid_pb2.SendTaskResponse(task_id=task_id)

    # Anfordern der Ergebnisse eines Tasks 
    def RequestResult(self, request, context):
        task_id = request.task_id
        if task_id not in self.tasks:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Task {task_id} not found")
            return taskgrid_pb2.RequestResultResponse()
            
        return taskgrid_pb2.RequestResultResponse(task=self.tasks[task_id])

    # Statistiken über die ausgeführten tasks für das monitoring
    def GetTaskStats(self, request, context):
        # Aktualisiere pending tasks
        actual_pending = set()
        for task_id, task in self.tasks.items():
            if task.status == "PENDING":
                actual_pending.add(task_id)
        
        self.pending_tasks = actual_pending
        
        task_stats = {}
        for task_type, stats in self.task_stats.items():
            times = stats['times']
            if times:
                avg_time = sum(times) / len(times)
                task_stats[task_type] = taskgrid_pb2.TaskTimingStats(
                    avg_time=avg_time,
                    max_time=stats['max_time'],
                    last_time=stats['last_time'],
                    recent_times=times[-100:]  # Letzten 100 statistiken
                )
        
        return taskgrid_pb2.TaskStatsResponse(
            pending_tasks=len(self.pending_tasks),
            task_stats=task_stats,
            task_counter=self.task_counter
        )
    
    # der loop der den workern die tasks zuweißt und dabei die anderen Funktionen aufruft
    def _dispatch_tasks(self):
        while True:
            task = self.task_queue.get()
            retry_count = self.task_retries.get(task.id, 0)
            
            try:
                # Lookup worker
                with grpc.insecure_channel(self.nameservice_address) as channel:
                    nameservice = taskgrid_pb2_grpc.NameServiceStub(channel)
                    lookup_response = nameservice.LookupWorker(
                        taskgrid_pb2.LookupWorkerRequest(type=task.type)
                    )
                    worker_address = lookup_response.address
                    
                    # Send task an worker mit timeout
                    with grpc.insecure_channel(worker_address) as worker_channel:
                        worker = taskgrid_pb2_grpc.WorkerServiceStub(worker_channel)
                        try:
                            response = worker.ProcessTask(task, timeout=10)  # 10s timeout
                            
                            task.result = response.result
                            task.status = "COMPLETED" if response.success else "FAILED"
                            task.timestamp_completed = int(time.time())
                            
                            if task.id in self.pending_tasks:
                                self.pending_tasks.remove(task.id)
                            
                            # Update processing time Statistik
                            if response.success:
                                processing_time = task.timestamp_completed - task.timestamp_created
                                stats = self.task_stats[task.type]
                                stats['last_time'] = processing_time
                                stats['max_time'] = max(stats['max_time'], processing_time)
                                stats['times'].append(processing_time)
                                if len(stats['times']) > 100:
                                    stats['times'] = stats['times'][-100:]
                                
                                if task.id in self.task_retries:
                                    del self.task_retries[task.id]
                                    
                            self.logger.info(f"Task {task.id} completed with status {task.status}")
                            
                        except grpc.RpcError as e:
                            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                                # Worker hat zu lange gebraucht
                                try:
                                    nameservice.DeregisterWorker(
                                        taskgrid_pb2.DeregisterWorkerRequest(address=worker_address)
                                    )
                                except Exception as dereg_e:
                                    self.logger.warning(f"Failed to mark worker as inactive: {dereg_e}")
                                raise
                            else:
                                raise
                            
            except grpc.RpcError as e:
                error_str = str(e)
                self.logger.error(f"Failed to process task {task.id}: {e}")
                
                # retries
                max_retries = 5
                if retry_count < max_retries:
                    backoff_time = min(2 ** retry_count, 30)  # Max 30s
                    self.task_retries[task.id] = retry_count + 1
                    self.logger.info(f"Requeuing task {task.id} (retry {retry_count + 1}) after {backoff_time}s")
                    time.sleep(backoff_time)
                    self.task_queue.put(task)
                else:
                    # alle retries fehlgeschlagen
                    task.status = "FAILED"
                    task.result = f"Error: {error_str} (max retries reached)"
                    task.timestamp_completed = int(time.time())
                    if task.id in self.pending_tasks:
                        self.pending_tasks.remove(task.id)
                    if task.id in self.task_retries:
                        del self.task_retries[task.id]
                    self.logger.error(f"Task {task.id} failed after {max_retries} retries.")

# Starten des RPC Servers
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