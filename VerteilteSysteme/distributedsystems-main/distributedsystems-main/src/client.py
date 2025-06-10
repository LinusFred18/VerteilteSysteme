import grpc
import taskgrid_pb2
import taskgrid_pb2_grpc
import json
import time
import logging

logging.basicConfig(level=logging.INFO)

# Das ist die Client Klasse.
# Sie sendet die Tasks an den Dispatcher und empfängt die Antwort dann auch wieder.
class TaskGridClient:
    def __init__(self, dispatcher_address):
        self.channel = grpc.insecure_channel(dispatcher_address)
        self.stub = taskgrid_pb2_grpc.ClientServiceStub(self.channel)
        self.logger = logging.getLogger("TaskGridClient")

    # sendete eine task (an den dispatcher, bei uns)
    def send_task(self, task_type, payload):
        try:
            response = self.stub.SendTask(
                taskgrid_pb2.SendTaskRequest(
                    type=task_type,
                    payload=payload
                )
            )
            self.logger.info(f"Task sent successfully. Task ID: {response.task_id}")
            return response.task_id
        except grpc.RpcError as e:
            self.logger.error(f"Failed to send task: {e}")
            raise
    # frägt die Ergebnisse der tasks ab
    def get_result(self, task_id, wait=True, timeout=30):
        start_time = time.time()
        while True:
            try:
                response = self.stub.RequestResult(
                    taskgrid_pb2.RequestResultRequest(task_id=task_id)
                )
                
                if response.task.status == "COMPLETED":
                    self.logger.info(f"Task {task_id} completed successfully")
                    return response.task.result
                elif response.task.status == "FAILED":
                    self.logger.error(f"Task {task_id} failed: {response.task.result}")
                    raise Exception(f"Task failed: {response.task.result}")
                
                if not wait or (time.time() - start_time) > timeout:
                    return None
                    
                time.sleep(1)  # Poll every second
                
            except grpc.RpcError as e:
                self.logger.error(f"Failed to get result: {e}")
                raise

#wird einmal aufgerufen um zu schauen ob alles die Beispiele funktioniert
def main():
    import os
    dispatcher_address = os.getenv('DISPATCHER_ADDRESS', 'localhost:50052')
    client = TaskGridClient(dispatcher_address)
    
    # Beispielhafte tasks
    tasks = [
        ("reverse", "Hello, World!"),
        ("sum", json.dumps([1, 2, 3, 4, 5])),
        ("hash", "secret message"),
        ("upper", "convert this to uppercase"),
        ("wait", "2")
    ]
    
    for task_type, payload in tasks:
        try:
            task_id = client.send_task(task_type, payload)
            print(f"\nSending {task_type} task...")
            result = client.get_result(task_id)
            print(f"Result: {result}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == '__main__':
    main() 