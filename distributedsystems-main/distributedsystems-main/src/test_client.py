import grpc
import taskgrid_pb2
import taskgrid_pb2_grpc
import json
import time
import logging
import random
import string
from datetime import datetime
import os

# Configure logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('taskgrid_tests.log'),
        logging.StreamHandler()
    ]
)

class TaskGridTestClient:
    def __init__(self, dispatcher_address):
        self.channel = grpc.insecure_channel(dispatcher_address)
        self.stub = taskgrid_pb2_grpc.ClientServiceStub(self.channel)
        self.logger = logging.getLogger("TaskGridTestClient")

    def generate_random_task(self):
        task_types = ["reverse", "sum", "hash", "upper", "wait"]
        task_type = random.choice(task_types)
        
        if task_type == "reverse":
            payload = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(5, 20)))
        elif task_type == "sum":
            numbers = [random.randint(1, 100) for _ in range(random.randint(2, 10))]
            payload = json.dumps(numbers)
        elif task_type == "hash":
            payload = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(10, 30)))
        elif task_type == "upper":
            payload = ''.join(random.choices(string.ascii_letters + string.digits + " ", k=random.randint(5, 20)))
        else:  # wait
            payload = str(random.uniform(0.5, 3.0))

        return task_type, payload

    def send_task(self, task_type, payload):
        try:
            response = self.stub.SendTask(
                taskgrid_pb2.SendTaskRequest(
                    type=task_type,
                    payload=payload
                )
            )
            self.logger.info(f"Task sent successfully. Task ID: {response.task_id}, Type: {task_type}, Payload: {payload}")
            return response.task_id
        except grpc.RpcError as e:
            self.logger.error(f"Failed to send task: {e}")
            raise

    def get_result(self, task_id, wait=True, timeout=30):
        start_time = time.time()
        while True:
            try:
                response = self.stub.RequestResult(
                    taskgrid_pb2.RequestResultRequest(task_id=task_id)
                )
                
                if response.task.status == "COMPLETED":
                    self.logger.info(f"Task {task_id} completed successfully. Result: {response.task.result}")
                    return response.task.result
                elif response.task.status == "FAILED":
                    self.logger.error(f"Task {task_id} failed: {response.task.result}")
                    raise Exception(f"Task failed: {response.task.result}")
                
                if not wait or (time.time() - start_time) > timeout:
                    return None
                    
                time.sleep(1)
                
            except grpc.RpcError as e:
                self.logger.error(f"Failed to get result: {e}")
                raise

def main():
    # Use Docker service name from environment variable
    dispatcher_address = os.getenv('DISPATCHER_ADDRESS', 'dispatcher:50052')
    client = TaskGridTestClient(dispatcher_address)
    
    print("Starting continuous task testing. Press Ctrl+C to stop.")
    print(f"Results are being logged to taskgrid_tests.log")
    
    try:
        while True:
            task_type, payload = client.generate_random_task()
            try:
                task_id = client.send_task(task_type, payload)
                result = client.get_result(task_id)
                
                # Additional statistics logging
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logging.info(f"TEST SUMMARY [{current_time}]:")
                logging.info(f"  Task Type: {task_type}")
                logging.info(f"  Input: {payload}")
                logging.info(f"  Output: {result}")
                logging.info("-" * 50)
                
            except Exception as e:
                logging.error(f"Error in test iteration: {e}")
            
            # Wait 30 seconds before next task
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\nTest client stopped by user.")
        
if __name__ == '__main__':
    main() 