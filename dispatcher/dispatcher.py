from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import json
from task import Task
import time

class Dispatcher:
    def __init__(self, host="0.0.0.0", port=8000, nameservice_url="http://nameservice:8001"):
        self.tasks = {}  # task_id -> Task
        self.nameservice = ServerProxy(nameservice_url, allow_none=True)
        self.server = SimpleXMLRPCServer((host, port), allow_none=True)
        self.server.register_function(self.send_task, "send_task")
        self.server.register_function(self.get_result, "get_result")
        self.server.register_function(self.receive_result, "receive_result")
        self.task_id_counter = 0

    def send_task(self, task_json):
        task = Task.from_json(task_json)
        self.task_id_counter += 1
        task.id = self.task_id_counter
        self.tasks[task.id] = task
        for _ in range(3):  # Bis zu 3 Versuche
            try:
                worker_url = self.nameservice.lookup_worker(task.type)
                if worker_url.startswith("Error"):
                    task.status = "failed"
                    task.result = worker_url
                    break
                worker = ServerProxy(f"http://{worker_url}", allow_none=True)
                worker.process_task(task.to_json())
                break
            except Exception as e:
                print(f"Error contacting worker: {e}")
                task.status = "failed"
                task.result = str(e)
                time.sleep(1)  # Warte vor Retry
        return task.id

    def get_result(self, task_id):
        task = self.tasks.get(task_id)
        if not task:
            return "Error: Task not found"
        return f"Result: {task.result}, Status: {task.status}"

    def receive_result(self, task_json):
        task = Task.from_json(task_json)
        if task.id in self.tasks:
            self.tasks[task.id] = task
            print(f"Received result for task {task.id}: {task.result}")
        return "Result received"

    def run(self):
        print("Starting dispatcher")
        self.server.serve_forever()

if __name__ == "__main__":
    dispatcher = Dispatcher()
    dispatcher.run()