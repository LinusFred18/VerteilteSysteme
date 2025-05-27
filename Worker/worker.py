import socket
import json
import hashlib
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import threading
import sys

class Worker:
    def __init__(self, task_type, worker_port, naming_service_url, dispatcher_host):
        self.task_type = task_type
        self.worker_port = worker_port
        self.naming_service_url = naming_service_url
        self.dispatcher_host = dispatcher_host
        self.dispatcher_port = 8001  # Muss mit dem Dispatcher übereinstimmen
        self.hostname = socket.gethostname()
        self.address = f"http://{self.hostname}:{self.worker_port}"

    def register(self):
        try:
            proxy = xmlrpc.client.ServerProxy(self.naming_service_url)
            result = proxy.register_worker(self.task_type, self.address)
            print(f"[INFO] Registrierung beim Namensdienst: {result}")
        except Exception as e:
            print(f"[ERROR] Registrierung fehlgeschlagen: {e}")
            raise

    def receive_task(self, task):
        print(f"[INFO] Aufgabe empfangen: {task}")
        if task.get("type") != self.task_type:
            return "[ERROR] Unzuständiger Worker"

        task_id = task.get("id")
        result = self.process_task(task.get("payload"))
        self.send_result(task_id, result)
        return True

    def process_task(self, payload):
        try:
            if self.task_type == "reverse":
                return payload[::-1]

            elif self.task_type == "sum":
                return sum(map(int, payload.split()))

            elif self.task_type == "hash":
                return hashlib.sha256(payload.encode()).hexdigest()

            elif self.task_type == "upper":
                return payload.upper()

            elif self.task_type == "wait":
                time.sleep(7)
                return "done"

            elif self.task_type == "length":
                return len(payload)

            elif self.task_type == "average":
                numbers = list(map(int, payload.split()))
                return sum(numbers) / len(numbers) if numbers else 0

            elif self.task_type == "prime":
                n = int(payload)
                if n < 2:
                    return False
                for i in range(2, int(n ** 0.5) + 1):
                    if n % i == 0:
                        return False
                return True

            else:
                return "[ERROR] Unbekannter Task-Typ"

        except Exception as e:
            return f"[ERROR] Verarbeitungsfehler: {str(e)}"

    def send_result(self, task_id, result):
        try:
            dispatcher_url = f"http://{self.dispatcher_host}:{self.dispatcher_port}"
            proxy = xmlrpc.client.ServerProxy(dispatcher_url)
            response = proxy.result_return(task_id, str(result))
            print(f"[INFO] Ergebnis gesendet: {response}")
        except Exception as e:
            print(f"[ERROR] Ergebnisübertragung fehlgeschlagen: {e}")

    def start_server(self):
        server = SimpleXMLRPCServer(("0.0.0.0", self.worker_port), allow_none=True)
        server.register_function(self.receive_task, "receive_task")
        print(f"[INFO] Worker-Server läuft unter {self.address}")
        server.serve_forever()

    def run(self):
        time.sleep(2)  # Warten, bis Namensdienst erreichbar ist
        self.register()
        server_thread = threading.Thread(target=self.start_server)
        server_thread.start()

# Optionaler Einstiegspunkt bei direktem Start (z. B. per Docker)
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Verwendung: python worker.py <type> <port> <namensdienst-url> <dispatcher-host>")
        sys.exit(1)

    task_type = sys.argv[1]
    port = int(sys.argv[2])
    naming_service_url = sys.argv[3]
    dispatcher_host = sys.argv[4]

    worker = Worker(task_type, port, naming_service_url, dispatcher_host)
    worker.run()
