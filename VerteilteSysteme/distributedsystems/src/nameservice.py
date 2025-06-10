import grpc
from concurrent import futures
import taskgrid_pb2
import taskgrid_pb2_grpc
from collections import defaultdict
import logging
import time
import threading

logging.basicConfig(level=logging.INFO)

# Das ist die NameService Klasse
# Sie kann verschiedene Worker registrieren und degistrieren
# Dabei speichert sie sie anhand ihrer Adresse und dem Typ des Service ab
# Über sie kann der Dispatcher Worker eines gewissen Types erfragen
# Sie läuft über gRPC auf Port 50051
class NameService(taskgrid_pb2_grpc.NameServiceServicer):
    def __init__(self):
        self.workers = defaultdict(dict)  # key: worker_type -> value: {address: {last_seen, status}}
        self.logger = logging.getLogger("NameService")
        self.worker_timeout = 5  # Sekunden bis ein Worker als inaktiv gilt
        
        # Cleanup Thread zum aufräumen inaktiver Worker
        self.cleanup_thread = threading.Thread(target=self._cleanup_inactive_workers, daemon=True)
        self.cleanup_thread.start()

    # Registrierung eines Workers anhand seines Typs und seiner Adresse
    def RegisterWorker(self, request, context):
        worker_type = request.type
        worker_address = request.address
        current_time = time.time()

        # Reaktivierung eines inaktiven workers
        if (worker_address in self.workers[worker_type] and 
            self.workers[worker_type][worker_address]["status"] == "INACTIVE"):
            self.logger.info(f"Reactivating worker of type {worker_type} at {worker_address}")

        self.workers[worker_type][worker_address] = {
            "last_seen": current_time,
            "status": "ACTIVE"
        }
        self.logger.info(f"Updated worker of type {worker_type} at {worker_address}")
        return taskgrid_pb2.RegisterWorkerResponse(success=True)

    # Anfordern eines Workers eines gewissen Typs
    def LookupWorker(self, request, context):
        worker_type = request.type
        current_time = time.time()
        
        # Nur Worker die in letzter Zeit sich gemeldet haben
        available_workers = {
            addr: info["last_seen"]
            for addr, info in self.workers.get(worker_type, {}).items()
            if info["status"] == "ACTIVE" and current_time - info["last_seen"] < self.worker_timeout
        }
        
        if not available_workers:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"No workers available for type {worker_type}")
            return taskgrid_pb2.LookupWorkerResponse()
        
        # Wähle den am längsten inaktiven Worker
        selected_worker = min(available_workers.items(), key=lambda x: x[1])[0]
        
        self.workers[worker_type][selected_worker]["last_seen"] = current_time
        
        return taskgrid_pb2.LookupWorkerResponse(address=selected_worker)

    # Abmelden eines Workers anhand seiner Adresse
    def DeregisterWorker(self, request, context):
        address = request.address
        success = False
        for worker_type in list(self.workers.keys()):
            if address in self.workers[worker_type]:
                # Setze inaktiv und letzte Zeit
                self.workers[worker_type][address]["status"] = "INACTIVE"
                self.workers[worker_type][address]["last_seen"] = time.time()
                success = True
                self.logger.info(f"Marked worker as inactive: {address}")
        return taskgrid_pb2.DeregisterWorkerResponse(success=success)

    # Gebe statistiken über die worker für die Monitoring komponente
    def GetWorkerStats(self, request, context):
        current_time = time.time()
        worker_counts = defaultdict(lambda: {"active": 0, "inactive": 0})
        active_addresses = []
        inactive_addresses = []
        worker_types_map = {}  # address -> type
        
        for worker_type, workers in self.workers.items():
            for addr, info in workers.items():
                time_since_last_seen = current_time - info["last_seen"]
                worker_types_map[addr] = worker_type
                
                # Aktive Worker markieren
                if time_since_last_seen < self.worker_timeout and info["status"] == "ACTIVE":
                    worker_counts[worker_type]["active"] += 1
                    active_addresses.append(addr)
                else:
                    # Inaktive Worker markieren
                    if info["status"] == "ACTIVE":
                        info["status"] = "INACTIVE"
                        self.logger.info(f"Marked worker as inactive due to timeout: {addr}")
                    worker_counts[worker_type]["inactive"] += 1
                    inactive_addresses.append(addr)
        
        worker_counts_response = {
            worker_type: counts["active"]  # Nur aktive Worker zählen
            for worker_type, counts in worker_counts.items()
        }
        
        return taskgrid_pb2.WorkerStatsResponse(
            worker_counts=worker_counts_response,
            worker_addresses=active_addresses,
            worker_types=worker_types_map
        )

    # Aufräumen inaktiver Worker nach einem gewissen Intervall
    def _cleanup_inactive_workers(self):
        while True:
            try:
                current_time = time.time()
                for worker_type in list(self.workers.keys()):
                    for addr, info in list(self.workers[worker_type].items()):
                        time_since_last_seen = current_time - info["last_seen"]
                        
                        # Inaktive Worker finden
                        if time_since_last_seen >= self.worker_timeout and info["status"] == "ACTIVE":
                            self.workers[worker_type][addr]["status"] = "INACTIVE"
                            self.logger.info(f"Marked worker as inactive due to timeout: {addr}")
                            
                        # Inaktive Worker nach 60 Sekunden entfernen
                        if time_since_last_seen >= 60: 
                            del self.workers[worker_type][addr]
                            self.logger.info(f"Removed inactive worker: {addr}")
                            
                    if not self.workers[worker_type]:
                        del self.workers[worker_type]
                        
            except Exception as e:
                self.logger.error(f"Error in cleanup thread: {e}")
            time.sleep(1)  # Läuft 1 mal pro sekunden

# Starten des RPC Servers
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    taskgrid_pb2_grpc.add_NameServiceServicer_to_server(NameService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    logging.info("NameService started on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve() 