import grpc
from concurrent import futures
import taskgrid_pb2
import taskgrid_pb2_grpc
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)

# Das ist die NameService Klasse
# Sie kann verschiedene Worker registrieren und degistrieren
# Dabei speichert sie sie anhand ihrer Adresse und dem Typ des Service ab
# Über sie kann der Dispatcher Worker eines gewissen Types erfragen
# Sie läuft über gRPC auf Port 50051
class NameService(taskgrid_pb2_grpc.NameServiceServicer):
    def __init__(self):
        self.workers = defaultdict(list)  # key: Workertyp | val: Liste der Workeradressen
        self.logger = logging.getLogger("NameService")

    # Registrierung eines Workers anhand seines Typs und seiner Adresse
    def RegisterWorker(self, request, context):
        worker_type = request.type
        worker_address = request.address
        self.workers[worker_type].append(worker_address)
        self.logger.info(f"Registered worker of type {worker_type} at {worker_address}")
        return taskgrid_pb2.RegisterWorkerResponse(success=True)

    # Anfordern eines Workers eines gewissen Typs
    def LookupWorker(self, request, context):
        worker_type = request.type
        available_workers = self.workers.get(worker_type, [])
        if not available_workers:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"No workers available for type {worker_type}")
            return taskgrid_pb2.LookupWorkerResponse()
        
        # round-robin auswahl des service
        worker = available_workers[0]
        available_workers.append(available_workers.pop(0))
        return taskgrid_pb2.LookupWorkerResponse(address=worker)

    # Abmelden eines Workers anhand seiner Adresse
    def DeregisterWorker(self, request, context):
        address = request.address
        success = False
        for worker_type in self.workers:
            if address in self.workers[worker_type]:
                self.workers[worker_type].remove(address)
                success = True
                self.logger.info(f"Deregistered worker at {address}")
        return taskgrid_pb2.DeregisterWorkerResponse(success=success)

    # Gebe statistiken über die worker für die Monitoring komponente
    def GetWorkerStats(self, request, context):
        worker_counts = {}
        all_addresses = []
        
        for worker_type, addresses in self.workers.items():
            worker_counts[worker_type] = len(addresses)
            all_addresses.extend(addresses)
        
        return taskgrid_pb2.WorkerStatsResponse(
            worker_counts=worker_counts,
            worker_addresses=all_addresses
        )

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