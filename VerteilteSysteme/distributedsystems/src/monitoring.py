import grpc
from concurrent import futures
import taskgrid_pb2
import taskgrid_pb2_grpc
import logging
from flask import Flask, jsonify
import threading
import time
from collections import defaultdict

logging.basicConfig(level=logging.INFO)

# Das ist die Monitoring Klasse.
# Sie überwacht den Dispatcher und den Nameservice direkt, um über sie informationen über die Worker zu erhalten.
# Über http://localhost:8080/stats kann man die monitoring Informationen aufrufen.
# Diese enthalten nicht nur welche Worker gerade welche Tasks ausführen, sondern auch welche Worker allgemein registriert sind.
class MonitoringService(taskgrid_pb2_grpc.MonitoringServiceServicer):
    def __init__(self, nameservice_address, dispatcher_address):
        self.nameservice_address = nameservice_address
        self.dispatcher_address = dispatcher_address
        self.logger = logging.getLogger("MonitoringService")
        self.stats = {
            "active_workers": 0,
            "pending_tasks": 0,
            "pending_tasks_details": [],
            "avg_processing_time": 0.0,
            "worker_types": {},
            "service_connections": {
                "nameservice": False,
                "dispatcher": False
            },
            "task_type_stats": {},
            "active_worker_list": [],
            "all_workers": defaultdict(dict)
        }
        
        # Starte stats collection thread
        self.collector_thread = threading.Thread(target=self._collect_stats)
        self.collector_thread.daemon = True
        self.collector_thread.start()

    # konvertiert die timing stats in ein dict
    def _convert_timing_stats_to_dict(self, stats):
        
        return {
            "avg_time": stats.avg_time,
            "max_time": stats.max_time,
            "last_time": stats.last_time,
            "recent_times": list(stats.recent_times)
        }

    # holt sich die Systemstats vom taskgrid_pb2
    def GetSystemStats(self, request, context):
        return taskgrid_pb2.SystemStatsResponse(
            active_workers=self.stats["active_workers"],
            pending_tasks=self.stats["pending_tasks"],
            avg_processing_time=self.stats["avg_processing_time"],
            service_connections=self.stats["service_connections"],
            task_type_stats=self.stats["task_type_stats"]
        )

    # Überprüfe Verbindung zum Dispatcher / Namensdienst
    def _check_service_connection(self, address, service_name, max_retries=3):
        for attempt in range(max_retries):
            try:
                with grpc.insecure_channel(address) as channel:
                    if service_name == "nameservice": # nameservice
                        stub = taskgrid_pb2_grpc.NameServiceStub(channel)
                        stub.GetWorkerStats(taskgrid_pb2.WorkerStatsRequest())
                    else:  # dispatcher
                        stub = taskgrid_pb2_grpc.ClientServiceStub(channel)
                        stub.GetTaskStats(taskgrid_pb2.TaskStatsRequest())
                    
                    self.stats["service_connections"][service_name] = True
                    if attempt > 0:
                        self.logger.info(f"Successfully connected to {service_name} after {attempt + 1} attempts")
                    return True
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1}/{max_retries} to connect to {service_name} failed: {str(e)}")
                time.sleep(min(2 ** attempt, 10)) # 10s limit
        
        self.logger.error(f"Failed to connect to {service_name} after {max_retries} attempts")
        self.stats["service_connections"][service_name] = False
        return False

    # holt sich alle relevanten statistiken
    def _collect_stats(self):
        """Collect statistics from services."""
        while True:
            try:
                # überprüfe verbindungen
                ns_connected = self._check_service_connection(self.nameservice_address, "nameservice")
                disp_connected = self._check_service_connection(self.dispatcher_address, "dispatcher")

                if not (ns_connected or disp_connected):
                    self.logger.error("Could not connect to any services, waiting before retry...")
                    time.sleep(5)
                    continue

                # ---- nameservice statistiken ----
                if ns_connected:
                    with grpc.insecure_channel(self.nameservice_address) as channel:
                        nameservice = taskgrid_pb2_grpc.NameServiceStub(channel)
                        worker_stats = nameservice.GetWorkerStats(taskgrid_pb2.WorkerStatsRequest())
                        
                        # aktive Worker
                        current_active_addresses = set(worker_stats.worker_addresses)
                        current_time = time.time()
                        active_workers = []
                        worker_types_count = defaultdict(int)

                        for addr in current_active_addresses:
                            worker_type = worker_stats.worker_types.get(addr, "unknown")
                            worker_types_count[worker_type] += 1
                            
                            self.stats["all_workers"][worker_type][addr] = {
                                "address": addr,
                                "last_seen": current_time,
                                "type": worker_type
                            }
                            
                            active_workers.append({
                                "type": worker_type,
                                "address": addr,
                                "status": "ACTIVE"
                            })

                        # Alte worker aufräumen
                        for worker_type in list(self.stats["all_workers"].keys()):
                            for addr in list(self.stats["all_workers"][worker_type].keys()):
                                if addr not in current_active_addresses:
                                    last_seen = self.stats["all_workers"][worker_type][addr]["last_seen"]
                                    time_since_last_seen = current_time - last_seen
                                    
                                    if time_since_last_seen > 30:  # entfernen nach 30s
                                        del self.stats["all_workers"][worker_type][addr]
                                        if not self.stats["all_workers"][worker_type]:
                                            del self.stats["all_workers"][worker_type]
                                        continue
                                    
                                    # Inaktiv
                                    if time_since_last_seen <= 10:
                                        worker_types_count[worker_type] += 1
                                        active_workers.append({
                                            "type": worker_type,
                                            "address": addr,
                                            "status": "INACTIVE"
                                        })
                        
                        # Statistiken updaten
                        self.stats["active_workers"] = len(current_active_addresses)
                        self.stats["worker_types"] = dict(worker_types_count)
                        self.stats["active_worker_list"] = sorted(
                            active_workers,
                            key=lambda x: (x["type"], x["status"] != "ACTIVE", x["address"])
                        )

                # ---- dispatcher statistiken ----
                if disp_connected:
                    with grpc.insecure_channel(self.dispatcher_address) as channel:
                        dispatcher = taskgrid_pb2_grpc.ClientServiceStub(channel)
                        task_stats = dispatcher.GetTaskStats(taskgrid_pb2.TaskStatsRequest())
                        
                        # Pending tasks
                        pending_tasks = []
                        for task_id in range(task_stats.task_counter):
                            try:
                                response = dispatcher.RequestResult(
                                    taskgrid_pb2.RequestResultRequest(task_id=task_id)
                                )
                                if response.task.status == "PENDING":
                                    pending_tasks.append({
                                        "id": response.task.id,
                                        "type": response.task.type,
                                        "payload": response.task.payload,
                                        "timestamp_created": response.task.timestamp_created,
                                        "waiting_time": int(time.time()) - response.task.timestamp_created
                                    })
                            except grpc.RpcError:
                                continue
                        
                        # update statistiken
                        self.stats["pending_tasks_details"] = pending_tasks
                        self.stats["pending_tasks"] = len(pending_tasks)
                        
                      
                        task_type_stats = {}
                        for task_type, stats in task_stats.task_stats.items():
                            task_type_stats[task_type] = self._convert_timing_stats_to_dict(stats)
                        self.stats["task_type_stats"] = task_type_stats
                        
                        # Average processing zeit
                        if task_type_stats:
                            avg_times = [stats["avg_time"] for stats in task_type_stats.values()]
                            if avg_times:
                                self.stats["avg_processing_time"] = sum(avg_times) / len(avg_times)

            except Exception as e:
                self.logger.error(f"Error collecting stats: {e}")
            
            time.sleep(5)  # Update alle 5s

# Erstellt die Flask app
app = Flask(__name__)
monitoring_service = None

@app.route('/stats', methods=['GET'])
def get_stats():
    if monitoring_service:
        return jsonify(monitoring_service.stats)
    return jsonify({"error": "Monitoring service not initialized"}), 500

@app.route('/workers', methods=['GET'])
def get_workers():
    if monitoring_service:
        return jsonify(monitoring_service.stats["active_worker_list"])
    return jsonify({"error": "Monitoring service not initialized"}), 500

# Starten des RPC Servers
def serve_grpc(nameservice_address, dispatcher_address, port):
    global monitoring_service
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    monitoring_service = MonitoringService(nameservice_address, dispatcher_address)
    taskgrid_pb2_grpc.add_MonitoringServiceServicer_to_server(monitoring_service, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logging.info(f"Monitoring gRPC service started on port {port}")
    return server

# Starten der REST Schnitstelle
def serve_rest(host, port):
    app.run(host=host, port=port)

if __name__ == '__main__':
    import os
    
    nameservice_address = os.getenv('NAMESERVICE_ADDRESS', 'localhost:50051')
    dispatcher_address = os.getenv('DISPATCHER_ADDRESS', 'localhost:50052')
    grpc_port = int(os.getenv('GRPC_PORT', '50053'))
    rest_port = int(os.getenv('REST_PORT', '8080'))
    
    # Starte gRPC server
    grpc_server = serve_grpc(nameservice_address, dispatcher_address, grpc_port)
    
    # Start REST server
    serve_rest('0.0.0.0', rest_port)
    
    grpc_server.wait_for_termination() 