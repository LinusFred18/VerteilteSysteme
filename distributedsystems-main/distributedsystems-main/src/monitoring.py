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
            "avg_processing_time": 0.0,
            "worker_types": {},
            "service_connections": {
                "nameservice": False,
                "dispatcher": False
            },
            "task_type_stats": {},  # Detailed timing stats per task type
            "active_worker_list": []  # List of {type, address} dicts
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

    # überprüft welche service connection verfügbar sind
    def _check_service_connection(self, address, service_name):
        try:
            with grpc.insecure_channel(address) as channel:

                grpc.channel_ready_future(channel).result(timeout=2)
                self.stats["service_connections"][service_name] = True
                return True
        except Exception as e:
            self.logger.error(f"Failed to connect to {service_name} at {address}: {e}")
            self.stats["service_connections"][service_name] = False
            return False

    # holt sich alle relevanten statistiken
    def _collect_stats(self):
        while True:
            try:
                # überprüft die service connections
                self._check_service_connection(self.nameservice_address, "nameservice")
                self._check_service_connection(self.dispatcher_address, "dispatcher")

                # Holt die worker stats vom nameservice
                if self.stats["service_connections"]["nameservice"]:
                    with grpc.insecure_channel(self.nameservice_address) as channel:
                        nameservice = taskgrid_pb2_grpc.NameServiceStub(channel)
                        worker_stats = nameservice.GetWorkerStats(taskgrid_pb2.WorkerStatsRequest())
                        
                        # Updatet die worker stats
                        total_workers = sum(worker_stats.worker_counts.values())
                        self.stats["active_workers"] = total_workers
                        self.stats["worker_types"] = dict(worker_stats.worker_counts)
                        
                        # Updatet die active worker Liste
                        active_workers = []
                        for worker_type, count in worker_stats.worker_counts.items():
                            type_addresses = [addr for addr in worker_stats.worker_addresses 
                                           if addr.startswith(f"worker-{worker_type}:")]
                            for addr in type_addresses:
                                active_workers.append({
                                    "type": worker_type,
                                    "address": addr,
                                    "status": "ACTIVE"
                                })
                        self.stats["active_worker_list"] = active_workers
                        self.logger.info(f"Active workers: {len(active_workers)}")

                # Collect task stats from dispatcher
                if self.stats["service_connections"]["dispatcher"]:
                    with grpc.insecure_channel(self.dispatcher_address) as channel:
                        dispatcher = taskgrid_pb2_grpc.ClientServiceStub(channel)
                        task_stats = dispatcher.GetTaskStats(taskgrid_pb2.TaskStatsRequest())
                        
                        # Updated die task stats
                        self.stats["pending_tasks"] = task_stats.pending_tasks
                        
                      
                        task_type_stats = {}
                        for task_type, stats in task_stats.task_stats.items():
                            task_type_stats[task_type] = self._convert_timing_stats_to_dict(stats)
                        self.stats["task_type_stats"] = task_type_stats
                        
                        # Berechnet die overall average processing time
                        if task_type_stats:
                            avg_times = [stats["avg_time"] for stats in task_type_stats.values()]
                            if avg_times:
                                self.stats["avg_processing_time"] = sum(avg_times) / len(avg_times)

            except Exception as e:
                self.logger.error(f"Error collecting stats: {e}")
            
            time.sleep(5)  # Update every 5 seconds

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
    
    # Starte REST Schnittstelle
    serve_rest('0.0.0.0', rest_port) 