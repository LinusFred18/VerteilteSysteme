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

class MonitoringService(taskgrid_pb2_grpc.MonitoringServiceServicer):
    def __init__(self, nameservice_address, dispatcher_address):
        self.nameservice_address = nameservice_address
        self.dispatcher_address = dispatcher_address
        self.logger = logging.getLogger("MonitoringService")
        self.stats = {
            "active_workers": 0,
            "pending_tasks": 0,
            "pending_tasks_details": [],  # List of pending task details
            "avg_processing_time": 0.0,
            "worker_types": {},
            "service_connections": {
                "nameservice": False,
                "dispatcher": False
            },
            "task_type_stats": {},  # Detailed timing stats per task type
            "active_worker_list": [],  # List of {type, address, status} dicts
            "all_workers": defaultdict(dict)  # Track all workers ever seen
        }
        
        # Start stats collection thread
        self.collector_thread = threading.Thread(target=self._collect_stats)
        self.collector_thread.daemon = True
        self.collector_thread.start()

    def _convert_timing_stats_to_dict(self, stats):
        """Convert TaskTimingStats protobuf to dictionary."""
        return {
            "avg_time": stats.avg_time,
            "max_time": stats.max_time,
            "last_time": stats.last_time,
            "recent_times": list(stats.recent_times)
        }

    def GetSystemStats(self, request, context):
        return taskgrid_pb2.SystemStatsResponse(
            active_workers=self.stats["active_workers"],
            pending_tasks=self.stats["pending_tasks"],
            avg_processing_time=self.stats["avg_processing_time"],
            service_connections=self.stats["service_connections"],
            task_type_stats=self.stats["task_type_stats"]
        )

    def _check_service_connection(self, address, service_name, max_retries=3):
        """Check connection to a service with retries."""
        for attempt in range(max_retries):
            try:
                with grpc.insecure_channel(address) as channel:
                    # Create appropriate stub based on service
                    if service_name == "nameservice":
                        stub = taskgrid_pb2_grpc.NameServiceStub(channel)
                        # Try a simple request
                        stub.GetWorkerStats(taskgrid_pb2.WorkerStatsRequest())
                    else:  # dispatcher
                        stub = taskgrid_pb2_grpc.ClientServiceStub(channel)
                        # Try a simple request
                        stub.GetTaskStats(taskgrid_pb2.TaskStatsRequest())
                    
                    self.stats["service_connections"][service_name] = True
                    if attempt > 0:
                        self.logger.info(f"Successfully connected to {service_name} after {attempt + 1} attempts")
                    return True
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1}/{max_retries} to connect to {service_name} failed: {str(e)}")
                time.sleep(min(2 ** attempt, 10))  # Exponential backoff, max 10 seconds
        
        self.logger.error(f"Failed to connect to {service_name} after {max_retries} attempts")
        self.stats["service_connections"][service_name] = False
        return False

    def _collect_stats(self):
        """Collect statistics from services."""
        while True:
            try:
                # Check service connections first
                ns_connected = self._check_service_connection(self.nameservice_address, "nameservice")
                disp_connected = self._check_service_connection(self.dispatcher_address, "dispatcher")

                if not (ns_connected or disp_connected):
                    self.logger.error("Could not connect to any services, waiting before retry...")
                    time.sleep(5)
                    continue

                # Collect worker stats from nameservice if connected
                if ns_connected:
                    with grpc.insecure_channel(self.nameservice_address) as channel:
                        nameservice = taskgrid_pb2_grpc.NameServiceStub(channel)
                        worker_stats = nameservice.GetWorkerStats(taskgrid_pb2.WorkerStatsRequest())
                        
                        # Get current active addresses and their types
                        current_active_addresses = set(worker_stats.worker_addresses)
                        current_time = time.time()
                        
                        # Update active worker list and all_workers tracking
                        active_workers = []
                        worker_types_count = defaultdict(int)

                        # Process all currently active workers
                        for addr in current_active_addresses:
                            worker_type = worker_stats.worker_types.get(addr, "unknown")  # Get actual worker type
                            worker_types_count[worker_type] += 1
                            
                            # Update all_workers with minimal info
                            self.stats["all_workers"][worker_type][addr] = {
                                "address": addr,
                                "last_seen": current_time,
                                "type": worker_type
                            }
                            
                            # Add to active workers list with status
                            active_workers.append({
                                "type": worker_type,
                                "address": addr,
                                "status": "ACTIVE"
                            })

                        # Clean up old workers from all_workers
                        for worker_type in list(self.stats["all_workers"].keys()):
                            for addr in list(self.stats["all_workers"][worker_type].keys()):
                                if addr not in current_active_addresses:
                                    last_seen = self.stats["all_workers"][worker_type][addr]["last_seen"]
                                    time_since_last_seen = current_time - last_seen
                                    
                                    if time_since_last_seen > 30:  # Remove from all_workers after 30 seconds
                                        del self.stats["all_workers"][worker_type][addr]
                                        if not self.stats["all_workers"][worker_type]:
                                            del self.stats["all_workers"][worker_type]
                                        continue
                                    
                                    # Add to active_worker_list as inactive if recently seen
                                    if time_since_last_seen <= 10:  # Show as inactive only if within 10 seconds
                                        worker_types_count[worker_type] += 1
                                        active_workers.append({
                                            "type": worker_type,
                                            "address": addr,
                                            "status": "INACTIVE"
                                        })
                        
                        # Update stats
                        self.stats["active_workers"] = len(current_active_addresses)
                        self.stats["worker_types"] = dict(worker_types_count)
                        self.stats["active_worker_list"] = sorted(
                            active_workers,
                            key=lambda x: (x["type"], x["status"] != "ACTIVE", x["address"])
                        )

                # Collect task stats from dispatcher if connected
                if disp_connected:
                    with grpc.insecure_channel(self.dispatcher_address) as channel:
                        dispatcher = taskgrid_pb2_grpc.ClientServiceStub(channel)
                        task_stats = dispatcher.GetTaskStats(taskgrid_pb2.TaskStatsRequest())
                        
                        # Get details of pending tasks
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
                        
                        # Update task stats with actual count from details
                        self.stats["pending_tasks_details"] = pending_tasks
                        self.stats["pending_tasks"] = len(pending_tasks)
                        
                        # Convert task stats to dictionary
                        task_type_stats = {}
                        for task_type, stats in task_stats.task_stats.items():
                            task_type_stats[task_type] = self._convert_timing_stats_to_dict(stats)
                        self.stats["task_type_stats"] = task_type_stats
                        
                        # Calculate overall average processing time
                        if task_type_stats:
                            avg_times = [stats["avg_time"] for stats in task_type_stats.values()]
                            if avg_times:
                                self.stats["avg_processing_time"] = sum(avg_times) / len(avg_times)

            except Exception as e:
                self.logger.error(f"Error collecting stats: {e}")
            
            time.sleep(5)  # Update every 5 seconds

# Create Flask app for REST API
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

def serve_grpc(nameservice_address, dispatcher_address, port):
    global monitoring_service
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    monitoring_service = MonitoringService(nameservice_address, dispatcher_address)
    taskgrid_pb2_grpc.add_MonitoringServiceServicer_to_server(monitoring_service, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logging.info(f"Monitoring gRPC service started on port {port}")
    return server

def serve_rest(host, port):
    app.run(host=host, port=port)

if __name__ == '__main__':
    import os
    
    nameservice_address = os.getenv('NAMESERVICE_ADDRESS', 'localhost:50051')
    dispatcher_address = os.getenv('DISPATCHER_ADDRESS', 'localhost:50052')
    grpc_port = int(os.getenv('GRPC_PORT', '50053'))
    rest_port = int(os.getenv('REST_PORT', '8080'))
    
    # Start gRPC server
    grpc_server = serve_grpc(nameservice_address, dispatcher_address, grpc_port)
    
    # Start REST server
    serve_rest('0.0.0.0', rest_port)
    
    # Keep the gRPC server running
    grpc_server.wait_for_termination() 