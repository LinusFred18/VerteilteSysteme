import grpc
from concurrent import futures
import taskgrid_pb2
import taskgrid_pb2_grpc
import logging
from flask import Flask, jsonify
import threading
import time

logging.basicConfig(level=logging.INFO)

class MonitoringService(taskgrid_pb2_grpc.MonitoringServiceServicer):
    def __init__(self, nameservice_address, dispatcher_address):
        self.nameservice_address = nameservice_address
        self.dispatcher_address = dispatcher_address
        self.logger = logging.getLogger("MonitoringService")
        self.stats = {
            "active_workers": 0,
            "pending_tasks": 0,
            "avg_processing_time": 0.0,
            "worker_types": {}
        }
        
        # Start stats collection thread
        self.collector_thread = threading.Thread(target=self._collect_stats)
        self.collector_thread.daemon = True
        self.collector_thread.start()

    def GetSystemStats(self, request, context):
        return taskgrid_pb2.SystemStatsResponse(
            active_workers=self.stats["active_workers"],
            pending_tasks=self.stats["pending_tasks"],
            avg_processing_time=self.stats["avg_processing_time"]
        )

    def _collect_stats(self):
        while True:
            try:
                # Collect worker stats from nameservice
                with grpc.insecure_channel(self.nameservice_address) as channel:
                    nameservice = taskgrid_pb2_grpc.NameServiceStub(channel)
                    # Note: We would need to add an RPC method to get all workers
                    # For now, we'll just collect what we can
                    
                # Collect task stats from dispatcher
                with grpc.insecure_channel(self.dispatcher_address) as channel:
                    dispatcher = taskgrid_pb2_grpc.ClientServiceStub(channel)
                    # Note: We would need to add an RPC method to get task stats
                    
                time.sleep(5)  # Update every 5 seconds
                
            except Exception as e:
                self.logger.error(f"Error collecting stats: {e}")
                time.sleep(5)

# Create Flask app for REST API
app = Flask(__name__)
monitoring_service = None

@app.route('/stats', methods=['GET'])
def get_stats():
    if monitoring_service:
        return jsonify(monitoring_service.stats)
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
    
    # Start REST API
    serve_rest('0.0.0.0', rest_port) 