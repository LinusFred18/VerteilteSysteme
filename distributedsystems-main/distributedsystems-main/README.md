# TaskGrid+ - Distributed Task Processing System

TaskGrid+ is a containerized, distributed task processing system that allows dynamic task distribution across multiple worker nodes. The system uses gRPC for communication and provides a name service for dynamic worker discovery.

## Architecture Overview

```
┌──────────────┐     ┌──────────────┐     ┌───────────────┐
│              │     │              │     │   Worker      │
│    Client    ├────►│  Dispatcher  ├────►│   (reverse)   │
│              │     │              │     │               │
└──────────────┘     └───────┬──────┘     └───────────────┘
                             │            ┌───────────────┐
                             │            │   Worker      │
                     ┌───────▼──────┐     │    (sum)      │
                     │              │     │               │
                     │ Nameservice  ├────►└───────────────┘
                     │              │     ┌───────────────┐
                     └───────┬──────┘     │   Worker      │
                             │            │   (hash)      │
                     ┌───────▼──────┐     │               │
                     │              │     └───────────────┘
                     │ Monitoring   │     ┌───────────────┐
                     │             │     │   Worker      │
                     └──────────────┘     │   (upper)     │
                                         │               │
                                         └───────────────┘
                                         ┌───────────────┐
                                         │   Worker      │
                                         │   (wait)      │
                                         │               │
                                         └───────────────┘
```

## System Components

### 1. Client Service

- **Port**: N/A (runs as needed)
- **Responsibilities**:
  - Sends task requests to dispatcher
  - Retrieves task results
  - Provides task status monitoring
- **Key Methods**:
  - `send_task(type, payload)`
  - `request_result(task_id)`

### 2. Dispatcher Service

- **Port**: 50052
- **Responsibilities**:
  - Task queue management
  - Worker discovery via nameservice
  - Task distribution
  - Result collection and storage
- **Key Features**:
  - Asynchronous task processing
  - Load balancing
  - Error handling and retries
  - Task status tracking

### 3. Nameservice

- **Port**: 50051
- **Responsibilities**:
  - Worker registration
  - Service discovery
  - Address resolution
- **Key Features**:
  - Dynamic worker registration/deregistration
  - Type-based worker lookup
  - Health checking (planned)

### 4. Worker Services

- **Ports**: 50060-50064
- **Types**:
  - **Reverse Worker** (50060)
    - String reversal operations
    - Example: "hello" → "olleh"
  - **Sum Worker** (50061)
    - Numerical array summation
    - Example: [1,2,3] → 6
  - **Hash Worker** (50062)
    - SHA256 hash calculation
    - Example: "text" → "982d9e3...""
  - **Upper Worker** (50063)
    - Text uppercase conversion
    - Example: "hello" → "HELLO"
  - **Wait Worker** (50064)
    - Simulated processing delays
    - Example: "2" → wait 2 seconds

### 5. Monitoring Service

- **Ports**:
  - gRPC: 50053
  - REST: 8080
- **Responsibilities**:
  - System statistics collection
  - Performance monitoring
  - Health checking
- **Metrics**:
  - Active workers
  - Pending tasks
  - Average processing time
  - Worker-specific statistics

## Communication Flow

1. **Task Submission**:

   ```
   Client → Dispatcher → Nameservice → Worker → Dispatcher → Client
   ```

2. **Worker Registration**:

   ```
   Worker → Nameservice (register)
   Dispatcher → Nameservice (lookup)
   ```

3. **Monitoring**:
   ```
   Monitoring ← Nameservice (worker stats)
   Monitoring ← Dispatcher (task stats)
   ```

## Network Configuration

All services operate within a Docker network (`taskgrid`) with the following characteristics:

- Network Type: Bridge
- Service Discovery: Docker DNS
- Inter-container Communication: gRPC
- External Access: Exposed ports

## Data Structures

### Task Object

```python
{
    "id": int,
    "type": string,
    "payload": string,
    "result": string,
    "status": string,
    "timestamp_created": int64,
    "timestamp_completed": int64
}
```

## Error Handling

1. **Worker Failures**:

   - Automatic task requeuing
   - Worker deregistration
   - Error reporting to client

2. **Network Issues**:

   - Connection retry mechanism
   - Timeout handling
   - Circuit breaker pattern (planned)

3. **Invalid Tasks**:
   - Input validation
   - Type checking
   - Error response to client

## Deployment

1. Build and start all services:

   ```bash
   docker-compose up --build
   ```

2. Run test client:

   ```bash
   docker-compose up test-client
   ```

3. Monitor system:
   ```bash
   curl http://localhost:8080/stats
   ```

## Development

### Adding New Worker Types

1. Update Proto Definition:

   ```protobuf
   // Add new task type
   message NewTaskType {
     string input = 1;
     string output = 2;
   }
   ```

2. Implement Worker:

   ```python
   def process_new_task(self, payload):
       # Implementation
       return result
   ```

3. Update Docker Configuration:
   ```yaml
   worker-new:
     build:
       args:
         - WORKER_TYPE=new
         - PORT=50065
   ```

## Testing

The system includes a test client that:

- Generates random tasks
- Sends them every 30 seconds
- Logs results to `taskgrid_tests.log`
- Provides real-time feedback

## Monitoring

Access system statistics through:

- **REST API**: http://localhost:8080/stats
- **gRPC Interface**: Port 50053
- **Log Files**: Container logs via Docker
