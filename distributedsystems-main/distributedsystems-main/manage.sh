#!/bin/bash

# Function to display usage
show_usage() {
    echo "Usage:"
    echo "  ./manage.sh start-core        # Start core services (nameservice, monitoring, dispatcher, test-client)"
    echo "  ./manage.sh start-workers [worker-type] [count]  # Start specific worker type with count"
    echo "  ./manage.sh stop              # Stop all services"
    echo "  ./manage.sh stop-worker [container-id]  # Stop a specific worker container"
    echo ""
    echo "Available worker types:"
    echo "  - reverse"
    echo "  - sum"
    echo "  - hash"
    echo "  - upper"
    echo "  - wait"
    echo "  - length"
    echo "  - average"
    echo "  - lower"
    echo "  - prime"
    echo ""
    echo "Example:"
    echo "  ./manage.sh start-core"
    echo "  ./manage.sh start-workers reverse 3"
    echo "  ./manage.sh stop"
    echo "  ./manage.sh stop-worker worker-length-5646ae59"
}

# Function to ensure network exists
ensure_network() {
    if ! docker network ls | grep -q "taskgrid"; then
        echo "Creating taskgrid network..."
        docker network create taskgrid
    fi
}

# Function to check if core services are running
check_core_services() {
    if ! docker ps | grep -q "nameservice"; then
        echo "Error: Core services are not running. Please start them first with:"
        echo "  ./manage.sh start-core"
        exit 1
    fi
}

# Function to wait for service to be healthy
wait_for_service() {
    local service=$1
    local max_attempts=30
    local attempt=1

    echo "Waiting for $service to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if docker ps | grep -q "$service"; then
            echo "$service is ready!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: $service is not ready yet..."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo "Error: $service failed to start within the expected time"
    return 1
}

# Function to start core services
start_core() {
    echo "Starting core services..."
    ensure_network
    
    # Start core services with rebuild
    docker-compose -f docker-compose.core.yml up -d --build

    # Wait for nameservice to be ready
    if ! wait_for_service "nameservice"; then
        echo "Error: Failed to start core services"
        stop_services
        exit 1
    fi

    echo "Core services started successfully. You can access:"
    echo "- Monitoring UI: http://localhost:8080"
    echo "- Nameservice: localhost:50051"
    echo "- Dispatcher: localhost:50052"
    echo "- Monitoring gRPC: localhost:50053"
}

# Function to stop all services
stop_services() {
    echo "Stopping all services..."
    docker-compose -f docker-compose.workers.yml down 2>/dev/null || true
    docker-compose -f docker-compose.core.yml down 2>/dev/null || true
    
    # Only remove network if no containers are using it
    if ! docker ps -q --filter network=taskgrid | grep -q .; then
        echo "Removing taskgrid network..."
        docker network rm taskgrid 2>/dev/null || true
    fi
    
    echo "All services stopped."
}

# Function to start workers
start_workers() {
    if [ -z "$1" ] || [ -z "$2" ]; then
        echo "Error: Worker type and count are required"
        show_usage
        exit 1
    fi

    # Check if core services are running first
    check_core_services

    worker_type=$1
    count=$2

    # Validate worker type
    valid_types=("reverse" "sum" "hash" "upper" "wait" "length" "average" "lower" "prime")
    valid=0
    for type in "${valid_types[@]}"; do
        if [ "$worker_type" == "$type" ]; then
            valid=1
            break
        fi
    done

    if [ $valid -eq 0 ]; then
        echo "Error: Invalid worker type '$worker_type'"
        show_usage
        exit 1
    fi

    echo "Starting $count instance(s) of $worker_type worker..."
    ensure_network
    
    # Stop existing workers of this type first
    docker-compose -f docker-compose.workers.yml stop worker-$worker_type
    docker-compose -f docker-compose.workers.yml rm -f worker-$worker_type
    
    # Start workers with rebuild
    docker-compose -f docker-compose.workers.yml up -d --build --scale worker-$worker_type=$count worker-$worker_type
}

# Function to stop a specific worker
stop_worker() {
    if [ -z "$1" ]; then
        echo "Error: Worker container ID is required"
        show_usage
        exit 1
    fi

    worker_id=$1
    
    # Check if the worker exists and is running
    if ! docker ps | grep -q "$worker_id"; then
        echo "Error: Worker container '$worker_id' not found or not running"
        exit 1
    fi
    
    echo "Stopping worker container '$worker_id'..."
    # Send SIGTERM to allow graceful shutdown
    docker stop --time 10 "$worker_id"
    
    echo "Worker stopped successfully."
}

# Main script logic
case "$1" in
    "start-core")
        start_core
        ;;
    "start-workers")
        start_workers "$2" "$3"
        ;;
    "stop")
        stop_services
        ;;
    "stop-worker")
        stop_worker "$2"
        ;;
    *)
        show_usage
        exit 1
        ;;
esac 