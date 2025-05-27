from xmlrpc.client import ServerProxy
import xmlrpc.client

class Client:
    def __init__(self, dispatcher_address):
        """
        Initializes the client with the dispatcher's address.
        :param dispatcher_address: The URL of the Dispatcher's RPC server (e.g., "http://localhost:8000/").
        """
        self.dispatcher = ServerProxy(dispatcher_address)

    def send_task(self, data_type, data_payload):
        """
        Sends a task with a given type and payload to the Dispatcher.
        :param data_type: The type of the task (e.g., "reverse", "sum"). [cite: 10]
        :param data_payload: The data associated with the task. [cite: 10]
        :return: A success message including the task ID, or an error message. [cite: 13]
        """
        try:
            # The Dispatcher expects 'type' and 'payload' [cite: 13]
            response = self.dispatcher.POST_TASK(data_type, data_payload)
            print(f"Task sent: {response}")
            return response
        except xmlrpc.client.Fault as err:
            print(f"A fault occurred: {err.faultCode}, {err.faultString}")
            return f"Error: {err.faultString}"
        except Exception as e:
            print(f"An error occurred: {e}")
            return f"Error: {e}"

    def request_result(self, task_id):
        """
        Requests the result for a given task ID from the Dispatcher.
        :param task_id: The ID of the task to retrieve the result for. [cite: 10, 13]
        :return: The result of the task or an error message. [cite: 13]
        """
        try:
            # The Dispatcher expects 'task_id' [cite: 13]
            response = self.dispatcher.GET_RESULT(task_id)
            print(f"Result for task {task_id}: {response}")
            return response
        except xmlrpc.client.Fault as err:
            print(f"A fault occurred: {err.faultCode}, {err.faultString}")
            return f"Error: {err.faultString}"
        except Exception as e:
            print(f"An error occurred: {e}")
            return f"Error: {e}"

    def receive_task(self):
        """
        This method is included as per the requirements[cite: 10], but a client
        in this system primarily sends and requests. If the client were to also
        act as a server for receiving tasks (e.g., for direct P2P communication
        or callbacks), then an RPC server would need to be set up within the client.
        For the given flow (Client -> Dispatcher -> Worker), this method's
        implementation would depend on how the client is expected to "receive"
        tasks (e.g., if it's polling or if the Dispatcher calls back).
        For now, it's a placeholder.
        """
        print("Client is ready to receive tasks (if applicable).")
        pass

# Example Usage (assuming a Dispatcher RPC server is running at http://localhost:8000/)
if __name__ == "__main__":
    dispatcher_address = "http://localhost:8000/" # This should be the address of your Dispatcher container
    client = Client(dispatcher_address)

    # Example: Send a 'reverse' task
    print("\nSending a 'reverse' task...")
    task_response = client.send_task("reverse", "hello_world")
    # You would parse task_response to get the task_id if successful
    # For demonstration, let's assume a task_id of 1 for the next step
    # In a real scenario, you'd extract the ID from task_response string like "Task empfangen, ID=123"

    # Example: Request a result for a task (replace with an actual task_id)
    # This assumes a task with ID 1 exists and has been processed
    # In a real application, you'd wait or poll for the result.
    print("\nRequesting result for task ID (example ID: 1):")
    result = client.request_result(1)

    # Example: Send a 'sum' task
    print("\nSending a 'sum' task...")
    client.send_task("sum", "1,2,3,4,5")

    # Example: Send a 'hash' task
    print("\nSending a 'hash' task...")
    client.send_task("hash", "my_secret_data")