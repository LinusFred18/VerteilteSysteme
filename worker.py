import socket
import json
import hashlib
import time
import os
import string
import re
import xmlrpc.client
import xmlrpc.server

# Register workers
def register():
    pass

# While-True loop (main)
def main():
    pass

# Check Payload for correctness
def check_payload():
    pass

# Tasks
def calculate_tasks(input):
    task_id = input.get("id")
    task_type = input.get("type")
    payload = input.get("payload")


    if task_type == "reverse":
        return payload[::-1]
    
    elif task_type == "sum":
        value = 0
        for number in payload.split():
            value = value + int(number)
        return value


    elif task_type == "hash":
        hash = hashlib.sha256((payload).encode('utf-8')) # Funktioniert das so?
        return hash

    elif task_type == "upper":
        return payload.upper()

    elif task_type == "wait":
        pass

    elif task_type == "length":
        return len(payload)

    elif task_type == "average":
        value = 0
        for number in payload.split():
            value = value + int(number)
        value = value/len(payload.replace(" ",""))
        return value

    elif task_type == "prime":
        payload = payload.replace(" ","")
        number = int(payload)
        if number < 2:
            return False
        for i in range(2, int(number ** 0.5) + 1):
            if number % i == 0:
                return False
        return True

    pass

# Return results to client
def send_results(result):
    pass


result = calculate_tasks()

if __name__ == "__main__":
    main()
