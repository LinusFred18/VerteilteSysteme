import json
import time

class Task:
    def __init__(self, id, type, payload, result="", status="pending", timestamp_created=None, timestamp_completed=None):
        self.id = id
        self.type = type[:32]  # Max 32 chars
        self.payload = payload[:1024]  # Max 1024 chars
        self.result = result[:1024]  # Max 1024 chars
        self.status = status[:16]  # Max 16 chars
        self.timestamp_created = timestamp_created or int(time.time())
        self.timestamp_completed = timestamp_completed

    def to_json(self):
        return json.dumps({
            "id": self.id,
            "type": self.type,
            "payload": self.payload,
            "result": self.result,
            "status": self.status,
            "timestamp_created": self.timestamp_created,
            "timestamp_completed": self.timestamp_completed
        })

    @staticmethod
    def from_json(data):
        data = json.loads(data)
        return Task(
            id=data["id"],
            type=data["type"],
            payload=data["payload"],
            result=data.get("result", ""),
            status=data.get("status", "pending"),
            timestamp_created=data.get("timestamp_created"),
            timestamp_completed=data.get("timestamp_completed")
        )