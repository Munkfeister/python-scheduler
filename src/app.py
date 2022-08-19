import sys
sys.path.append("./dependencies")

import json
import jsonschema
import os

from jsonschema import validate

from scheduler import Scheduler
from queue_helper import Queue

def send_job(server):
    patching_queue_name = "rhel-patching"
    print(server)

class Main(object):
    
    scheduler_queue_name = "rhel-patching-scheduler"

    def __init__(self) -> None:
        print("Loading validation schema...")
        self.payloadSchema = self.get_validation_schema()

        print("Starting Scheduler...")
        with Scheduler() as self.schedule:
            self.schedule.start()

            print("Starting Queue Listener...")
            schedule_queue = Queue(self.scheduler_queue_name, self.process_message)
            schedule_queue.start()

    def get_validation_schema(self):
        with open('%s/schemas/default.json' % os.path.dirname(__file__), 'r') as f:
            schema_data = f.read()

        return json.loads(schema_data)

    def process_message(self, channel, method, properties, body):
        print(" [x] Received %r" % body)

        message_is_valid, validation_message = self.message_valid(body)

        if message_is_valid:
            payload = json.loads(body)

            if payload["action"] == "add":
                return_payload = self.schedule.add(send_job, payload["server"], payload["scheduledDateTime"])
            elif payload["action"] == "delete":
                return_payload = self.schedule.delete(payload["id"])
            elif payload["action"] == "list":
                return_payload = self.schedule.list()
            else:
                return_payload = { "status": "failed", "message": "Unknown action: " + payload["action"] }
        else:
            return_payload = { "status": "failed", "message": validation_message}
        
        print(json.dumps(return_payload, sort_keys=True, indent=4, separators=(',', ': '), default=str))

    def message_valid(self, message):
        try:
            payload = json.loads(message)
        except ValueError as err:
            message = "Invalid JSON message: " + message.decode("utf8")
            return False, message

        try:
            validate(
                instance=payload,
                schema=self.payloadSchema,
                format_checker=jsonschema.FormatChecker()
            )
        except Exception as err:
            message = "Invalid message passed: " + err.message
            print("Payload: " + json.dumps(payload, sort_keys=True, indent=4, separators=(',', ': '), default=str))
            return False, message

        return True, None
        
if __name__ == "__main__":
    main = Main()