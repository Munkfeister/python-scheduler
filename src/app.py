import sys
sys.path.append("./dependencies")

import json
import jsonschema

from jsonschema import validate

from scheduler import Scheduler
from queue_helper import Queue

class Main(object):

    payloadSchema = {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": [
                    "add",
                    "delete"
                ]
            },
            "id": {
                "type": "string",
            },
            "server": {
                "type": "string",
            },
            "scheduledDateTime": {
                "type": "string",
                "format": "date-time"
            }
        },
        "allOf": [
            {
                "if": {
                    "properties": {
                        "action": {
                            "const": "delete"
                        }
                    }   
                },
                "then": {
                    "required": [
                        "id"
                    ]
                }
            },
            {
                "if": {
                    "properties": {
                        "action": {
                            "const": "add"
                        }
                    }   
                },
                "then": {
                    "required": [
                        "server",
                        "scheduledDateTime"
                    ]
                }
            }
        ],
        "required": [
            "action"
        ],
        "additionalProperties": False
    }

    def __init__(self) -> None:
        print("Starting Scheduler...")
        schedule = Scheduler()
        schedule.start()

        print("Starting Queue Listener...")
        myqueue = Queue(self.process_message)
        myqueue.start()

    def process_message(self, channel, method, properties, body):
        print(" [x] Received %r" % body)

        if self.message_valid(body):
            payload = json.loads(body)
            print(payload)

    def message_valid(self, message):
        try:
            payload = json.loads(message)
        except ValueError as err:
            print("Invalid JSON message: " + message.decode("utf8"))
            return False

        try:
            validate(
                instance=payload,
                schema=self.payloadSchema,
                format_checker=jsonschema.FormatChecker()
            )
        except Exception as err:
            print("Invalid message passed: " + err.message)
            print("Payload: " + json.dumps(payload))
            return False

        return True        
        
if __name__ == "__main__":
    main = Main()