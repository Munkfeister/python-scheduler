from gc import callbacks
import queue
import pika

class Queue(object):
    queue = "rhel-patching-scheduler"

    def __init__(self, callback):
        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.queue)
        self.channel.basic_consume(
            queue=self.queue,
            auto_ack=True,
            on_message_callback=callback
        )

    def start(self):
        print("Waiting for messages...")
        self.channel.start_consuming()
