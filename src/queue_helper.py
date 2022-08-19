from gc import callbacks
import queue
import pika

class Queue(object):

    def __init__(self, queue, callback):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue)
        self.channel.basic_consume(
            queue=queue,
            auto_ack=True,
            on_message_callback=callback
        )

    def start(self):
        print("Waiting for messages...")
        self.channel.start_consuming()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        print("Shuting down Queue...")
        self.connection.close()

    def send_job(self, server):
        print(server)