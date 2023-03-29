import pika
import ast


class MetaClass(type):
    _instance = {}

    def __call__(cls, *args, **kwargs):
        """Singleton Design Pattern"""

        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
            return cls._instance[cls]


class RabbitMqServerConfigure(metaclass=MetaClass):
    def __init__(self, host="localhost", queue="assignments", exchange="assignments", routing_key="assignments.report"):
        """Server initialization"""

        self.host = host
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key


class RabbitmqServer:
    def __init__(self, server):
        """
        :param server: Object of class RabbitMqServerConfigure
        """

        self.server = server
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.server.host))
        self._channel = self._connection.channel()
        queue = self._channel.queue_declare(queue=self.server.queue)
        queue_name = queue.method.queue
        self._channel.queue_bind(exchange=self.server.exchange, queue=queue_name, routing_key=self.server.routing_key)
        print("Server started waiting for Messages ")

    @staticmethod
    def callback(ch, method, properties, body):
        Payload = body.decode("utf-8")
        Payload = ast.literal_eval(Payload)
        print("Data Received : {}".format(Payload))

    def start_server(self):
        self._channel.basic_consume(queue=self.server.queue, on_message_callback=RabbitmqServer.callback, auto_ack=True)
        self._channel.start_consuming()


if __name__ == "__main__":
    server_configure = RabbitMqServerConfigure(
        host="localhost", queue="assignments", exchange="assignments", routing_key="assignments.report"
    )

    server = RabbitmqServer(server=server_configure)
    server.start_server()
