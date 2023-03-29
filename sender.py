import pika
import ast


class MetaClass(type):
    _instance = {}

    def __call__(cls, *args, **kwargs):
        """Singleton Design Pattern"""

        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
            return cls._instance[cls]


class RabbitmqConfigure(metaclass=MetaClass):
    def __init__(
        self,
        host="localhost",
        routingKey="phero_assignemnts",
        exchange="assignment",
        exchange_type="direct",
    ):
        """Configure Rabbit Mq Server"""

        self.host = host
        self.routingKey = routingKey
        self.exchange = exchange
        self.exchange_type = exchange_type


class RabbitMq:
    def __init__(self, server):
        """
        :param server: Object of class RabbitmqConfigure
        """

        self.server = server

        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.server.host))
        self._channel = self._connection.channel()
        # self._channel.queue_declare(queue=self.server.queue)
        self._channel.exchange_declare(exchange=server.exchange, exchange_type=server.exchange_type)

    def publish(self, payload={}):
        """
        :param payload: JSON payload
        :return: None
        """

        self._channel.basic_publish(
            exchange=self.server.exchange, routing_key=self.server.routingKey, body=str(payload)
        )

        print("Published Message: {}".format(payload))
        self._connection.close()


if __name__ == "__main__":
    server = RabbitmqConfigure(host="localhost", routingKey="assignments.report", exchange="assignments")

    rabbitmq = RabbitMq(server)
    rabbitmq.publish(payload={"Data": 22})
