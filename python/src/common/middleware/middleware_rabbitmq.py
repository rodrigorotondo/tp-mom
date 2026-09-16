import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    
    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=queue_name)

    def close(self):
        self.connection.close()

    def send(self,message):
        self.channel.basic_publish(exchange='',routing_key=self.queue_name,body=message)


    #Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    #cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    # message - El valor tal y como lo recibe el método send de esta clase.
    # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            delivery_tag = method.delivery_tag
            def ack():
                ch.basic_ack(delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag, requeue=True)
            
            on_message_callback(body,ack,nack)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback,auto_ack=False)
        self.channel.start_consuming()
        
        

    def stop_consuming(self):
        self.channel.stop_consuming()


        

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=exchange_name, exchange_type="direct")
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        for routing_key in routing_keys:
            self.channel.queue_bind(exchange=exchange_name,queue=self.queue_name,routing_key=routing_key)
        
    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            delivery_tag = method.delivery_tag
            def ack():
                ch.basic_ack(delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag, requeue=True)
            
            on_message_callback(body,ack,nack)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback,auto_ack=False)
        self.channel.start_consuming()

    def stop_consuming(self):
            self.channel.stop_consuming()
    def send(self,message):
        for topic in self.routing_keys:
            self.channel.basic_publish(exchange=self.exchange_name,routing_key=topic,body=message)
    def close(self):
        self.connection.close()