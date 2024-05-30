import grpc
from concurrent import futures
import threading
import logging
import time

import message_broker_pb2
import message_broker_pb2_grpc

class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

class MessageBrokerServicer(message_broker_pb2_grpc.MessageBrokerServicer):

    def __init__(self):
        self.topics = {"Tema 1": [], "Tema 2": [], "Tema 3": []}
        self.locks = {topic: threading.Lock() for topic in self.topics}
        self.cond_vars = {topic: threading.Condition(lock) for topic, lock in self.locks.items()}
        self.subscribers = {topic: [] for topic in self.topics}
        self.queue_timers = {topic: None for topic in self.topics}

    def Publish(self, request, context):
        topic = request.topic
        message = request.message
        peer = context.peer()  # Obtener información del cliente
        logging.info(f"Publicación desde {peer} en el tema {topic}: {message}")

        with self.locks[topic]:
            if len(self.topics[topic]) >= 5:
                logging.warning(f"Cola llena para el tema {topic}")
                return message_broker_pb2.PublishResponse(status="Cola llena")
            self.topics[topic].append(message)
            logging.info(f"Mensaje enviado al tema {topic}")
            return message_broker_pb2.PublishResponse(status="Mensaje publicado")

    def Subscribe(self, request, context):
        topic = request.topic
        peer = context.peer()  # Obtener información del cliente
        logging.info(f"Suscripción desde {peer} al tema {topic}")

        if topic in self.topics:
            with self.locks[topic]:
                self.subscribers[topic].append(peer)
                # Si hay mensajes en cola, se envían hasta 5 mensajes
                if self.topics[topic]:
                    messages = self.topics[topic][:2]
                    del self.topics[topic][:2]
                    for message in messages:
                        yield message_broker_pb2.SubscribeResponse(sender=name, message=message)
                    logging.info(f"Mensajes enviados a los subscriptores del tema {topic}")
        else:
            logging.warning(f"Tema no encontrado {topic}")
            context.abort(grpc.StatusCode.NOT_FOUND, "Tema no encontrado")

    def ValidateConnection(self, request, context):
        peer = context.peer()  # Obtener información del cliente
        ip_port = peer.split(':')[-1]  # Extraer el puerto
        logging.info(f"Cliente conectado desde el puerto {ip_port}")
        return message_broker_pb2.ValidateConnectionResponse(status="Conexión validada")

    def empty_queue(self, topic):
        with self.locks[topic]:
            del self.topics[topic][:]
            logging.info(f"La cola para el tema {topic} se ha vaciado debido a la falta de suscriptores.")

def serve():
    print("Servidor gRPC iniciado...")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    message_broker_pb2_grpc.add_MessageBrokerServicer_to_server(MessageBrokerServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    logging.info("Servidor iniciado")
    server.wait_for_termination()

if __name__ == '__main__':
    # Configuración del logging con FlushFileHandler
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = FlushFileHandler('server.log')
    formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%d/%m/%Y:%H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    serve()