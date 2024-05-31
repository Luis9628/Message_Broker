import grpc
import logging
import message_broker_pb2
import message_broker_pb2_grpc

def run_client():
    try:
        with grpc.insecure_channel('localhost:50051') as channel:
            try:
                grpc.channel_ready_future(channel).result(timeout=10)  
                stub = message_broker_pb2_grpc.MessageBrokerStub(channel)
                print("Cliente gRPC iniciado. Puede comenzar a enviar mensajes.")
                logging.info("Cliente gRPC conectado exitosamente al servidor.")

                while True:
                    # Seleccionar si es publicador, subscriptor o salir
                    user_type = input("Seleccione 'p' para publicador, 's' para subscriptor o 'q' para salir: ")
                    if user_type == 'p':
                        name = input("Ingrese su nombre: ")
                        publish_menu(stub, name)
                    elif user_type == 's':
                        name = input("Ingrese su nombre: ")
                        subscribe_menu(stub, name)
                    elif user_type == 'q':
                        break
                    else:
                        print("Opción no válida. Inténtelo de nuevo.")
            except grpc.FutureTimeoutError:
                logging.error("No se pudo conectar con el servidor: Tiempo de espera agotado.")
                print("No se pudo conectar con el servidor: Tiempo de espera agotado.")
    except grpc.RpcError as e:
        logging.error(f"Error al conectar con el servidor: {e}")
        print(f"Error al conectar con el servidor: {e}")

def publish_menu(stub, name):
    while True:
        action = input(
            "Ingrese 'p' para publicar un mensaje o 'q' para salir: ")
        if action == 'p':
            topic = input("Ingrese el tema al que desea publicar el mensaje: ")
            message = input("Ingrese el mensaje a publicar: ")
            publish_message(stub, topic, message, name)
        elif action == 'q':
            return
        else:
            print("Opción no válida. Inténtelo de nuevo.")

def publish_message(stub, topic, message, name):
    try:
        request = message_broker_pb2.PublishRequest(topic=topic, message=message)
        response = stub.Publish(request)
        if response.status == "Cola llena":
            logging.warning("Cola llena. No se pudo publicar el mensaje.")
        else:
            print("Mensaje publicado exitosamente.")
    except grpc.RpcError as e:
        logging.error(f"Error de red: {e.code()} - {e.details()}")
        print(f"Error de red: {e.code()} - {e.details()}")

def subscribe_menu(stub, name):
    while True:
        action = input(
            "Ingrese 's' para suscribirse a un tema o 'q' para salir: ")
        if action == 's':
            topic = input("Ingrese el tema al que desea suscribirse: ")
            subscribe_to_topic(stub, topic, name)
        elif action == 'q':
            return
        else:
            print("Opción no válida. Inténtelo de nuevo.")

def subscribe_to_topic(stub, topic, name):
    try:
        request = message_broker_pb2.SubscribeRequest(topic=topic)
        responses = stub.Subscribe(request)
        received_messages = False
        for response in responses:
            print(f"Mensaje recibido de {response.sender}: {response.message}")
            received_messages = True
        if not received_messages:
            print("No hay mensajes en cola para este tema.")
        logging.info(f"Suscripción exitosa al tema {topic}.")
    except grpc.RpcError as e:
        logging.error(f"Error de red: {e.code()} - {e.details()}")
        print(f"Error de red: {e.code()} - {e.details()}")

if __name__ == '__main__':
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler('client.log')  # Cambiar el nombre del archivo si es necesario
    formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%d/%m/%Y:%H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    run_client()
