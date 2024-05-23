import csv
import socket
import threading
from datetime import datetime
import time

host = "0.0.0.0"
port = 5050

# Función para enviar una línea de texto por el socket
def send_line(line, client_socket):
    client_socket.send(line.encode('utf-8'))

# Función que maneja cada conexión de cliente
def manejar_cliente(client_socket, client_address):
    print(f"Cliente conectado desde {client_address}")
    try:
        with open('gaming_gaming.csv', newline='') as file:
            reader = csv.reader(file)
            # Leer la primera línea para tener una referencia de tiempo inicial
            next(reader)  # Ignorar la primera línea si contiene encabezados
            for line in reader:
                # Formatea los datos según sea necesario para enviarlos por el socket
                data_to_send = ','.join(line) + '\n'
                # Enviar la línea por el socket
                send_line(data_to_send, client_socket)
                # Esperar un tiempo si es necesario antes de enviar la próxima línea
                time.sleep(1)  # Ajusta el tiempo según sea necesario
    except Exception as e:
        print(f'Error: {e}')
    finally:
        client_socket.close()
        print(f"Cliente desconectado desde {client_address}")

# Crea el socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Vincula el socket al host y puerto
sock.bind((host, port))

# Escucha por conexiones entrantes
sock.listen()

# Acepta conexiones entrantes y maneja cada cliente en un hilo separado
while True:
    print(f"Esperando conexiones ...")
    client, address = sock.accept()
    t = threading.Thread(target=manejar_cliente, args=(client, address))
    t.start()