import socket

# Configuración del servidor y puerto
#host = 'localhost'
#port = 5050

host = '4.tcp.ngrok.io'
port = 19099


# Crear el socket del cliente
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Conectarse al servidor
client_socket.connect((host, port))

while True:
    # Recibir datos del servidor
    data = client_socket.recv(1024)
    if not data:
        # Si no hay datos, la conexión se ha cerrado
        print("Conexión cerrada por el servidor.")
        break
    # Decodificar los datos y mostrarlos en la consola
  #  print("Datos recibidos: ", data.decode('utf-8'))
    print("Datos recibidos: ", data)

# Cerrar el socket del cliente
client_socket.close()

