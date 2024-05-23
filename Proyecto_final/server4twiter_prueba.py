import socket
import threading
import csv
import time
from datetime import datetime

# Define the host and port
host = "0.0.0.0"
port = 5050

# File path and delimiter for CSV
file_path = 'gaming_gaming.csv'
delimiter = ','

# Create the socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the host and port
sock.bind((host, port))

# Listen for incoming connections
sock.listen()

# Function to send a line over the socket
def send_line(line, client_socket):
    client_socket.send(line.encode('utf-8'))

# Function to handle each client connection
def handle_client(client_socket, client_address):
    print(f"Client connected from {client_address}")
    
    try:
        with open(file_path, newline='') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header row
            
            for line in reader:
                timestamp = datetime.fromtimestamp(float(line[5])).strftime('%Y-%m-%d %H:%M:%S')
                formatted_line = f"{timestamp},{line[6]},{line[7]},{line[8]},{line[9]},{line[10]}\n"
                
                send_line(formatted_line, client_socket)
                #time.sleep(1)  # Adjust sleep time as needed
                
    except Exception as e:
        print(f"Error: {e}")
    
    client_socket.close()
    print(f"Client disconnected from {client_address}")

# Accept incoming connections and handle each client in a separate thread
while True:
    print("Waiting for connections...")
    client, address = sock.accept()
    client_thread = threading.Thread(target=handle_client, args=(client, address))
    client_thread.start()
