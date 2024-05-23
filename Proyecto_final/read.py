import socket

# Server details
server_ip = '127.0.0.1'  # Replace with your server's IP address or ngrok URL
server_port = 5050

def read_data():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((server_ip, server_port))
        print(f"Connected to {server_ip}:{server_port}")
        while True:
            data = s.recv(1024).decode('utf-8')
            if not data:
                break
            print(data)

if __name__ == '__main__':
    read_data()
