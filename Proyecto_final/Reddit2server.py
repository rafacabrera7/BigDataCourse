import socket
import threading
import time
import praw
import re
# Reddit API credentials
client_id = 'NRnx0GZmntTbXO7Kk36MSg'
client_secret = 'Lm-XKgoPpQMaiBEgJk-Faul_qmR5Tw'
user_agent = '0LaUwU0'

# Set up PRAW with Reddit API credentials
reddit = praw.Reddit(client_id=client_id,
                     client_secret=client_secret,
                     user_agent=user_agent)

# Define host and port
host = "0.0.0.0"
port = 5050

# Create the socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the host and port
sock.bind((host, port))

# Listen for incoming connections
sock.listen()

def clean_text(text):
    """Remove links and special characters."""
    clean_text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    clean_text = re.sub(r'[^A-Za-z0-9\s,.?!\'\"]+', '', text)
    return clean_text

def fetch_new_posts(subreddit_name, limit=50):
    """Fetch new posts from a subreddit."""
    subreddit = reddit.subreddit(subreddit_name)
    new_posts = subreddit.new(limit=limit)
    
    cleaned_posts = []
    for post in new_posts:
        if post.selftext.strip():  # Check if selftext is not empty
            post_info = {
                'title': post.title,
                'selftext': clean_text(post.selftext)
            }
            cleaned_posts.append(post_info)
    return cleaned_posts

def send_line(line, client_socket):
    client_socket.send((line).encode('utf-8'))

def manejar_cliente(client_socket, client_address):
    print(f"Cliente conectado desde {client_address}")
    subreddit_name = 'videogames'  # Replace with your subreddit
    polling_interval = 30  # Poll every 10 seconds

    try:
        while True:
            print(f"Fetching new posts from r/{subreddit_name}...")
            posts = fetch_new_posts(subreddit_name)
            for post in posts:
                line = f"{post['title']} {post['selftext']}"
                #line = "{"+line+"}"
                send_line(line, client_socket)
                print(f'Sent to {client_address}:\n{line}')
            print("Waiting for the next polling interval...")
            time.sleep(polling_interval)
    except ConnectionResetError:
        print(f"Cliente desconectado desde {client_address}")
    finally:
        client_socket.close()

# Accept incoming connections and handle each client in a separate thread
while True:
    print("Esperando conexiones entrantes...")
    client, address = sock.accept()
    t = threading.Thread(target=manejar_cliente, args=(client, address))
    t.start()
