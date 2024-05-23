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

def fetch_new_posts(subreddit_name, sent_posts, limit=5):
    """Fetch new posts from a subreddit, excluding already sent posts."""
    subreddit = reddit.subreddit(subreddit_name)
    new_posts = subreddit.new(limit=limit)
    
    cleaned_posts = []
    for post in new_posts:
        if post.id not in sent_posts and post.selftext.strip():  # Check if post is not already sent and has non-empty selftext
            post_info = {
                'id': post.id,
                'title': post.title,
                'selftext': clean_text(post.selftext)
            }
            cleaned_posts.append(post_info)
            sent_posts.add(post.id)
    return cleaned_posts

def send_line(line, client_socket):
    client_socket.send((line + "\n").encode('utf-8'))

def handle_client(client_socket, client_address):
    print(f"Client connected from {client_address}")
    subreddit_name = 'videogames'  # Replace with your desired subreddit
    polling_interval = 30  # Poll every 30 seconds
    sent_posts = set()  # Set to keep track of sent post IDs

    try:
        while True:
            print(f"Fetching new posts from r/{subreddit_name}...")
            posts = fetch_new_posts(subreddit_name, sent_posts)
            for post in posts:
                line = f"{post['title']} {post['selftext']}"
                send_line(line, client_socket)
                print(f'Sent to {client_address}:\n{line}')
            print("Waiting for the next polling interval...")
            time.sleep(polling_interval)
    except ConnectionResetError:
        print(f"Client disconnected from {client_address}")
    finally:
        client_socket.close()

# Accept incoming connections and handle each client in a separate thread
while True:
    print("Waiting for incoming connections...")
    client, address = sock.accept()
    t = threading.Thread(target=handle_client, args=(client, address))
    t.start()