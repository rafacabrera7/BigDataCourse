import praw
import re
import time
import socket
import threading

# Reddit API credentials
client_id = 'NRnx0GZmntTbXO7Kk36MSg'
client_secret = 'Lm-XKgoPpQMaiBEgJk-Faul_qmR5Tw'
user_agent = '0LaUwU0'

# Set up PRAW with Reddit API credentials
reddit = praw.Reddit(client_id=client_id,
                     client_secret=client_secret,
                     user_agent=user_agent)

# Socket server details
host = "0.0.0.0"
port = 5050

def clean_text(text):
    """Remove links, and special characters."""
    clean_text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    clean_text = re.sub(r'[^A-Za-z0-9\s,.?!\'\"]+', '', text)
    return clean_text

def fetch_comments(submission):
    """Fetch comments from a submission."""
    submission.comments.replace_more(limit=None)
    comments = submission.comments.list()
    cleaned_comments = []
    for comment in comments:
        if comment.body.strip():  # Check if comment body is not empty or whitespace
            cleaned_comments.append(clean_text(comment.body))
    return cleaned_comments

def fetch_new_posts(subreddit_name, limit=50):
    """Fetch new posts from a subreddit."""
    subreddit = reddit.subreddit(subreddit_name)
    new_posts = subreddit.new(limit=limit)
    
    cleaned_posts = []
    for post in new_posts:
        if post.selftext.strip():  # Check if selftext is not empty
            post_info = {
                'title': post.title,
                'selftext': clean_text(post.selftext),
                'comments': fetch_comments(post)
            }
            cleaned_posts.append(post_info)
    return cleaned_posts

def send_data_to_client(client_socket, subreddit_name, polling_interval):
    while True:
        try:
            print(f"Fetching new posts from r/{subreddit_name}...")
            posts = fetch_new_posts(subreddit_name)
            for post in posts:
                data = f"Title: {post['title']}\nContent: {post['selftext']}\nComments:\n"
                for comment in post['comments']:
                    data += f"- {comment}\n"
                data += "\n"
                client_socket.send(data.encode('utf-8'))
            print("Waiting for the next polling interval...")
            time.sleep(polling_interval)
        except ConnectionResetError:
            print("Client disconnected.")
            break
        except Exception as e:
            print(f"Error: {e}")
            break

def handle_client(client_socket, client_address):
    print(f"Client connected from {client_address}")
    subreddit_name = 'videogames'  # Replace with your subreddit
    polling_interval = 10  # Poll every 10 seconds
    send_data_to_client(client_socket, subreddit_name, polling_interval)
    client_socket.close()
    print(f"Client disconnected from {client_address}")

def start_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen()
    print(f"Server listening on {host}:{port}")
    
    while True:
        client_socket, client_address = sock.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
        client_thread.start()

if __name__ == '__main__':
    start_server()


