import praw
import time
import re

# Reddit API credentials
client_id = 'NRnx0GZmntTbXO7Kk36MSg'
client_secret = 'Lm-XKgoPpQMaiBEgJk-Faul_qmR5Tw'
user_agent = '0LaUwU0'

# Set up PRAW with Reddit API credentials
reddit = praw.Reddit(client_id=client_id,
                     client_secret=client_secret,
                     user_agent=user_agent)

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
            comment_info = {
                'comment_body': clean_text(comment.body)
            }
            cleaned_comments.append(comment_info)
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
                'selftext': clean_text(post.selftext)
                #'comments': fetch_comments(post)
            }
            cleaned_posts.append(post_info)
    return cleaned_posts

def main():
    subreddit_name = 'videogames'  # Replace with your subreddit
    polling_interval = 1  # Poll every 10 seconds

    while True:
        print(f"Fetching new posts from r/{subreddit_name}...")
        posts = fetch_new_posts(subreddit_name)
        for post in posts:
            print(f"Title: {post['title']}")
            print(f"Content: {post['selftext']}")
            print("Comments:")
            #for comment in post['comments']:
            #    print(f"- {comment['comment_body']}\n")
        
        print("Waiting for the next polling interval...")
        time.sleep(polling_interval)

if __name__ == '__main__':
    main()
