import praw
import time

# Reddit API credentials
client_id = 'wdp4PcKa2YYOJYW3dedKEw'
client_secret = 'UQPwGxrJSrdlHxuJ9fJgbJJoJoI3eA'
user_agent = 'rafacabrera7'

# Set up PRAW with Reddit API credentials
reddit = praw.Reddit(client_id=client_id,
                     client_secret=client_secret,
                     user_agent=user_agent)

def fetch_new_posts(subreddit_name, limit=10):
    """Fetch new posts from a subreddit."""
    subreddit = reddit.subreddit(subreddit_name)
    new_posts = subreddit.new(limit=limit)
    posts = []
    for post in new_posts:
        post_info = {
            'title': post.title,
            'author': post.author.name if post.author else 'N/A',
            'created_utc': post.created_utc,
            'selftext': post.selftext,
            'url': post.url
        }
        posts.append(post_info)
    return posts

def main():
    subreddit_name = 'announcements'  # Replace with your subreddit
    polling_interval = 10  # Poll every 10 seconds

    while True:
        print(f"Fetching new posts from r/{subreddit_name}...")
        posts = fetch_new_posts(subreddit_name)
        for post in posts:
            print(f"Title: {post['title']}")
            print(f"Author: {post['author']}")
            print(f"Created (UTC): {post['created_utc']}")
            print(f"Content: {post['selftext']}")
            print(f"URL: {post['url']}\n")
        
        print("Waiting for the next polling interval...")
        time.sleep(polling_interval)

if __name__ == '__main__':
    main()
