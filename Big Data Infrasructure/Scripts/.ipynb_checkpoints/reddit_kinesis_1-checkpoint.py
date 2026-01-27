import os
import json
import time
from datetime import datetime, timedelta  # Add timedelta for time calculations
import praw
import boto3
from dotenv import load_dotenv

# Step 1: Load environment variables from .env file
load_dotenv()

# Fetch Reddit API credentials from environment variables
reddit_client_id = os.getenv('REDDIT_CLIENT_ID')
reddit_client_secret = os.getenv('REDDIT_CLIENT_SECRET')
reddit_user_agent = os.getenv('REDDIT_USER_AGENT')

# Step 2: Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name='eu-north-1')

# Step 3: Initialize Reddit API connection
reddit = praw.Reddit(
    client_id=reddit_client_id,
    client_secret=reddit_client_secret,
    user_agent=reddit_user_agent
)
  
# Step 4: Choose subreddit to stream from
subreddit = reddit.subreddit("all")

def stream_to_kinesis(submission):
    # Debugging: Print the raw epoch time
    print(f"Raw created_utc: {submission.created_utc}")
    
    # Convert created_utc to a readable datetime format if valid
    if submission.created_utc:
        created_time = datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S')
    else:
        created_time = None
        print(f"Submission {submission.id} has no created_utc timestamp.")
    
    data = {
        'id': submission.id,
        'author': str(submission.author),
        'title': submission.title,
        'subreddit': str(submission.subreddit),
        'created_time': created_time,  # Use the converted datetime
        'score': submission.score,
        'num_comments': submission.num_comments,
        'is_self_post': submission.is_self,
        'flair_text': submission.link_flair_text,
        'upvote_ratio': submission.upvote_ratio,
        'edited': submission.edited,
        'over_18': submission.over_18,
        'thumbnail': submission.thumbnail,
        'stickied': submission.stickied
    }
        
    # Debugging: Print the data before sending
    print(f"Data to send: {data}")

    response = kinesis_client.put_record(
        StreamName='reddit-bde',  # Use the Kinesis stream name
        Data=json.dumps(data),
        PartitionKey=submission.id
    )
    print(f"Sent data to Kinesis: {response}")

# Step 5: Stream new submissions from subreddit for 2 hours
start_time = datetime.now()
max_duration = timedelta(hours=1)  # Run the script for 2 hours

for submission in subreddit.stream.submissions():
    stream_to_kinesis(submission)
    
    # Break the loop if 2 hours have passed
    if datetime.now() - start_time > max_duration:
        print("Stopping script after running for 1 hours.")
        break

    time.sleep(1)  # Adjust the delay if needed
