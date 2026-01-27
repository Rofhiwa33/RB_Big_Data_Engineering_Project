import os
import json
import time
from datetime import datetime  
import praw
import boto3
from dotenv import load_dotenv

# Step 1: Load environment variables from .env file
load_dotenv()

# Fetch Reddit API credentials from environment variables 
reddit_client_id = os.getenv('') 
reddit_client_secret = os.getenv('')
reddit_user_agent = os.getenv('')

# Step 2: Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name='eu-north-1')

# Step 3: Initialize Reddit API connection
reddit = praw.Reddit(
    client_id=reddit_client_id,
    client_secret=reddit_client_secret,
    user_agent=reddit_user_agent
)

# Step 4: Choose subreddit to stream from
subreddit = reddit.subreddit("data")

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
        'created_time': created_time,  
        'score': submission.score,
        'num_comments': submission.num_comments
    }

    # Debugging: Print the data before sending
    print(f"Data to send: {data}")

    response = kinesis_client.put_record(
        StreamName='Reddit',  # Use the Kinesis stream name - mine is "Reddit"
        Data=json.dumps(data),
        PartitionKey=submission.id
    )
    print(f"Sent data to Kinesis: {response}")

# Step 5: Stream new submissions from subreddit
for submission in subreddit.stream.submissions():
    stream_to_kinesis(submission)
    time.sleep(1)  