import praw
import boto3
import os
import json
import io  # To use StringIO for in-memory data
import csv  # To create CSV format
from datetime import datetime
from botocore.exceptions import NoCredentialsError

def get_secret():
    secret_name = "redddit-user-secret" 
    region_name = "eu-north-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret
    except Exception as e:
        print(f"Error retrieving secret: {e}")
    return None

# Fetch the secrets
secrets = get_secret()
if not secrets:
    raise NoCredentialsError("Could not load secrets")

# Set up Reddit API using secrets
reddit = praw.Reddit(client_id=secrets['reddit_client_id'],
                     client_secret=secrets['reddit_client_secret'],
                     user_agent=secrets['reddit_user_agent'])

# Set up AWS environment variables
os.environ['AWS_ACCESS_KEY_ID'] = secrets['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY'] = secrets['aws_secret_access_key']
os.environ['AWS_DEFAULT_REGION'] = secrets['aws_default_region']

# Initialize S3 client
s3 = boto3.client('s3', region_name='eu-north-1')
bucket_name = 'reddit-batch-data-bde'  # Ensure this is a unique bucket name
filename = f"reddit_batch_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"  # Dynamic filename with timestamp

# Fetch top 20,000 posts from subreddit
subreddit = reddit.subreddit('learnpython')
subreddit_data = []

for post in subreddit.hot(limit=30000):
    data = {
        'title': post.title,
        'score': post.score,
        'url': post.url,
        'comments': post.num_comments
    }
    subreddit_data.append(data)

# Use StringIO to create an in-memory CSV file
csv_buffer = io.StringIO()
csv_writer = csv.DictWriter(csv_buffer, fieldnames=['title', 'score', 'url', 'comments'])
csv_writer.writeheader()

# Write only the first 100 posts to the CSV for testing purposes
for data in subreddit_data[:100]:  # You can change this limit as needed
    csv_writer.writerow(data)

## Upload the CSV file directly from memory to S3
try:
    s3.put_object(Bucket=bucket_name, Key=filename, Body=csv_buffer.getvalue())
    print(f"Uploaded {filename} to S3 bucket {bucket_name}")
except Exception as e:
    print(f"Error uploading file to S3: {e}")

# Kinesis setup (this part remains unchanged)
kinesis = boto3.client('kinesis', region_name='eu-north-1')

# Stream Reddit data to Kinesis
for post in subreddit.hot(limit=10):
    data = {
        'title': post.title,
        'score': post.score,
        'url': post.url,
        'comments': post.num_comments
    }
    kinesis.put_record(
        StreamName='reddit-stream',
        Data=json.dumps(data),
        PartitionKey='partition_key'
    )

# Kinesis: Get stream description and print Shard IDs
response = kinesis.describe_stream(StreamName='reddit-stream')
shards = response['StreamDescription']['Shards']
for shard in shards:
    print(f"Shard ID: {shard['ShardId']}")
