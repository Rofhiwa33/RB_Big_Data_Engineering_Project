import boto3
import json
import re
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
from textblob import TextBlob
import numpy as np
from decimal import Decimal
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Custom stopwords list
stop_words = {
    'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', 'aren\'t', 'as',
    'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'can\'t', 'cannot',
    'could', 'couldn\'t', 'did', 'didn\'t', 'do', 'does', 'doesn\'t', 'doing', 'don\'t', 'down', 'during', 'each',
    'few', 'for', 'from', 'further', 'had', 'hadn\'t', 'has', 'hasn\'t', 'have', 'haven\'t', 'having', 'he', 'he\'d',
    'he\'ll', 'he\'s', 'her', 'here', 'here\'s', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'how\'s', 'i', 
    'i\'d', 'i\'ll', 'i\'m', 'i\'ve', 'if', 'in', 'into', 'is', 'isn\'t', 'it', 'it\'s', 'its', 'itself', 'let\'s',
    'me', 'more', 'most', 'mustn\'t', 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 
    'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', 'shan\'t', 'she', 'she\'d', 'she\'ll', 
    'she\'s', 'should', 'shouldn\'t', 'so', 'some', 'such', 'than', 'that', 'that\'s', 'the', 'their', 'theirs', 
    'them', 'themselves', 'then', 'there', 'there\'s', 'these', 'they', 'they\'d', 'they\'ll', 'they\'re', 'they\'ve', 
    'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', 'wasn\'t', 'we', 'we\'d', 'we\'ll',
    'we\'re', 'we\'ve', 'were', 'weren\'t', 'what', 'what\'s', 'when', 'when\'s', 'where', 'where\'s', 'which', 'while', 
    'who', 'who\'s', 'whom', 'why', 'why\'s', 'with', 'won\'t', 'would', 'wouldn\'t', 'you', 'you\'d', 'you\'ll', 
    'you\'re', 'you\'ve', 'your', 'yours', 'yourself', 'yourselves'
}

# Set up AWS clients
kinesis_client = boto3.client('kinesis', region_name='eu-north-1')  
dynamodb_client = boto3.resource('dynamodb', region_name='eu-north-1')  

kinesis_stream_name = 'reddit-bde' 
dynamodb_table_name = 'tbl_reddit_processed' 

dynamodb_table = dynamodb_client.Table(dynamodb_table_name)

# Initialize a dictionary to track author activity
author_activity = {}

# List to store anomalies
anomalies = []

def preprocess_record(record):
    """
    Preprocess a single record from Kinesis data.
    Applies all preprocessing steps to the incoming data.
    """
    # Convert created_time to ISO 8601 string
    created_time_str = record['created_time']
    if isinstance(created_time_str, str):
        created_time_obj = datetime.strptime(created_time_str, '%Y-%m-%d %H:%M:%S')
        
        # Make it aware if it's naive
        if created_time_obj.tzinfo is None:
            created_time_obj = created_time_obj.replace(tzinfo=timezone.utc)
    else:
        created_time_obj = created_time_str  # Assuming it's already a datetime object

    record['created_time'] = created_time_obj.strftime('%Y-%m-%d %H:%M:%S')
    
    # Ensure score and num_comments have default values
    record['score'] = record.get('score', 0)
    record['num_comments'] = record.get('num_comments', 0)
    
    # Normalize title and flair text by converting to lowercase
    record['title'] = record['title'].lower()
    if record.get('flair_text'):
        record['flair_text'] = record['flair_text'].lower()
    
    # Remove punctuation from title
    record['title'] = re.sub(r'[^\w\s]', '', record['title'])
    
    # Tokenize title and remove stopwords
    record['title_tokens'] = [word for word in record['title'].split() if word not in stop_words]
    
    # Sentiment analysis on title
    blob = TextBlob(record['title'])
    record['sentiment'] = blob.sentiment.polarity  # Ranges from -1 (negative) to 1 (positive)
    
    # Calculate post age in minutes
    post_age_minutes = (datetime.now(timezone.utc) - created_time_obj).total_seconds() / 60
    record['post_age_minutes'] = post_age_minutes
    
    # Create popularity score
    record['popularity_score'] = (record['score'] * record.get('upvote_ratio', 0)) + (record['num_comments'] * 0.5)
    
    # Determine if post is media or text
    record['post_type'] = 'media' if record.get('thumbnail') != 'self' else 'text'
    
    # Determine time of day
    record['time_of_day'] = 'day' if 6 <= created_time_obj.hour < 18 else 'night'
    
    # Track author activity (incremental tracking, store in a separate dictionary)
    author_activity[record['author']] = author_activity.get(record['author'], 0) + 1
    record['author_activity_count'] = author_activity[record['author']]
    
    return record

def detect_anomalies(data):
    # Convert DataFrame to ensure correct data types
    df = pd.DataFrame(data)
    
    # Convert relevant columns from Decimal to float
    df['score'] = df['score'].astype(float)
    df['num_comments'] = df['num_comments'].astype(float)
    df['popularity_score'] = df['popularity_score'].astype(float)

    # Calculate Z-scores for relevant columns
    for column in ['score', 'num_comments', 'popularity_score']:
        z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
        anomaly_indices = np.where(z_scores > 3)[0]  # Z-score > 3 is an anomaly

        for index in anomaly_indices:
            print(f"Anomaly detected in record {df.iloc[index]['id']}: {df.iloc[index]}")

def get_records_from_kinesis(shard_iterator):
    """Retrieve records from the Kinesis stream."""
    response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=100)
    return response['Records'], response['NextShardIterator']

def process_data(records):
    """
    Process and preprocess the data from Kinesis records.
    """
    data = []
    for record in records:
        # Decode Kinesis data (assuming JSON format)
        payload = json.loads(record['Data'])
        
        # Preprocess each record
        processed_record = preprocess_record(payload)
        
        # Save to DynamoDB
        save_to_dynamodb(processed_record)
        
        # Log the processed record to the console
        logging.info("Processed Record: %s", processed_record)
        
        # Add processed record to the list
        data.append(processed_record)
    
    return data

def save_to_dynamodb(record):
    """
    Save a processed record to DynamoDB, converting float types to Decimal.
    """
    # Convert float values in the record to Decimal
    for key, value in record.items():
        if isinstance(value, float):
            record[key] = Decimal(str(value))  

    try:
        # Create a new item in the DynamoDB table
        dynamodb_table.put_item(Item=record)
        print(f"Saved to DynamoDB: {record['id']}")
    except Exception as e:
        print(f"Error saving to DynamoDB: {e}")

def main():
    # Get the stream description to retrieve the shard ID
    response = kinesis_client.describe_stream(StreamName=kinesis_stream_name)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']

    # Get the initial shard iterator
    shard_iterator_response = kinesis_client.get_shard_iterator(
        StreamName=kinesis_stream_name,
        ShardId=shard_id,
        ShardIteratorType='LATEST'  # 'TRIM_HORIZON' to read all data from the beginning
    )
    
    shard_iterator = shard_iterator_response['ShardIterator']

    # Set the start time for the 2-minutes limit
    start_time = datetime.now(timezone.utc)
    time_limit = timedelta(minutes=55)

    while True:
        # Check if the time limit has been reached
        if datetime.now(timezone.utc) - start_time >= time_limit:
            logging.info("2-minutes processing time limit reached. Exiting.")
            break

        # Get records from Kinesis
        records, shard_iterator = get_records_from_kinesis(shard_iterator)
        if records:
            # Process the retrieved records
            processed_data = process_data(records)
            # Detect anomalies
            detect_anomalies(processed_data)
        
        # Sleep for a short period to avoid hitting the Kinesis limit
        time.sleep(1)

if __name__ == '__main__':
    main()
