import boto3
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)

def lambda_handler(event, context):
    # Create a session and Athena client
    session = boto3.Session(region_name="eu-north-1")
    athena_client = session.client('athena')
    s3_client = session.client('s3')

    # Athena database name
    database_name = 'default'
    # Temporary output location for Athena results
    temp_output_location = 's3://reddit-processed-athena-results/temp/'
    # Final output file location
    final_output_location = 's3://reddit-processed-athena-results/latest-data.csv'
    bucket_name = 'reddit-processed-athena-results'
    temp_folder = 'temp/'

    # Define the query
    query = '''
    SELECT 
        created_time,
        TRY_CAST(sentiment AS DECIMAL(38,9)) AS sentiment,
        TRY_CAST(upvote_ratio AS DECIMAL(38,9)) AS upvote_ratio,
        thumbnail,
        TRY_CAST(author_activity_count AS DECIMAL(38,9)) AS author_activity_count,
        edited,
        over_18,
        author,
        title,
        subreddit,
        TRY_CAST(score AS DECIMAL(38,9)) AS score,
        TRY_CAST(num_comments AS DECIMAL(38,9)) AS num_comments,
        is_self_post,
        stickied,
        time_of_day,
        post_type,
        id,
        TRY_CAST(post_age_minutes AS DECIMAL(38,9)) AS post_age_minutes,
        TRY_CAST(popularity_score AS DECIMAL(38,9)) AS popularity_score,
        flair_text
    FROM "default"."tbl_reddit_processed"
    WHERE 
        sentiment IS NOT NULL 
        AND sentiment != '' 
        AND TRY_CAST(sentiment AS DECIMAL(38,9)) IS NOT NULL  -- Filter out non-numeric values
        AND upvote_ratio IS NOT NULL 
        AND upvote_ratio != '' 
        AND TRY_CAST(upvote_ratio AS DECIMAL(38,9)) IS NOT NULL
        AND author_activity_count IS NOT NULL 
        AND author_activity_count != '' 
        AND TRY_CAST(author_activity_count AS DECIMAL(38,9)) IS NOT NULL
        AND score IS NOT NULL 
        AND score != '' 
        AND TRY_CAST(score AS DECIMAL(38,9)) IS NOT NULL
        AND num_comments IS NOT NULL 
        AND num_comments != '' 
        AND TRY_CAST(num_comments AS DECIMAL(38,9)) IS NOT NULL
        AND post_age_minutes IS NOT NULL 
        AND post_age_minutes != '' 
        AND TRY_CAST(post_age_minutes AS DECIMAL(38,9)) IS NOT NULL
        AND popularity_score IS NOT NULL 
        AND popularity_score != '' 
        AND TRY_CAST(popularity_score AS DECIMAL(38,9)) IS NOT NULL;
    '''

    # Execute the query
    logging.info("Executing the query")
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': temp_output_location}  # Store in a temp folder
    )

    # Get the query execution ID
    query_execution_id = response['QueryExecutionId']
    logging.info("ATHENA QUERY EXECUTION ID: " + query_execution_id)

    status = 'RUNNING'
    while status in ['RUNNING', 'QUEUED']:
        logging.info(f"Current status: {status}")
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        time.sleep(5)  # Wait for 5 seconds before polling again

    # Check if the query execution succeeded
    if status == 'SUCCEEDED':
        logging.info("Query executed successfully. Fetching results...")

        # Get the result file name from the output location
        query_output_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
        logging.info(f"Temporary Athena result location: {query_output_location}")

        # Extract the key (file path) from the output location
        temp_csv_key = query_output_location.split(f"{bucket_name}/")[-1]

        # Copy the result from the temp location to the desired file path (latest-data.csv)
        copy_source = {'Bucket': bucket_name, 'Key': temp_csv_key}
        s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key='latest-data.csv')

        # Optionally, clean up the temp files
        s3_client.delete_object(Bucket=bucket_name, Key=temp_csv_key)

        logging.info(f"File copied to {final_output_location}")

        return {
            'statusCode': 200,
            'body': {
                'message': 'Query result saved to latest-data.csv',
                'query_execution_id': query_execution_id
            }
        }
    else:
        logging.error('Query execution failed. Status: %s', status)
        failure_reason = response['QueryExecution']['Status']['StateChangeReason']
        logging.error(failure_reason)
        return {
            'statusCode': 500,
            'body': {
                'message': 'Query execution failed',
                'reason': failure_reason
            }
        }



