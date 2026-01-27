from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("DynamoDBProcessing") \
        .getOrCreate()

    # Specify your DynamoDB table name
    dynamodb_table_name = "tbl_RedditKinesis"

    # Load data from DynamoDB
    df = spark.read \
        .format("dynamodb") \
        .option("tableName", dynamodb_table_name) \
        .load()

    # Show the data
    df.show()

    # Example processing: Filter records where score > 50
    filtered_df = df.filter(col("score") > 1)
    filtered_df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
