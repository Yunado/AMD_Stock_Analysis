import datetime
import json
import boto3
import time
import csv

STREAM_NAME = "mp11v2_ds"


def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client("s3")

    # Set bucket name and file key
    bucket_name = "mp11v2-flink-s3"
    key = "AMDprices2021-2022.csv"

    # TODO: Get CSV file from S3
    response = s3.get_object(Bucket=bucket_name, Key=key)
    lines = response["Body"].read().decode("utf-8").split("\n")
    # TODO: Split data by lines and extract column names from first row
    csv_reader = csv.reader(lines)
    column_names = next(csv_reader)
    # TODO: Iterate over rows and generate data for Kinesis stream
    kinesis_client = boto3.client("kinesis")
    for line in csv_reader:
        data_dict = dict(zip(column_names, line))
        if not data_dict:
            return {"statusCode": 200, "body": json.dumps("No data to process")}
        generate(STREAM_NAME, kinesis_client, data_dict)

    # Return response
    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}


def get_data(data_dict):
    # Generate data dictionary for Kinesis stream
    return {
        "date": data_dict["Date"],
        "ticker": "AMD",
        "open_price": data_dict["Open"],
        "high": data_dict["High"],
        "low": data_dict["Low"],
        "close_price": data_dict["Close"],
        "adjclose": data_dict["Adj Close"],
        "volume": data_dict["Volume"],
        "event_time": datetime.datetime.now().isoformat(),
    }


def generate(stream_name, kinesis_client, data_dict):
    # Get data dictionary and print it
    data = get_data(data_dict)
    print(data)
    # TODO: Put record to Kinesis stream
    response = kinesis_client.put_record(
        StreamName=stream_name, Data=json.dumps(data), PartitionKey=data["ticker"]
    )


# Test with
# {
#   "bucketName": "mp11v2-flink-s3",
#   "fileName": "AMDprices2021-2022.csv"
# }
