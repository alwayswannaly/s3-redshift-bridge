import boto3
from io import TextIOWrapper

s3_client = boto3.client("s3")
bucket_name = '<S3_BUCKET_NAME>'

def file_iterator(s3_file_key):
  return s3_client.get_object(
    Bucket = bucket_name,
    Key = s3_file_key
  )['Body'].iter_lines()
