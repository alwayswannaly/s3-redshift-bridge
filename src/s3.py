import os
import boto3

bucket_name = os.environ['S3_BUCKET_NAME']
checkpoint_file_name = '_checkpoint_'

def file_iterator(s3_file_key):
  return boto3.client('s3').get_object(
    Bucket = bucket_name,
    Key = s3_file_key
  )['Body'].iter_lines()

def list_files(prefix):
  files = boto3.resource('s3').Bucket(bucket_name).objects.filter(Prefix = prefix)
  return [file.key for file in files]

def create_checkpoint(content):
  boto3.resource('s3').Object(bucket_name, checkpoint_file_name).put(Body=content)

def read_checkpoint():
  return boto3.client('s3').get_object(
    Bucket = bucket_name,
    Key = checkpoint_file_name
  )['Body'].read()

