import os
import json
import redshift
from file_handler import file_iterator

db_cursor = redshift.conn.cursor()

# Loading env variables -> `export $(xargs <.env`

def lambda_handler(event, context):
  events = {}

  for record in event['Records']:
    s3_file_key = record['s3']['object']['key']
    file_content = []

    for line in file_iterator(s3_file_key):
      file_content += json.loads(line.decode())

    insert_ops = list(filter(lambda item: list(item.keys())[0] == 'INSERT', file_content))
    redshift.batch_insert(db_cursor, insert_ops)

    delete_ops = list(filter(lambda item: list(item.keys())[0] == 'REMOVE', file_content))
    redshift.batch_delete(db_cursor, insert_ops)

    return { 'success': True }
