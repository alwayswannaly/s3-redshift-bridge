import os
import json
import redshift
import s3
from datetime import datetime

file_prefix = datetime.now().strftime("%Y/%m/%d")
print(s3.list_files(file_prefix))

checkpoint = s3.read_checkpoint()

print(checkpoint)

# # Loading env variables -> `export $(xargs <.env`

# events = {}

#   for record in event['Records']:
#     s3_file_key = record['s3']['object']['key']
#     file_content = []

#     for line in file_iterator(s3_file_key):
#       file_content += json.loads(line.decode())

#     insert_ops = list(filter(lambda item: list(item.keys())[0] == 'INSERT', file_content))
#     redshift.batch_insert(db_cursor, insert_ops)

#     delete_ops = list(filter(lambda item: list(item.keys())[0] == 'REMOVE', file_content))
#     redshift.batch_delete(db_cursor, insert_ops)

#     return { 'success': True }
