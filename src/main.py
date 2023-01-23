import os
import json
import redshift
import s3
import time
from datetime import datetime, timedelta

current_time = datetime.now()
today_file_prefix = current_time.strftime("%Y/%m/%d")
yesterday_file_prefix = (current_time - timedelta(days=1)).strftime("%Y/%m/%d")

checkpoint = s3.read_checkpoint()
print("Starting from checkpoint: {}".format(checkpoint))

prefixes = [yesterday_file_prefix, today_file_prefix]
files = s3.list_files(prefixes=prefixes, checkpoint=checkpoint)

print("Pending files: {}".format(files))

for file_key in files:
  file_content = []
  for line in s3.file_iterator(file_key):
    file_content += json.loads(line.decode())

  insert_ops = list(filter(lambda item: list(item.keys())[0] == 'INSERT' and item['INSERT']['keys']['partition_key'].startswith("scaler"), file_content))
  remove_ops = list(filter(lambda item: list(item.keys())[0] == 'REMOVE' and item['REMOVE']['keys']['partition_key'].startswith("scaler"), file_content))

  redshift.batch_insert(insert_ops)
  redshift.batch_remove(remove_ops)

  s3.create_checkpoint(file_key)
  print("Successfully finished {}".format(file_key))
