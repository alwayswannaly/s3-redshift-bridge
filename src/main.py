import os
import json
import redshift
import s3
import time
import sentry
import slack
from datetime import datetime, timedelta

inserts = 0
deletes = 0

def main():
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
    inserts += len(insert_ops)
    redshift.batch_remove(remove_ops)
    deletes += len(remove_ops)

    s3.create_checkpoint(file_key)
    print("Successfully finished {}".format(file_key))

  print("Sending notifications to slack 🚀")
  execution_time = str(datetime.now() - current_time)
  new_checkpoint = s3.read_checkpoint()

  slack.send_webhook_notification({
    "time_taken": finish_time,
    "inserted_records": str(inserts),
    "deleted_records": str(deletes),
    "checkpoint": new_checkpoint
  })

main()
