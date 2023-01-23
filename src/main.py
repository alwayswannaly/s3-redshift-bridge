import os
import json
import redshift
import s3
from datetime import datetime

file_prefix = datetime.now().strftime("%Y/%m/%d")
checkpoint = s3.read_checkpoint()
print(checkpoint)

files = s3.list_files(prefix=file_prefix, checkpoint=checkpoint)

print(files)

# Loading env variables -> `export $(xargs <.env)`

insert_ops = []
remove_ops = []

for file_key in files:
  file_content = []
  for line in s3.file_iterator(file_key):
    file_content += json.loads(line.decode())

  insert_ops += list(filter(lambda item: list(item.keys())[0] == 'INSERT' and item['INSERT']['keys']['partition_key'].startswith("scaler"), file_content))
  remove_ops += list(filter(lambda item: list(item.keys())[0] == 'REMOVE' and item['REMOVE']['keys']['partition_key'].startswith("scaler"), file_content))
  s3.create_checkpoint(file_key)

redshift.batch_insert(insert_ops)
redshift.batch_remove(remove_ops)



