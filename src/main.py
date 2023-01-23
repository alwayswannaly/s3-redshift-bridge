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

a = redshift.db_cursor.execute("CREATE TABLE ebdb_dynamo_user_sessions (id integer primary key, partition_key character varying, primary_sort_key character varying, auth_method character varying, device_properties_client_name character varying, device_properties_client_version character varying, device_properties_device_type character varying, device_properties_os_name character varying, device_properties_os_version character varying, location_city character varying, location_country character varying, location_country_code character varying, location_ip character varying, location_postal_code character varying, location_region character varying, location_region_code character varying, product character varying, remote_ip character varying, session_state character varying, user_agent character varying, created_at timestamp, session_expiry_time timestamp, updated_at timestamp) compound sortkey (partition_key, primary_sort_key);")

print(a)
print("hello")

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
