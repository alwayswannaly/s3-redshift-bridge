from amazon.ion import simpleion
import amazon.ion.simple_types as types
import os
import redshift_connector
from utils import batch_iterator, flatten
import boto3
import gzip

bucket_name = os.environ["S3_BUCKET_NAME"]
table_name = os.environ['REDSHIFT_TABLE_NAME']

TABLE_KEYS = [
  'partition_key', 'primary_sort_key', 'auth_method',
  'device_properties_client_name', 'device_properties_client_version',
  'device_properties_device_type', 'device_properties_os_name',
  'device_properties_os_version', 'location_city', 'location_country',
  'location_country_code', 'location_ip', 'location_postal_code',
  'location_region', 'location_region_code', 'product', 'remote_ip',
  'session_state', 'user_agent', 'created_at', 'session_expiry_time',
  'updated_at'
]

conn = redshift_connector.connect(
  host = os.environ['REDSHIFT_HOST'],
  database = os.environ['REDSHIFT_DATABASE'],
  port = int(os.environ['REDSHIFT_PORT']),
  user = os.environ['REDSHIFT_USER'],
  password = os.environ['REDSHIFT_PASSWORD']
)

conn.rollback()
conn.autocommit = True

def read_file(name):
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, name)
  with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipfile:
    content = gzipfile.read()
  content = content.decode("utf-8")
  content = simpleion.loads(content, single_value=False)
  return list(filter(lambda item: item['Item']['partition_key'].startswith("scaler"), content))

files = ["AWSDynamoDB/01675882948292-3d4edafe/data/74wftxy2de5ehkstocgxwsbbga.ion.gz"]

i = 0
def batch_insert(data, batch_size = 500):
  for batched_data in batch_iterator(data, batch_size):
    values = []
    for row in batched_data:
      value = flatten(row['Item'])
      res = []
      for key in TABLE_KEYS:
        if value[key] is None or type(value[key]) is types.IonPyNull:
          escaped_value = "null"
        else:
          escaped_value = "\'" + value[key].replace("'", "''") + "\'"
        res.append(escaped_value)
      values.append("(" + ",".join(res) + ")")
    global i
    i += len(values)
    query = "INSERT INTO {} VALUES {};".format(table_name, ",".join(values))
    conn.cursor().execute(query)
    print("Inserted {} records".format(i))

for file in files:
  print("starting file {}".format(file))
  content = read_file(file)
  batch_insert(content)

