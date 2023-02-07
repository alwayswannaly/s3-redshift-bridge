import os
import redshift_connector
from utils import batch_iterator, flatten
from concurrent import futures
import traceback

table_name = os.environ['REDSHIFT_TABLE_NAME']

# Sample flattened hash of DynamoDB table
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

executor = futures.ProcessPoolExecutor(2)

conn = redshift_connector.connect(
  host = os.environ['REDSHIFT_HOST'],
  database = os.environ['REDSHIFT_DATABASE'],
  port = int(os.environ['REDSHIFT_PORT']),
  user = os.environ['REDSHIFT_USER'],
  password = os.environ['REDSHIFT_PASSWORD']
)

conn.rollback()
conn.autocommit = True

# These below functions could be parallalised
def batch_insert(data, batch_size = 500):
  futures.wait([executor.submit(_insert, batched_data) for batched_data in batch_iterator(data, batch_size)])

def _insert(data):
  try:
    values = []
    for row in data:
      value = flatten(row['INSERT']['data'])
      res = []
      for key in TABLE_KEYS:
        if value[key] is None:
          escaped_value = "null"
        else:
          escaped_value = "\'" + value[key].replace("'", "''") + "\'"
        res.append(escaped_value)

      values.append("(" + ",".join(res) + ")")

    query = "INSERT INTO {} VALUES {};".format(table_name, ",".join(values))

    conn.cursor().execute(query)
    print("Inserted {} items".format(len(data)))
  except Exception:
    print(traceback.format_exc())

def batch_remove(data, batch_size = 500):
  futures.wait([executor.submit(_remove, batched_data) for batched_data in batch_iterator(data, batch_size)])

def _remove(data):
  try:
    values = []
    for row in data:
      value = flatten(row['REMOVE']['keys'])
      values.append("(primary_sort_key=\'{}\' AND partition_key=\'{}\')".format(
        value['primary_sort_key'],
        value['partition_key']
      ))

    query = "DELETE FROM {} where {}".format(table_name, " OR ".join(values))
    conn.cursor().execute(query)
    print("Deleted {} items".format(len(data)))
  except Exception:
    print(traceback.format_exc())