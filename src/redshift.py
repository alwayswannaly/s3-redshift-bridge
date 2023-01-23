import os
import redshift_connector
from utils import batch_iterator, flatten

table_name = os.environ['REDSHIFT_TABLE_NAME']

# Sample flattened hash of DynamoDB table
TABLE_KEYS = [
  'id', 'partition_key', 'primary_sort_key', 'auth_method',
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

db_cursor = conn.cursor()

# These below functions could be parallelised
def batch_insert(data, batch_size = 500):
  for batched_data in batch_iterator(data, batch_size):
    values = []
    for row in batched_data:
      value = flatten(row['INSERT']['data'])
      res = []
      for key in TABLE_KEYS:
        res.append(value[key] if value.get(key) else "default")

      values.append("(" + ",".join(res) + ")")

    query = "INSERT INTO {} VALUES {};".format(table_name, ",".join(values))
    db_cursor.execute(query)

def batch_delete(data, batch_size = 500):
  for batched_data in batch_iterator(data, batch_size):
    values = []
    for row in batched_data:
      value = flatten(row['INSERT']['data'])
      values.append("(primary_sort_key=\"{}\" AND partition_key=\"{}\")".format(
        value['primary_sort_key'],
        value['partition_key']
      ))

    query = "DELETE FROM {} where {}".format(table_name, " OR ".join(values))
    db_cursor.execute(query)
