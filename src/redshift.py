import os
import redshift_connector
from utils import batch_iterator, flatten

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

conn = redshift_connector.connect(
  host = os.environ['REDSHIFT_HOST'],
  database = os.environ['REDSHIFT_DATABASE'],
  port = int(os.environ['REDSHIFT_PORT']),
  user = os.environ['REDSHIFT_USER'],
  password = os.environ['REDSHIFT_PASSWORD']
)

conn.rollback()
conn.autocommit = True

db_cursor = conn.cursor()

#db_cursor.execute("drop table {};".format(table_name))
#db_cursor.execute("CREATE TABLE scaler_ebdb_dynamo_user_sessions (partition_key varchar(150), primary_sort_key varchar(300), auth_method varchar(50), device_properties_client_name character varying, device_properties_client_version character varying, device_properties_device_type character varying, device_properties_os_name character varying, device_properties_os_version character varying, location_city varchar(100), location_country varchar(100), location_country_code varchar(10), location_ip varchar(50), location_postal_code varchar(100), location_region varchar(100), location_region_code varchar(100), product varchar(30), remote_ip varchar(50), session_state varchar(20), user_agent varchar(500), created_at varchar(50), session_expiry_time varchar(50), updated_at varchar(50)) compound sortkey (partition_key, primary_sort_key);")

# These below functions could be parallalised
def batch_insert(data, batch_size = 500):
  for batched_data in batch_iterator(data, batch_size):
    values = []
    for row in batched_data:
      value = flatten(row['INSERT']['data'])
      res = []
      for key in TABLE_KEYS:
        row_value = "\'" + value[key] + "\'" if value[key] else "null"
        res.append(row_value)

      values.append("(" + ",".join(res) + ")")

    print(values)
    query = "INSERT INTO {} VALUES {};".format(table_name, ",".join(values))
    db_cursor.execute(query)

def batch_remove(data, batch_size = 500):
  for batched_data in batch_iterator(data, batch_size):
    values = []
    for row in batched_data:
      print(row)
      value = flatten(row['REMOVE']['keys'])
      values.append("(primary_sort_key=\'{}\' AND partition_key=\'{}\')".format(
        value['primary_sort_key'],
        value['partition_key']
      ))

    query = "DELETE FROM {} where {}".format(table_name, " OR ".join(values))
    print(query)
    db_cursor.execute(query)
