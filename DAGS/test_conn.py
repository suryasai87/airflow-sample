from airflow import settings
from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook

conn = BaseHook.get_connection('blaine_aws_conn')
print(conn.get_extra())

'''
def create_conn(conn_id, conn_type, host, login, password, port):
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port
    )
session = settings.Session()
conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

if str(conn_name) == str(conn_id):
  return logging.info(f"Connection {conn_id} already exists")
else
  session.add(conn)
  session.commit()
  logging.info(Connection.log_info(conn))
  logging.info(f'Connection {conn_id} is created')
  '''
 
