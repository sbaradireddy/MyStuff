import boto3
import json
import ibm_db

# Initialize a session using Amazon Secrets Manager
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name='your_aws_region'  # e.g., 'us-east-1'
)

def get_secret(secret_name):
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

secret_name = "db2/credentials"
credentials = get_secret(secret_name)

dsn_hostname = credentials['hostname']
dsn_uid = credentials['username']
dsn_pwd = credentials['password']
dsn_database = credentials['database']
dsn_port = credentials['port']
dsn_protocol = "TCPIP"

dsn = (
    "DATABASE={0};"
    "HOSTNAME={1};"
    "PORT={2};"
    "PROTOCOL={3};"
    "UID={4};"
    "PWD={5};"
).format(dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd)

try:
    conn = ibm_db.connect(dsn, "", "")
    print("Connected to database:", dsn_database)

    sql = "SELECT * FROM your_table_name"
    stmt = ibm_db.exec_immediate(conn, sql)

    result = ibm_db.fetch_assoc(stmt)
    while result:
        print(result)
        result = ibm_db.fetch_assoc(stmt)

    ibm_db.close(conn)
    print("Connection closed.")
except Exception as e:
    print("Unable to connect:", str(e))