import time
import boto3
DATABASE = 'parcialp1'
TABLE = 'punto1'
#Funcion para insertar datos en la base de datos
def handler(event, context):
    query = 'MSCK REPAIR TABLE `punto1`;'
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        }
    )
    return response