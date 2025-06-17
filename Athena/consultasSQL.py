import boto3
import json
import time
from datetime import datetime

# Obtener la fecha actual
year = f"{datetime.now().year}"
month = f"{datetime.now().month:02}"
day = f"{datetime.now().day:02}"

 
# Configuración de Athena
athena_client = boto3.client('athena', region_name='us-east-1')
database = 'datalake-energia-stage'
output_location = f's3://datalake-energia-analytics/athena-results/{year}/{month}/{day}/'
query = """
    SELECT p.id_proveedor, p.nombre_proveedor, t.tipo_transaccion, MAX(t.precio)
    FROM transacciones t
    JOIN proveedores p
    ON t.id_persona = p.id_proveedor
    ORDER BY p.nombre_proveedor
    """

def lambda_handler(event, context):
    try:
        # Ejecutar consulta
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        query_execution_id = response['QueryExecutionId']
        # Esperar a que la consulta termine
        while True:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            if status in ['QUEUED', 'RUNNING']:
                time.sleep(5)  # Verificar cada 2 segundos

        if status == 'SUCCEEDED':
            # Obtener los resultados
            results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            # Procesar los resultados
            rows = []
            for row in results['ResultSet']['Rows']:
                rows.append([data.get('VarCharValue', '') for data in row['Data']])
                
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Query ejecutado con exito!',
                    'results': rows
                })
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': f'Query fallo con status: {status}',
                    'error': query_status['QueryExecution']['Status'].get('StateChangeReason', '')
                })
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error al ejecutar el query',
                'error': str(e)
            })
        }
