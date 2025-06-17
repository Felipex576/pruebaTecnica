import boto3
from datetime import datetime

# Obtener la fecha actual
now = datetime.now()
year = f"{now.year}"
month = f"{now.month:02}"
day = f"{now.day:02}"

# Inicializar el cliente de S3
s3_client = boto3.client('s3', region_name='us-east-1')
dynamodb_cliente = boto3.resource('dynamodb', region_name='us-east-1')

# Definiciones
nombre_tabla = 'cargue-raw-logs'
nombre_bucket = 'datalake-energia-raw'
archivos = ['transacciones.csv', 'clientes.csv', 'proveedores.csv']
rutas = ['transacciones', 'clientes', 'proveedores']


def lambda_handler(event, context):
    try:
        for archivo, ruta in zip(archivos, rutas):
            ruta_salida = f"{ruta}/{year}/{month}/{day}/{archivo}"
            s3_client.upload_file(archivo, nombre_bucket, ruta_salida)
            print(f'Archivo {archivo} subido exitosamente a {nombre_bucket}/{ruta_salida}')
            
            # Registrar metadata en DynamoDB
            item = {
                'nombre_archivo': archivo,
                'ruta_s3': ruta_salida,
                'bucket': nombre_bucket,
                'fecha_carga': now,
                'tabla_logica': ruta,                 # Puede representar entidad origen
                'estado': 'EXITO'
            }
            nombre_tabla.put_item(Item=item)
        return {
            'statusCode': 200,
            'body': 'Archivos subidos exitosamente.'
        }
    except Exception as e:
        print(f'Error al subir los archivos: {e}')
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
