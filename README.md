1. Ejercicio 1
A continuación, se presenta la documentación solicitada del ejercicio 1.
La estructura del repositorio es la siguiente:

energy-datalake-pipeline/
├── Glue/
│   ├── datalake-raw-to-stage-job.py
├── Lambda/
│   └── cargueDatos.py
├── Athena/
│   └── consultasSQL.py
├── data/
│   ├── proveedores.csv
│   ├── clientes.csv
│   └── transacciones.csv
│   ├── generarData.ipynb



1.1. Arquitectura propuesta


1.2. Descripción pipeline datos
El Datalake se organizara en S3 con las siguientes capas, cada una particionada por fecha de carga (año/mes/dia):
RAW: Almacena archivos. csv originales, sin transformaciones.
STAGE: Datos .parquet transformados. 
ANALYTICS: Datos enriquecidos para análisis en Redshift o BI.
Una función Lambda carga los archivos .csv al bucket S3 RAW en AWS. Luego, se ejecuta un crawler para obtener el catálogo de datos en Glue. Se guardan los registros de cargue en una tabla de DynamoDB. Este proceso está orquestado con Step Functions y programada su ejecución y periodicidad con EventBridge.
Se realizan transformaciones de datos por medio de un Glue Job, usando PySpark. Este job recibe parámetros de rutas de entrada(RAW) y salida(STAGE). Se guardan los resultados en formato .parquet en el bucket S3 STAGE. Se repite el proceso de catalogado de datos. La orquestación y programación, también está dada por Step Functions y EventBridge.
Una función lambda ejecuta queries en Athena sobre los datos de STAGE, dada la base de datos y se guardan los resultados en S3 ANALYTICS. La orquestación y programación sigue los patrones anteriormente descritos.
Lake Formation será clave para brindar acceso granular al catálogo de datos a los distintos usuarios del datalake y los roles que desempeñe cada uno. CloudWatch registrará los logs y se monitoreará todo el proceso. IAM dará permisos a tanto a usuario como a roles para interactuar entre servicios, siguiendo el principio de menor privilegio. 

1.3. IAM roles y permisos
Para asignar roles y permisos se puede realizar desde IAM. Se asignan las políticas usando json. El rol se asigna a las funciones lambda y al job de glue. Existen políticas preestablecidas que se pueden añadir a los usuario y/o roles, pero estas brindan acceso total a los servicios y no es una buena práctica. 

Rol de lambda

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Access",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::bucket",
        "arn:aws:s3:::bucket/*"
      ]
    },
    {
      "Sid": "DynamoDBAccess",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem"
      ],
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/tabla"
    },
    {
      "Sid": "AthenaAccess",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults"
      ],
      "Resource": "*"
    },
    {
      "Sid": "AthenaWorkgroupAndS3Output",
      "Effect": "Allow",
      "Action": [
        "athena:GetWorkGroup",
        "athena:GetDataCatalog",
        "glue:GetTable",
        "glue:GetDatabase"
      ],
      "Resource": "*"
    }
  ]
}

Rol de Glue
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3ReadAccessSource",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::bucket-s3-bucket",
        "arn:aws:s3:::bucket-s3-bucket/*"
        "arn:aws:s3:::bucket-s3-bucket2",
        "arn:aws:s3:::bucket-s3-bucket2/*"
      ]
    },
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchLogsAccess",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    },
    {
      "Sid": "GlueJobExecutionAccess",
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:GetJob"
      ],
      "Resource": "*"
    }
  ]
}

Assume role policy glue
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

