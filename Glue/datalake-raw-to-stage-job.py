import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.sql import functions as F


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PROVEEDOR_RAW', 'CLIENTE_RAW', 'TRANSACCION_RAW', 'PROVEEDOR_STAGE', 'CLIENTE_STAGE', 'TRANSACCION_STAGE']) 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args) 

#Se obtiene la fecha actual
year = f"{datetime.now().year}"
month = f"{datetime.now().month:02}"
day = f"{datetime.now().day:02}"

# Carga los parametros en variables
proveedor_source = args['PROVEEDOR_RAW']
cliente_source = args['CLIENTE_RAW']
transaccion_source = args['TRANSACCION_RAW']
proveedor_output = args['PROVEEDOR_STAGE']
cliente_output = args['CLIENTE_STAGE']
transaccion_output = args['TRANSACCION_STAGE']

#Construye la ruta de lectura RAW S3 con la fecha de hoy
proveedor_source_today = f"{proveedor_source}/{year}/{month}/{day}/"
cliente_source_today = f"{cliente_source}/{year}/{month}/{day}/"
transaccion_source_today = f"{transaccion_source}/{year}/{month}/{day}/"

#Lee los .csv
proveedores = spark.read.option("header", "true").csv(proveedor_source_today)
clientes =  spark.read.option("header", "true").csv(cliente_source_today)
transacciones = spark.read.option("header", "true").csv(transaccion_source_today)

# Transformaciones 
proveedores = proveedores.withColumn("nombre_proveedor", F.regexp_replace(F.col("nombre_proveedor"), "[\",]", ""))
clientes = clientes.withColumn("tipo_identificacion_cliente", F.upper(F.col("tipo_identificacion_cliente")))
transacciones = transacciones.withColumn("id_transaccion", 
    F.when(F.col("tipo_transaccion") == "venta", F.concat(F.lit("V"), F.col("id_transaccion"))) \
    .when(F.col("tipo_transaccion") == "compra", F.concat(F.lit("C"), F.col("id_transaccion"))) \
    .otherwise(F.col("id_transaccion")))

#Construye la ruta de guardado STAGE S3 con la fecha de hoy
proveedor_output_today = f"{proveedor_output}/{year}/{month}/{day}/"
cliente_output_today = f"{cliente_output}/{year}/{month}/{day}/"
transaccion_output_today = f"{transaccion_output}/{year}/{month}/{day}/"

#Guarda los DF en formato parquet con las transformaciones realizadas
proveedores.write.mode("overwrite").parquet(proveedor_output_today)
clientes.write.mode("overwrite").parquet(cliente_output_today)
transacciones.write.mode("overwrite").parquet(transaccion_output_today)

job.commit()
