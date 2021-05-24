import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "demo", table_name = "demo_rawdata", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "demo", table_name = "demo_rawdata", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("tpep_pickup_datetime", "string", "tpep_pickup_datetime", "string"), ("tpep_dropoff_datetime", "string", "tpep_dropoff_datetime", "string"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("total_amount", "double", "total_amount", "double")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("tpep_pickup_datetime", "string", "tpep_pickup_datetime", "string"), ("tpep_dropoff_datetime", "string", "tpep_dropoff_datetime", "string"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("total_amount", "double", "total_amount", "double")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", catalog_database_name = "demo", format = "csv", connection_options = {"path": "s3://demo-processed-data/", "partitionKeys": [], "enableUpdateCatalog":true, "updateBehavior":"UPDATE_IN_DATABASE"}, catalog_table_name = "demo-processed-data", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.getSink(path = "s3://demo-processed-data/", connection_type = "s3", updateBehavior = "UPDATE_IN_DATABASE", partitionKeys = [], enableUpdateCatalog = True, transformation_ctx = "DataSink0")
DataSink0.setCatalogInfo(catalogDatabase = "demo",catalogTableName = "demo-processed-data")
DataSink0.setFormat("csv")
DataSink0.writeFrame(Transform0)

job.commit()