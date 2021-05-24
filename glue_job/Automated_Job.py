import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [format_options = {"quoteChar":"\"","escaper":"","withHeader":True,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://mapping-file-nyctlc-nvirginia/"], "recurse":True}, transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"quoteChar":"\"","escaper":"","withHeader":True,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://mapping-file-nyctlc-nvirginia/"], "recurse":True}, transformation_ctx = "DataSource0")
## @type: DataSource
## @args: [database = "datacatalog", table_name = "glueblog_raws3bucket_12b2ee3rjvrdz", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "datacatalog", table_name = "glueblog_raws3bucket_12b2ee3rjvrdz", transformation_ctx = "DataSource1")
## @type: DropFields
## @args: [paths = ["congestion_surcharge", "tolls_amount", "tip_amount", "mta_tax", "extra", "store_and_fwd_flag", "improvement_surcharge", "vendorid", "ratecodeid"], transformation_ctx = "Transform5"]
## @return: Transform5
## @inputs: [frame = DataSource1]
Transform5 = DropFields.apply(frame = DataSource1, paths = ["congestion_surcharge", "tolls_amount", "tip_amount", "mta_tax", "extra", "store_and_fwd_flag", "improvement_surcharge", "vendorid", "ratecodeid"], transformation_ctx = "Transform5")
## @type: Join
## @args: [keys2 = ["LocationID"], keys1 = ["pulocationid"], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame1 = Transform5, frame2 = DataSource0]
Transform1 = Join.apply(frame1 = Transform5, frame2 = DataSource0, keys2 = ["LocationID"], keys1 = ["pulocationid"], transformation_ctx = "Transform1")
## @type: ApplyMapping
## @args: [mappings = [("tpep_pickup_datetime", "string", "tpep_pickup_datetime", "string"), ("tpep_dropoff_datetime", "string", "tpep_dropoff_datetime", "string"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("total_amount", "double", "total_amount", "double"), ("Borough", "string", "pu_borough", "string"), ("Zone", "string", "pu_zone", "string")], transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [frame = Transform1]
Transform4 = ApplyMapping.apply(frame = Transform1, mappings = [("tpep_pickup_datetime", "string", "tpep_pickup_datetime", "string"), ("tpep_dropoff_datetime", "string", "tpep_dropoff_datetime", "string"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("total_amount", "double", "total_amount", "double"), ("Borough", "string", "pu_borough", "string"), ("Zone", "string", "pu_zone", "string")], transformation_ctx = "Transform4")
## @type: Join
## @args: [keys2 = ["LocationID"], keys1 = ["dolocationid"], transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [frame1 = Transform4, frame2 = DataSource0]
Transform3 = Join.apply(frame1 = Transform4, frame2 = DataSource0, keys2 = ["LocationID"], keys1 = ["dolocationid"], transformation_ctx = "Transform3")
## @type: ApplyMapping
## @args: [mappings = [("tpep_pickup_datetime", "string", "tpep_pickup_datetime", "timestamp"), ("tpep_dropoff_datetime", "string", "tpep_dropoff_datetime", "timestamp"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("total_amount", "double", "total_amount", "double"), ("pu_borough", "string", "pu_borough", "string"), ("pu_zone", "string", "pu_zone", "string"), ("Borough", "string", "do_borough", "string"), ("Zone", "string", "do_zone", "string")], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [frame = Transform3]
Transform2 = ApplyMapping.apply(frame = Transform3, mappings = [("tpep_pickup_datetime", "string", "tpep_pickup_datetime", "timestamp"), ("tpep_dropoff_datetime", "string", "tpep_dropoff_datetime", "timestamp"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("total_amount", "double", "total_amount", "double"), ("pu_borough", "string", "pu_borough", "string"), ("pu_zone", "string", "pu_zone", "string"), ("Borough", "string", "do_borough", "string"), ("Zone", "string", "do_zone", "string")], transformation_ctx = "Transform2")
## @type: Filter
## @args: [f = lambda row : (row["fare_amount"] > 0 and row["total_amount"] > 0), transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = Transform2]
Transform0 = Filter.apply(frame = Transform2, f = lambda row : (row["fare_amount"] > 0 and row["total_amount"] > 0), transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://glueblog-processeds3bucket-1y124ih5bow6h/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "csv", connection_options = {"path": "s3://glueblog-processeds3bucket-1y124ih5bow6h/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()