import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME','TABLE_NAME', 'BUCKET_NAME', 'FORMAT'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print('*******************************')
print(args['JOB_NAME'])
print(args['DATABASE_NAME'])
print(args['TABLE_NAME'])
print(args['BUCKET_NAME'])
print(args['FORMAT'])
print('*******************************')

## @type: DataSource
## @args: [database = args['DATABASE_NAME'], table_name = args['TABLE_NAME'], transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['DATABASE_NAME'], table_name = args['TABLE_NAME'], transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [[PLACEHOLDER]], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [[PLACEHOLDER]], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": args['BUCKET_NAME']}, format = "json", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": args['BUCKET_NAME']}, format = args['FORMAT'], transformation_ctx = "datasink2")
job.commit()