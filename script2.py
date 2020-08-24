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
## @args: [database = "default", table_name = "data", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = ['DATABASE_NAME'], table_name = ['TABLE_NAME'], transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("licenseplate", "string", "licenseplate", "string"), ("sensor", "string", "sensor", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = DropFields.apply(frame = datasource0, paths = [['FIELDS_LIST']],transformation_ctx="dropped_fpt")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://hw2/data-clean"}, format = "json", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": ['BUCKET_NAME']}, format = ['FORMAT'], transformation_ctx = "datasink2")
job.commit()