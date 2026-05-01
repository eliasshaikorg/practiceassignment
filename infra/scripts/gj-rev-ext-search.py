import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from S3 and show output (sent to CloudWatch)
df = glueContext.create_dynamic_frame.from_options(
    "s3", {"paths": ["s3://your-bucket/path/"]}, "csv", {"withHeader": True}
)
df.toDF().show()

job.commit()
#done