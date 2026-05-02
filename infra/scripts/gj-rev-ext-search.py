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

# Read the tab-delimited data.sql file from S3
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://s3-raw-zone-dev-bucket-528733132057/externalclickdata/inputdata.sql"]},
    format="csv",
    format_options={"withHeader": True, "separator": "\t"}
)

# Convert to Spark DataFrame and log results to CloudWatch
df = dynamic_frame.toDF()
df.show(truncate=False)
row_count = df.count()
print(f"Read {row_count} rows from s3://s3-raw-zone-dev-bucket-528733132057/externalclickdata/inputdata.sql")
print("-------------------")
print("-------------------")
print("-------------------")
print(df)


# Write the result out as Parquet to the conformed S3 bucket.
output_path = "s3://s3-conformed-zone-dev-bucket-528733132057/externalclickdata/cleaneddata/"
df.write.mode("overwrite").parquet(output_path)
print(f"Wrote output to {output_path} as Parquet")

job.commit()
