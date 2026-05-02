import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Explicit schema for the tab-delimited file
schema = StructType([
    StructField("hit_time_gmt", IntegerType(), True),
    StructField("date_time", TimestampType(), True),
    StructField("user_agent", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("event_list", StringType(), True),
    StructField("geo_city", StringType(), True),
    StructField("geo_region", StringType(), True),
    StructField("geo_country", StringType(), True),
    StructField("pagename", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("product_list", StringType(), True),
    StructField("referrer", StringType(), True)
])

input_path = "s3://s3-rawdev-bucket-528733132057/externalclickdata/inputdata.sql"

df = glueContext.spark_session.read.csv(
    input_path,
    sep="\t",
    header=True,
    schema=schema,
    timestampFormat="yyyy-MM-dd HH:mm:ss"
)

# Optionally reorder columns to a canonical order if needed
df = df.select(
    "hit_time_gmt",
    "date_time",
    "user_agent",
    "ip",
    "event_list",
    "geo_city",
    "geo_region",
    "geo_country",
    "pagename",
    "page_url",
    "product_list",
    "referrer"
)

# Log results to CloudWatch
df.show(truncate=False)
row_count = df.count()
print(f"Read {row_count} rows from {input_path}")

# Write the result out as Parquet to the conformed S3 bucket
output_path = "s3://s3-conformeddev-bucket-528733132057/externalclickdata/cleaneddata/"
df.write.mode("overwrite").parquet(output_path)
print(f"Wrote output to {output_path} as Parquet")

job.commit()
