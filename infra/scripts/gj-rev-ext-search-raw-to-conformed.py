import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType, DoubleType
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

# Canonical order of base fields
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

# Parse product_list into exploded product rows
product_df = df.withColumn(
    "product_entry",
    F.explode(F.split(F.col("product_list"), ","))
).withColumn(
    "product_entry",
    F.trim(F.col("product_entry"))
).filter(
    (F.col("product_entry").isNotNull()) & (F.col("product_entry") != "")
).withColumn(
    "product_parts",
    F.split(F.col("product_entry"), ";")
).withColumn(
    "product_category",
    F.element_at(F.col("product_parts"), 1)
).withColumn(
    "product_name",
    F.element_at(F.col("product_parts"), 2)
).withColumn(
    "product_quantity",
    F.when(F.size(F.col("product_parts")) >= 3, F.col("product_parts")[2].cast(IntegerType())).otherwise(None)
).withColumn(
    "product_total_revenue",
    F.when(F.size(F.col("product_parts")) >= 4, F.col("product_parts")[3].cast(DoubleType())).otherwise(None)
).withColumn(
    "product_extra",
    F.when(F.size(F.col("product_parts")) >= 5, F.col("product_parts")[4]).otherwise(None)
).withColumn(
    "product_custom_events",
    F.when((F.col("product_extra").isNotNull()) & (F.col("product_extra") != ""), F.split(F.col("product_extra"), "\\|"))
     .otherwise(F.array())
).drop("product_entry", "product_parts", "product_extra")

# Keep only relevant product-level fields
grouped_product_df = product_df.select(
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
    "referrer",
    "product_category",
    "product_name",
    "product_quantity",
    "product_total_revenue",
    "product_custom_events"
)

# Aggregate total revenue by base row if needed
revenue_per_event_df = grouped_product_df.groupBy(
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
    "referrer"
).agg(
    F.sum("product_total_revenue").alias("revenue_total")
)

# Log counts and sample rows
print(f"Read {df.count()} rows from {input_path}")
print(f"Exploded into {grouped_product_df.count()} product-level rows")
grouped_product_df.show(20, truncate=False)

# Write the base cleaned data and product-level conformed data to S3
base_output_path = "s3://s3-conformeddev-bucket-528733132057/externalclickdata/cleaneddata/"
product_output_path = "s3://s3-conformeddev-bucket-528733132057/externalclickdata/product_level/"
revenue_output_path = "s3://s3-conformeddev-bucket-528733132057/externalclickdata/revenue_summary/"

base_csv_output_path = "s3://s3-conformeddev-bucket-528733132057/externalclickdata/cleaneddata_csv/"
product_csv_output_path = "s3://s3-conformeddev-bucket-528733132057/externalclickdata/product_level_csv/"
revenue_csv_output_path = "s3://s3-conformeddev-bucket-528733132057/externalclickdata/revenue_summary_csv/"

df.write.mode("overwrite").parquet(base_output_path)
print(f"Wrote base cleaned data to {base_output_path} as Parquet")
df.write.mode("overwrite").option("header", "true").csv(base_csv_output_path)
print(f"Wrote base cleaned data to {base_csv_output_path} as CSV")

grouped_product_df.write.mode("overwrite").parquet(product_output_path)
print(f"Wrote product-level data to {product_output_path} as Parquet")
grouped_product_df.write.mode("overwrite").option("header", "true").csv(product_csv_output_path)
print(f"Wrote product-level data to {product_csv_output_path} as CSV")

revenue_per_event_df.write.mode("overwrite").parquet(revenue_output_path)
print(f"Wrote revenue summary data to {revenue_output_path} as Parquet")
revenue_per_event_df.write.mode("overwrite").option("header", "true").csv(revenue_csv_output_path)
print(f"Wrote revenue summary data to {revenue_csv_output_path} as CSV")

job.commit()
