"""
AWS Glue Job: Raw -> Conformed
-----------------------------------------------------------------------
Reads the raw tab-delimited hit-level data from S3, applies light
cleaning, and writes TWO tables to the conformed zone:

  1. BASE TABLE  — every hit row, regardless of whether it has a
                   product_list. This preserves the true first hit per
                   IP (which may be a Home page with no products) and
                   is what Script 2 uses to identify the search engine
                   and keyword that started each session.

  2. PRODUCT TABLE — only rows that carry a product_list, exploded to
                     one row per product. This is what Script 2 uses
                     to read purchase revenue.

Why two tables?
  The explode + filter on product_list was silently dropping hits that
  had no products (Home, Search Results, Cart, Checkout pages). Those
  dropped rows include the true entry hit for many IPs, which holds the
  external search engine referrer. Without them, session attribution in
  Script 2 is wrong.

Key fix vs original script:
  - product_custom_events stored as pipe-delimited STRING (not
    array<string>) so CSV output works without AnalysisException.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME", "INPUT_FILE"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Passed in at job run time via --INPUT_FILE s3://bucket/path/file.sql
INPUT_PATH = args["INPUT_FILE"]

# Table 1: ALL hits — used by Script 2 for session origin detection
BASE_OUTPUT_PARQUET = "s3://s3-conformeddev-bucket-528733132057/externalclickdata/all_hits/"
BASE_OUTPUT_CSV     = "s3://s3-conformeddev-bucket-528733132057/externalclickdata/all_hits_csv/"

# Table 2: Product-level rows only — used by Script 2 for purchase revenue
PROD_OUTPUT_PARQUET = "s3://s3-conformeddev-bucket-528733132057/externalclickdata/product_level/"
PROD_OUTPUT_CSV     = "s3://s3-conformeddev-bucket-528733132057/externalclickdata/product_level_csv/"

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

RAW_SCHEMA = StructType([
    StructField("hit_time_gmt", IntegerType(),   True),
    StructField("date_time",    TimestampType(), True),
    StructField("user_agent",   StringType(),    True),
    StructField("ip",           StringType(),    True),
    StructField("event_list",   StringType(),    True),
    StructField("geo_city",     StringType(),    True),
    StructField("geo_region",   StringType(),    True),
    StructField("geo_country",  StringType(),    True),
    StructField("pagename",     StringType(),    True),
    StructField("page_url",     StringType(),    True),
    StructField("product_list", StringType(),    True),
    StructField("referrer",     StringType(),    True),
])

# ---------------------------------------------------------------------------
# Processor class
# ---------------------------------------------------------------------------

class HitLevelProcessor:
    """
    Encapsulates all Raw -> Conformed transformation logic.

    Produces two output tables:
      - base_df    : every hit (all IPs, all pages, with or without products)
      - product_df : product-level rows exploded from product_list
    """

    def __init__(self, spark_session, logger):
        self.spark  = spark_session
        self.logger = logger

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def read_raw(self, path: str):
        df = (
            self.spark.read.csv(
                path,
                sep="\t",
                header=True,
                schema=RAW_SCHEMA,
                timestampFormat="yyyy-MM-dd HH:mm:ss",
            )
        )
        count = df.count()
        self.logger.info(f"[read_raw] Loaded {count} rows from {path}")
        return df

    # ------------------------------------------------------------------
    # Table 1: Base — ALL hits, cleaned
    # ------------------------------------------------------------------

    def clean_base(self, df):
        """
        Trim whitespace on all string columns and enforce column order.
        Keeps EVERY hit row — including those with no product_list.
        This is essential so Script 2 can find the true first hit per IP.
        """
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, F.trim(F.col(field.name)))

        # Normalise empty strings to NULL for easier downstream filtering
        df = df.withColumn(
            "event_list",
            F.when(F.col("event_list") == "", None).otherwise(F.col("event_list")),
        ).withColumn(
            "product_list",
            F.when(F.col("product_list") == "", None).otherwise(F.col("product_list")),
        ).withColumn(
            "referrer",
            F.when(F.col("referrer") == "", None).otherwise(F.col("referrer")),
        )

        return df.select(
            "hit_time_gmt", "date_time", "user_agent", "ip",
            "event_list", "geo_city", "geo_region", "geo_country",
            "pagename", "page_url", "product_list", "referrer",
        )

    # ------------------------------------------------------------------
    # Table 2: Products — exploded from product_list
    # ------------------------------------------------------------------

    def explode_products(self, df):
        """
        Explode product_list into one row per product entry.
        Only rows that actually carry a product_list are included here.

        Format: [Category];[Name];[Qty];[Revenue];[CustomEvents]|[...];[MerchEvar]
        Products are comma-separated; attributes are semicolon-separated.

        IMPORTANT: product_custom_events is stored as a plain pipe-delimited
        STRING (e.g. "200|201") — NOT as array<string> — so the DataFrame
        can be written to CSV without an AnalysisException.
        """
        return (
            df
            # Only process rows that have a product_list
            .filter(F.col("product_list").isNotNull())
            # Explode comma-separated products into individual rows
            .withColumn(
                "product_entry",
                F.explode(F.split(F.col("product_list"), ",")),
            )
            .withColumn("product_entry", F.trim(F.col("product_entry")))
            .filter(
                F.col("product_entry").isNotNull()
                & (F.col("product_entry") != "")
            )
            # Split each product on semicolons
            .withColumn("_parts", F.split(F.col("product_entry"), ";"))
            # Parse product attributes
            .withColumn("product_category", F.element_at(F.col("_parts"), 1))
            .withColumn("product_name",     F.element_at(F.col("_parts"), 2))
            .withColumn(
                "product_quantity",
                F.when(F.size("_parts") >= 3, F.col("_parts")[2].cast(IntegerType()))
                 .otherwise(None),
            )
            .withColumn(
                "product_total_revenue",
                F.when(F.size("_parts") >= 4, F.col("_parts")[3].cast(DoubleType()))
                 .otherwise(None),
            )
            # Keep raw "event1|event2" string — do NOT split into array<string>
            .withColumn(
                "product_custom_events",
                F.when(
                    (F.size("_parts") >= 5)
                    & F.col("_parts")[4].isNotNull()
                    & (F.col("_parts")[4] != ""),
                    F.col("_parts")[4],
                ).otherwise(None),
            )
            .drop("product_entry", "_parts")
            .select(
                "hit_time_gmt", "date_time", "user_agent", "ip",
                "event_list", "geo_city", "geo_region", "geo_country",
                "pagename", "page_url", "referrer",
                "product_category", "product_name",
                "product_quantity", "product_total_revenue", "product_custom_events",
            )
        )

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def write_parquet(self, df, path: str, label: str = ""):
        df.write.mode("overwrite").parquet(path)
        self.logger.info(f"[write_parquet] {label} -> {path}  rows={df.count()}")

    def write_csv(self, df, path: str, label: str = ""):
        df.write.mode("overwrite").option("header", "true").csv(path)
        self.logger.info(f"[write_csv] {label} -> {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    processor = HitLevelProcessor(spark, logger)

    # 1. Ingest raw file
    raw_df = processor.read_raw(INPUT_PATH)

    # 2. Clean — keeps ALL hits (Table 1)
    base_df = processor.clean_base(raw_df)

    # 3. Explode products — only hits with product_list (Table 2)
    product_df = processor.explode_products(base_df)

    # 4. Write Table 1: all hits
    processor.write_parquet(base_df, BASE_OUTPUT_PARQUET, "all_hits")
    processor.write_csv(base_df,     BASE_OUTPUT_CSV,     "all_hits")

    # 5. Write Table 2: product-level
    processor.write_parquet(product_df, PROD_OUTPUT_PARQUET, "product_level")
    processor.write_csv(product_df,     PROD_OUTPUT_CSV,     "product_level")

    logger.info(
        f"[main] base rows={base_df.count()}, product rows={product_df.count()}"
    )

    base_df.show(25, truncate=False)
    product_df.show(25, truncate=False)

    job.commit()


if __name__ == "__main__":
    main()