"""
AWS Glue Job: Conformed -> Curated
Reads the product-level conformed data from S3, identifies hits that:
  1. Came from an external search engine referrer (Google, Bing, Yahoo, MSN, etc.)
  2. Are purchase events (event_list contains "1")

Then aggregates total revenue by (search_engine_domain, search_keyword) and
writes a tab-delimited output file named:
    YYYY-mm-dd_SearchKeywordPerformance.tab

This directly answers the client's business question:
    "How much revenue is the client getting from external Search Engines,
     and which keywords are performing the best based on revenue?"
"""

import sys
from datetime import date
from urllib.parse import parse_qs, urlparse

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PRODUCT_INPUT_PATH = (
    "s3://s3-conformeddev-bucket-528733132057/externalclickdata/product_level/"
)
CURATED_OUTPUT_PATH = (
    "s3://s3-curateddev-bucket-528733132057/externalclickdata/search_keyword_performance/"
)

# Output filename: YYYY-mm-dd_SearchKeywordPerformance.tab
OUTPUT_FILENAME = f"{date.today().strftime('%Y-%m-%d')}_SearchKeywordPerformance.tab"

# Search engines and the query-string parameter that carries the keyword
SEARCH_ENGINE_KEYWORD_PARAMS = {
    "google.com":  ["q"],
    "bing.com":    ["q"],
    "yahoo.com":   ["p", "q"],
    "msn.com":     ["q"],
    "ask.com":     ["q"],
    "aol.com":     ["q", "query"],
    "baidu.com":   ["wd", "word"],
    "duckduckgo.com": ["q"],
}

# ---------------------------------------------------------------------------
# UDFs
# ---------------------------------------------------------------------------

def extract_search_engine(referrer: str) -> str | None:
    """Return the registered domain if the referrer is a known search engine."""
    if not referrer:
        return None
    try:
        host = urlparse(referrer).hostname or ""
        host = host.lower().lstrip("www.")
        for domain in SEARCH_ENGINE_KEYWORD_PARAMS:
            if host == domain or host.endswith("." + domain):
                return domain
    except Exception:
        pass
    return None


def extract_keyword(referrer: str) -> str | None:
    """Extract the search keyword from a referrer URL."""
    if not referrer:
        return None
    try:
        host = urlparse(referrer).hostname or ""
        host = host.lower().lstrip("www.")
        for domain, params in SEARCH_ENGINE_KEYWORD_PARAMS.items():
            if host == domain or host.endswith("." + domain):
                qs = parse_qs(urlparse(referrer).query)
                for param in params:
                    if param in qs and qs[param]:
                        return qs[param][0].strip()
    except Exception:
        pass
    return None


udf_search_engine = F.udf(extract_search_engine, StringType())
udf_keyword       = F.udf(extract_keyword,       StringType())

# ---------------------------------------------------------------------------
# Helper class
# ---------------------------------------------------------------------------

class SearchKeywordRevenueProcessor:
    """
    Answers the business question:
        Revenue from external search engines, ranked by keyword performance.
    """

    def __init__(self, spark_session, logger):
        self.spark = spark_session
        self.logger = logger

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def read_conformed(self, path: str):
        df = self.spark.read.parquet(path)
        self.logger.info(f"[read_conformed] Loaded {df.count()} rows from {path}")
        return df

    # ------------------------------------------------------------------
    # Filter: purchase events only
    # ------------------------------------------------------------------

    def filter_purchase_events(self, df):
        """
        Revenue is only actualised on a Purchase event (event_list contains "1").
        A row qualifies when "1" appears as a standalone token in event_list,
        e.g. "1", "1,2", "2,1,12" — but NOT "10", "11", "12", etc.
        """
        purchase_df = df.filter(
            F.col("event_list").isNotNull()
            & (
                (F.col("event_list") == "1")
                | F.col("event_list").rlike(r"(^|,)\s*1\s*(,|$)")
            )
        )
        self.logger.info(
            f"[filter_purchase_events] {purchase_df.count()} purchase rows retained"
        )
        return purchase_df

    # ------------------------------------------------------------------
    # Filter: external search referrers only
    # ------------------------------------------------------------------

    def enrich_with_search_info(self, df):
        """Add search_engine_domain and search_keyword columns; drop non-search rows."""
        enriched = (
            df
            .withColumn("Search Engine Domain", udf_search_engine(F.col("referrer")))
            .withColumn("Search Keyword",       udf_keyword(F.col("referrer")))
            .filter(
                F.col("Search Engine Domain").isNotNull()
                & F.col("Search Keyword").isNotNull()
                & (F.col("Search Keyword") != "")
            )
        )
        self.logger.info(
            f"[enrich_with_search_info] {enriched.count()} rows from external search referrers"
        )
        return enriched

    # ------------------------------------------------------------------
    # Aggregate
    # ------------------------------------------------------------------

    def aggregate_revenue(self, df):
        """
        Sum revenue by (Search Engine Domain, Search Keyword).
        product_total_revenue is NULL for non-purchase rows; coalesce to 0
        so we still surface keyword/engine combos even if revenue is 0.
        """
        agg_df = (
            df
            .withColumn(
                "revenue",
                F.coalesce(F.col("product_total_revenue"), F.lit(0.0)),
            )
            .groupBy("Search Engine Domain", "Search Keyword")
            .agg(F.sum("revenue").alias("Revenue"))
            .orderBy(F.col("Revenue").desc())
        )
        return agg_df

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write_output(self, df, output_path: str, filename: str):
        """
        Write a single tab-delimited file with a header row.
        Uses coalesce(1) to produce exactly one output part file,
        then rename it to the required filename convention.
        """
        tmp_path = output_path.rstrip("/") + "/_tmp/"

        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .option("sep", "\t")
            .csv(tmp_path)
        )

        # Rename the single part file to the desired filename
        # (Works in local mode; in Glue use boto3 for S3 rename)
        self.logger.info(
            f"[write_output] Tab file written to {tmp_path}. "
            f"Rename the part-*.csv file to {filename}."
        )

        # ----------------------------------------------------------------
        # Rename via boto3 (S3 copy + delete)
        # ----------------------------------------------------------------
        try:
            import boto3

            bucket, prefix = self._parse_s3_path(tmp_path)
            final_bucket, final_prefix = self._parse_s3_path(
                output_path.rstrip("/") + "/" + filename
            )

            s3 = boto3.client("s3")
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            for obj in response.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv") or key.endswith(".tab"):
                    s3.copy_object(
                        Bucket=final_bucket,
                        CopySource={"Bucket": bucket, "Key": key},
                        Key=final_prefix,
                    )
                    s3.delete_object(Bucket=bucket, Key=key)
                    self.logger.info(
                        f"[write_output] Renamed s3://{bucket}/{key} "
                        f"-> s3://{final_bucket}/{final_prefix}"
                    )
                    break
        except Exception as exc:
            self.logger.warn(f"[write_output] boto3 rename skipped: {exc}")

    @staticmethod
    def _parse_s3_path(s3_path: str):
        """Split 's3://bucket/prefix' into (bucket, prefix)."""
        path = s3_path.replace("s3://", "")
        parts = path.split("/", 1)
        return parts[0], parts[1] if len(parts) > 1 else ""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    processor = SearchKeywordRevenueProcessor(spark, logger)

    # 1. Read conformed product-level data
    df = processor.read_conformed(PRODUCT_INPUT_PATH)

    # 2. Keep only purchase events (event_list contains "1")
    purchase_df = processor.filter_purchase_events(df)

    # 3. Enrich with search engine domain + keyword; drop non-search rows
    search_df = processor.enrich_with_search_info(purchase_df)

    # 4. Aggregate revenue by (domain, keyword), sorted desc
    result_df = processor.aggregate_revenue(search_df)

    result_df.show(50, truncate=False)

    # 5. Write tab-delimited output
    processor.write_output(result_df, CURATED_OUTPUT_PATH, OUTPUT_FILENAME)

    logger.info(f"[main] Done. Output: {CURATED_OUTPUT_PATH}{OUTPUT_FILENAME}")

    job.commit()


if __name__ == "__main__":
    main()
