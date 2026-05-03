"""
AWS Glue Job: Conformed -> Curated
-----------------------------------------------------------------------
Answers the business question:
    "How much revenue is the client getting from external Search Engines,
     and which keywords are performing the best based on revenue?"

Session Attribution Logic
--------------------------
A visitor's session is identified by their IP address.

  Step 1 — Read ALL HITS (base table from Script 1).
            For each IP, find the true first hit (lowest hit_time_gmt)
            across ALL page types — not just product pages.
            This is critical: the entry hit for many visitors is a Home
            or category page that carries no product_list and would be
            missing if we only looked at the product-level table.
            If that first hit's referrer is an external search engine,
            capture the domain and keyword. IPs that did not enter via
            a search engine are excluded.

  Step 2 — Read PRODUCT-LEVEL HITS (product table from Script 1).
            Keep only rows where event_list contains the standalone
            purchase event "1" AND product_total_revenue > 0.

  Step 3 — JOIN session search origin (Step 1) onto purchase rows
            (Step 2) by IP. This attributes each purchase to the
            keyword that brought the visitor in.

  Step 4 — Aggregate total revenue by (search_engine_domain, keyword),
            sort descending, write tab-delimited output.

Output: YYYY-mm-dd_SearchKeywordPerformance.tab
"""

import sys
from datetime import date
from urllib.parse import parse_qs, urlparse

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window, functions as F
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

# Table 1 written by Script 1: ALL hits (needed for true first-hit per IP)
ALL_HITS_INPUT_PATH = (
    "s3://s3-conformeddev-bucket-528733132057/externalclickdata/all_hits/"
)

# Table 2 written by Script 1: product-level rows (needed for revenue)
PRODUCT_INPUT_PATH = (
    "s3://s3-conformeddev-bucket-528733132057/externalclickdata/product_level/"
)

CURATED_OUTPUT_PATH = (
    "s3://s3-curateddev-bucket-528733132057/externalclickdata/search_keyword_performance/"
)

OUTPUT_FILENAME = f"{date.today().strftime('%Y-%m-%d')}_SearchKeywordPerformance.tab"

# Known search engines and the query-string params that carry the keyword
SEARCH_ENGINE_PARAMS = {
    "google.com":     ["q"],
    "bing.com":       ["q"],
    "yahoo.com":      ["p", "q"],
    "msn.com":        ["q"],
    "ask.com":        ["q"],
    "aol.com":        ["q", "query"],
    "baidu.com":      ["wd", "word"],
    "duckduckgo.com": ["q"],
}

# ---------------------------------------------------------------------------
# UDFs — parse referrer URL
# ---------------------------------------------------------------------------

def _registered_domain(hostname: str):
    """Strip 'www.' and return the domain if it's a known search engine."""
    if not hostname:
        return None
    host = hostname.lower().lstrip("www.")
    for domain in SEARCH_ENGINE_PARAMS:
        if host == domain or host.endswith("." + domain):
            return domain
    return None


def extract_search_engine(referrer: str):
    if not referrer:
        return None
    try:
        return _registered_domain(urlparse(referrer).hostname or "")
    except Exception:
        return None


def extract_keyword(referrer: str):
    if not referrer:
        return None
    try:
        parsed = urlparse(referrer)
        domain = _registered_domain(parsed.hostname or "")
        if not domain:
            return None
        qs = parse_qs(parsed.query)
        for param in SEARCH_ENGINE_PARAMS[domain]:
            if param in qs and qs[param]:
                # Lowercase so "Ipod" and "ipod" are treated as the same keyword
                return qs[param][0].strip().lower()
    except Exception:
        pass
    return None


udf_search_engine = F.udf(extract_search_engine, StringType())
udf_keyword       = F.udf(extract_keyword,       StringType())

# ---------------------------------------------------------------------------
# Processor class
# ---------------------------------------------------------------------------

class SearchKeywordRevenueProcessor:
    """
    Attributes purchase revenue to the search engine / keyword that
    initiated each visitor's session (identified by IP address).

    Uses two conformed tables:
      all_hits_df  — every page hit, used to find the true first hit per IP
      product_df   — product-level rows, used to extract purchase revenue
    """

    def __init__(self, spark_session, logger):
        self.spark  = spark_session
        self.logger = logger

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def read_all_hits(self, path: str):
        df = self.spark.read.parquet(path)
        self.logger.info(f"[read_all_hits] {df.count()} rows from {path}")
        return df

    def read_products(self, path: str):
        df = self.spark.read.parquet(path)
        self.logger.info(f"[read_products] {df.count()} rows from {path}")
        return df

    # ------------------------------------------------------------------
    # Step 1: True first hit per IP → search engine + keyword
    # ------------------------------------------------------------------

    def get_session_search_origin(self, all_hits_df):
        """
        Finds the very first hit for each IP across ALL page types
        (not just product pages). If that hit's referrer is an external
        search engine, captures the domain and keyword.

        IPs whose first hit did NOT come from a search engine are dropped
        — they are not search-driven sessions and should not appear in
        the keyword performance report.

        Returns: ip | search_engine_domain | search_keyword
        """
        window = Window.partitionBy("ip").orderBy(F.col("hit_time_gmt").asc())

        session_df = (
            all_hits_df
            # Rank all hits per IP by time; rank=1 is the entry (first) hit
            .withColumn("_rank", F.rank().over(window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
            # Parse the referrer of that first hit
            .withColumn("search_engine_domain", udf_search_engine(F.col("referrer")))
            .withColumn("search_keyword",        udf_keyword(F.col("referrer")))
            # Drop IPs that didn't arrive from a search engine
            .filter(
                F.col("search_engine_domain").isNotNull()
                & F.col("search_keyword").isNotNull()
                & (F.col("search_keyword") != "")
            )
            .select("ip", "search_engine_domain", "search_keyword")
            # Guard against ties in hit_time_gmt: take one row per IP
            .dropDuplicates(["ip"])
        )

        self.logger.info(
            f"[get_session_search_origin] {session_df.count()} IPs "
            f"entered via external search engine"
        )
        return session_df

    # ------------------------------------------------------------------
    # Step 2: Purchase revenue from product-level table
    # ------------------------------------------------------------------

    def get_purchase_revenue(self, product_df):
        """
        Keeps only rows that:
          (a) are purchase events — event_list contains standalone token "1"
              (regex guards against matching events 10, 11, 12, etc.)
          (b) have a positive product_total_revenue recorded

        Per Appendix B: "Revenue is only actualized when the purchase
        event is set in the events_list."

        Returns: ip | product_name | product_total_revenue
        """
        purchase_df = (
            product_df
            .filter(
                F.col("event_list").isNotNull()
                & F.col("event_list").rlike(r"(^|,)\s*1\s*(,|$)")
                & F.col("product_total_revenue").isNotNull()
                & (F.col("product_total_revenue") > 0)
            )
            .select("ip", "product_name", "product_total_revenue")
        )

        self.logger.info(
            f"[get_purchase_revenue] {purchase_df.count()} purchase product rows"
        )
        return purchase_df

    # ------------------------------------------------------------------
    # Step 3 + 4: Attribute + aggregate
    # ------------------------------------------------------------------

    def attribute_and_aggregate(self, session_df, purchase_df):
        """
        Left-joins purchase revenue onto session search origin by IP.
        ALL search-driven sessions appear in the output — IPs that never
        purchased (e.g. 112.33.98.231) show Revenue = 0 instead of being
        dropped. IPs that did purchase contribute their actual revenue.

        Aggregates total revenue by (search_engine_domain, search_keyword)
        and sorts descending so the best-performing keyword is first.
        """
        result_df = (
            # Start from session_df (left) so every search-driven IP is kept
            session_df
            .join(purchase_df, on="ip", how="left")
            # NULL revenue (no purchase) becomes 0
            .withColumn(
                "product_total_revenue",
                F.coalesce(F.col("product_total_revenue"), F.lit(0.0)),
            )
            .groupBy("search_engine_domain", "search_keyword")
            .agg(F.sum("product_total_revenue").alias("Revenue"))
            .orderBy(F.col("Revenue").desc())
            .withColumnRenamed("search_engine_domain", "Search Engine Domain")
            .withColumnRenamed("search_keyword",       "Search Keyword")
        )

        self.logger.info(
            f"[attribute_and_aggregate] {result_df.count()} "
            f"(engine, keyword) combinations in final output"
        )
        return result_df

    # ------------------------------------------------------------------
    # Step 5: Write tab-delimited output
    # ------------------------------------------------------------------

    def write_output(self, df, output_path: str, filename: str):
        """
        Writes a single tab-delimited .tab file with a header row.
        coalesce(1) forces one part file; boto3 renames it to the
        required YYYY-mm-dd_SearchKeywordPerformance.tab convention.
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

        try:
            import boto3

            def parse_s3(path):
                p = path.replace("s3://", "")
                parts = p.split("/", 1)
                return parts[0], parts[1] if len(parts) > 1 else ""

            src_bucket, src_prefix = parse_s3(tmp_path)
            dst_bucket, dst_key    = parse_s3(output_path.rstrip("/") + "/" + filename)

            s3 = boto3.client("s3")
            objects = s3.list_objects_v2(Bucket=src_bucket, Prefix=src_prefix)

            for obj in objects.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv") or key.endswith(".tab"):
                    s3.copy_object(
                        Bucket=dst_bucket,
                        CopySource={"Bucket": src_bucket, "Key": key},
                        Key=dst_key,
                    )
                    s3.delete_object(Bucket=src_bucket, Key=key)
                    self.logger.info(
                        f"[write_output] Renamed "
                        f"s3://{src_bucket}/{key} -> s3://{dst_bucket}/{dst_key}"
                    )
                    break
        except Exception as exc:
            self.logger.warn(f"[write_output] boto3 rename skipped: {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    processor = SearchKeywordRevenueProcessor(spark, logger)

    # Step 1: Read all hits → true first hit per IP → search origin
    all_hits_df = processor.read_all_hits(ALL_HITS_INPUT_PATH)
    session_df  = processor.get_session_search_origin(all_hits_df)

    # Step 2: Read product rows → purchase revenue only
    product_df  = processor.read_products(PRODUCT_INPUT_PATH)
    purchase_df = processor.get_purchase_revenue(product_df)

    # Step 3 + 4: Attribute revenue to keyword, aggregate, sort
    result_df = processor.attribute_and_aggregate(session_df, purchase_df)

    result_df.show(50, truncate=False)

    # Step 5: Write output file
    processor.write_output(result_df, CURATED_OUTPUT_PATH, OUTPUT_FILENAME)

    logger.info(f"[main] Done -> {CURATED_OUTPUT_PATH}{OUTPUT_FILENAME}")
    job.commit()


if __name__ == "__main__":
    main()