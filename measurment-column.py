# ============================================================
# Glue Notebook: Unpivot Penske IoT sensor data -> CSV for QuickSight
# Strategy: process per measurement_name group (shared schema),
#           unpivot dynamically, union, write CSV.
# ============================================================
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from functools import reduce
import re

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ---------------- CONFIG ----------------
SRC         = "s3://pske-stg-maintenance/projects/ard-iot-data/ps.NA_PENSKE_TRIAL.measurements.results/"
OUT_DETAIL  = "s3://pske-stg-maintenance/analytics/sensor_long_csv/"
OUT_SUMMARY = "s3://pske-stg-maintenance/analytics/measurement_summary_csv/"
MEAS_COL    = "metadata_measurementconfigurationname"   # the column you care about

# ---------------- READ ----------------
# recursiveFileLookup walks every date/subfolder/SchemaType file.
df = (spark.read
      .option("recursiveFileLookup", "true")
      .option("mergeSchema", "true")          # ok here: we only touch shared cols per group
      .parquet(SRC))

print("Loaded columns:", len(df.columns), "| rows:", df.count())


def base_signal(colname):
    """resultdata_enginespeed_154 -> enginespeed"""
    return re.sub(r"_\d+$", "", colname.replace("resultdata_", ""))

# Distinct measurement names actually present
groups = [r[0] for r in df.select(MEAS_COL).distinct().collect() if r[0] is not None]
print("Measurement groups:", groups)

# Metadata columns we keep on every row (adjust to taste)
KEEP = [c for c in df.columns
        if c in (MEAS_COL, "metadata_vin", "metadata_vehicleid",
                 "metadata_teststepid", "metadata_start", "metadata_end")]

long_parts = []
for g in groups:
    gdf = df.filter(F.col(MEAS_COL) == g)

    # Within this group, find sensor columns that are actually populated
    signal_cols = [c for c in gdf.columns if c.startswith("resultdata_")]
    # Drop all-null columns for this group (they belong to other schemas)
    non_null = [c for c, n in
                gdf.select([F.count(F.col(c)).alias(c) for c in signal_cols])
                   .first().asDict().items() if n > 0]
    if not non_null:
        continue

    # CRITICAL: cast every value to string so stack() has one common type.
    # (numeric/bool/string mix would otherwise fail)
    casted = [F.col(c).cast("string").alias(c) for c in non_null]
    gdf2 = gdf.select(*KEEP, *casted)

    pairs = ",".join([f"'{c}', `{c}`" for c in non_null])
    expr  = f"stack({len(non_null)}, {pairs}) as (signal_col, signal_value)"

    part = (gdf2.select(*KEEP, F.expr(expr))
                .filter(F.col("signal_value").isNotNull())
                .withColumn("signal_name",
                            F.regexp_replace(
                                F.regexp_replace(F.col("signal_col"), r"^resultdata_", ""),
                                r"_\d+$", ""))
                .withColumnRenamed(MEAS_COL, "measurement_name"))
    long_parts.append(part)

long_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), long_parts)
long_df.cache()
print("Unpivoted rows:", long_df.count())


# -------- DETAIL CSV (sharded; partitioned by measurement_name) --------
(long_df.write.mode("overwrite")
   .option("header", "true")
   .partitionBy("measurement_name")
   .csv(OUT_DETAIL))

# -------- SUMMARY CSV (matches your 01_measurement_name_counts.csv) --------
total = long_df.count()
summary = (long_df.groupBy("measurement_name")
           .agg(F.count("*").alias("row_count"))
           .withColumn("pct", F.round(F.col("row_count") / total * 100, 1))
           .orderBy(F.desc("row_count")))
summary = summary.withColumn(
    "rank", F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"]).Window.orderBy(F.desc("row_count"))))

# coalesce(1) => single CSV file (summary is small, so this is safe)
(summary.coalesce(1).write.mode("overwrite")
   .option("header", "true").csv(OUT_SUMMARY))
summary.show(truncate=False)
