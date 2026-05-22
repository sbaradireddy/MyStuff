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
