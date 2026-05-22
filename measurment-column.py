# --- Glue notebook (PySpark) ---

# Source and destination
src = "s3://pske-stg-maintenance/projects/ard-iot-data/ps.NA_PENSKE_TRIAL.measurements.results/20260520/"
dst = "s3://pske-stg-maintenance/projects/ard-iot-data/output/penske_fl_extendedtesting/"

# Value to filter on
target = "penske fl extendedtesting measurement sensorsignals"

# 1) Read, prune to needed columns, filter
out = (
    spark.read.parquet(src)
    .select("metadata_measurementconfigurationname", "measurement_sensorsignals")
    .filter("metadata_measurementconfigurationname = '" + target + "'")
)

# 2) Preview before writing
out.show(20, truncate=False)
print("rows:", out.count())

# 3) Write to S3 as a single CSV file (with header)
(
    out.coalesce(1)
    .write
    .mode("overwrite")          # overwrites dst on re-run; use "append" to add instead
    .option("header", True)
    .csv(dst)
)

print("done -> " + dst)
