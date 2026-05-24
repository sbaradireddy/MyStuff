import re
from pyspark.sql import functions as F

# --- Config ---
S3_PATH     = "s3://pske-stg-maintenance/projects/ard-iot-data/ps.NA_PENSKE_TRIAL.measurements.results/20260521/020000/"
OUTPUT_PATH = "s3://pske-stg-maintenance/projects/ard-iot-data/filtered_sensorsignals_long/"
COLUMN      = "metadata_measurementconfigurationname"
TARGET      = "Penske_FL_ExtendedTesting_Measurement_SensorSignals"

result = (
    spark.read.option("mergeSchema", "true").parquet(S3_PATH)
    .filter(F.col(COLUMN) == TARGET)
)

# 1) Group columns by base name (name with the trailing number stripped)
pat = re.compile(r'^(.*?)(\d+)$')
groups, statics = {}, []
for c in result.columns:
    m = pat.match(c)
    if m:
        base = m.group(1).strip()        # e.g. "result data_enginespeed"
        idx  = int(m.group(2))           # e.g. 154
        groups.setdefault(base, {})[idx] = c
    else:
        statics.append(c)                # non-numbered cols (metadata, ids, etc.)

# Only collapse bases that form a real numbered series (>1 column)
series   = {b: m for b, m in groups.items() if len(m) > 1}
statics += [next(iter(m.values())) for b, m in groups.items() if len(m) == 1]

# Clean final names: "result data_enginespeed" -> "resultdata_enginespeed"
clean = {b: re.sub(r'\s+', '', b) for b in series}

print("Static columns kept:", statics)
for b in series:
    idxs = sorted(series[b])
    print(f"  {clean[b]}: {len(idxs)} cols ({idxs[0]}..{idxs[-1]})")

# 2) Unpivot: one row per sample_index, one column per sensor base
all_idx = sorted({i for m in series.values() for i in m})

struct_arr = F.array(*[
    F.struct(
        F.lit(i).alias("sample_index"),
        *[
            (F.col(f"`{series[b][i]}`") if i in series[b] else F.lit(None)).alias(clean[b])
            for b in series
        ]
    )
    for i in all_idx
])

long_df = (
    result
    .withColumn("_s", F.explode(struct_arr))
    .select(*[F.col(f"`{c}`") for c in statics], F.col("_s.*"))
)

long_df.show(20, truncate=False)
print("Output rows:", long_df.count())

# 3) Write to S3
long_df.write.mode("overwrite").parquet(OUTPUT_PATH)
print("Written to:", OUTPUT_PATH)
