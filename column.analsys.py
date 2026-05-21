import boto3
import pandas as pd
from io import BytesIO

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — point to your output CSV
# ══════════════════════════════════════════════════════════════════════════════
BUCKET        = "pske-stg-maintenance"
OUTPUT_PREFIX = "projects/ard-iot-data/reporting/analysis/"
TARGET_COL    = "metadata_measurementconfigurationname"   # yellow column

# paste your exact CSV filename here
CSV_FILENAME  = "03_reporting_data_SchemaType1_20260513_20260519.csv"

s3 = boto3.client("s3")

# ══════════════════════════════════════════════════════════════════════════════
# HELPER
# ══════════════════════════════════════════════════════════════════════════════
def write_csv_s3(df, filename, desc=""):
    if df is None or len(df) == 0:
        print(f"  SKIP {filename} — empty")
        return None
    key = f"{OUTPUT_PREFIX}measurement_analysis/{filename}"
    try:
        buf = BytesIO()
        df.to_csv(buf, index=False, encoding="utf-8")
        buf.seek(0)
        s3.put_object(Bucket=BUCKET, Key=key,
                      Body=buf.read(),
                      ContentType="text/csv; charset=utf-8")
        size = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        print(f"  ✓ {desc:50s} ({size:,} bytes)")
        return f"s3://{BUCKET}/{key}"
    except Exception as e:
        print(f"  ✗ FAILED {filename}: {e}")
        return None

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — load your CSV from S3
# ══════════════════════════════════════════════════════════════════════════════
print("━"*60)
print("STEP 1 — loading CSV from S3")
print("━"*60)

key = f"{OUTPUT_PREFIX}{CSV_FILENAME}"
try:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    df  = pd.read_csv(BytesIO(obj["Body"].read()))
    print(f"  ✓ Loaded : {len(df):,} rows x {len(df.columns):,} cols")
except Exception as e:
    print(f"  ✗ Could not load {key}: {e}")
    print(f"\n  Available CSVs in {OUTPUT_PREFIX}:")
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=OUTPUT_PREFIX
    ):
        for obj in page.get("Contents", []):
            name = obj["Key"].split("/")[-1]
            size = round(obj["Size"]/1024, 1)
            if name.endswith(".csv"):
                print(f"    {name}  ({size} KB)")
    raise

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — confirm yellow column exists and has data
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print(f"STEP 2 — checking column: {TARGET_COL}")
print("━"*60)

# find column — handle slight name differences
matched_col = None
for col in df.columns:
    if col.strip().lower() == TARGET_COL.strip().lower():
        matched_col = col
        break
if not matched_col:
    # fuzzy match
    for col in df.columns:
        if "measurementconfigurationname" in col.lower():
            matched_col = col
            break

if not matched_col:
    print(f"  ✗ Column not found: {TARGET_COL}")
    print(f"  Available columns containing 'measurement':")
    for c in df.columns:
        if "measurement" in c.lower():
            print(f"    {c}")
    raise RuntimeError(f"Column not found: {TARGET_COL}")

print(f"  ✓ Found column  : {matched_col}")
print(f"  Total rows      : {len(df):,}")
print(f"  Non-null rows   : {df[matched_col].notna().sum():,}")
print(f"  Null rows       : {df[matched_col].isna().sum():,}")
print(f"  Fill %          : {round(df[matched_col].notna().mean()*100,1)}%")
print(f"  Unique values   : {df[matched_col].nunique()}")

# ── keep only rows where this column has a value ──────────────────────────────
df_valid = df[df[matched_col].notna()].copy()
df_valid[matched_col] = df_valid[matched_col].astype(str).str.strip()
print(f"\n  Rows with data  : {len(df_valid):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — analysis of the yellow column
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 3 — measurement name analysis")
print("━"*60)

# ── A: value counts ───────────────────────────────────────────────────────────
value_counts = (df_valid[matched_col]
                .value_counts()
                .reset_index()
                .rename(columns={matched_col: "measurement_name",
                                 "count":     "row_count"}))
value_counts["pct_of_total"] = round(
    value_counts["row_count"] / len(df_valid) * 100, 1
)
value_counts["rank"] = range(1, len(value_counts)+1)

print(f"\n  All measurement names found:")
print(f"  {'rank':<5} {'count':>6}  {'pct':>6}  name")
print(f"  {'─'*5} {'─'*6}  {'─'*6}  {'─'*50}")
for _, row in value_counts.iterrows():
    print(f"  {int(row['rank']):<5} {int(row['row_count']):>6}  "
          f"{row['pct_of_total']:>5}%  {row['measurement_name']}")

# ── B: per measurement — which other columns have data ───────────────────────
meta_cols     = ["_date_folder","_mile_bucket"]
exclude_cols  = [matched_col] + meta_cols
data_cols     = [c for c in df_valid.columns if c not in exclude_cols]

coverage_rows = []
for mname in value_counts["measurement_name"]:
    sub = df_valid[df_valid[matched_col] == mname]
    row = {
        "measurement_name" : mname,
        "row_count"        : len(sub),
    }
    # count how many data columns are populated for this measurement
    for col in data_cols:
        fill = round(sub[col].notna().mean() * 100, 1)
        row[col] = fill
    coverage_rows.append(row)

coverage_df = pd.DataFrame(coverage_rows)

# ── C: for the highlighted measurement (SensorSignals) — deep column analysis
target_measurement = "Penske_FL_ExtendedTesting_Measurement_SensorSignals"
# find best match
best_match = None
for mname in value_counts["measurement_name"]:
    if "sensorsignal" in mname.lower() or "sensor_signal" in mname.lower():
        best_match = mname
        break
if not best_match:
    best_match = value_counts["measurement_name"].iloc[0]

print(f"\n  Deep analysis on: {best_match}")
sub_df = df_valid[df_valid[matched_col] == best_match].copy()
print(f"  Rows            : {len(sub_df):,}")

# columns with data for this specific measurement
col_fill = []
for col in data_cols:
    fill    = round(sub_df[col].notna().mean() * 100, 1)
    unique  = sub_df[col].nunique()
    is_num  = pd.api.types.is_numeric_dtype(sub_df[col])
    samples = (sub_df[col].dropna().astype(str)
               .str.strip().head(3).tolist())
    samples = (samples + ["","",""])[:3]
    if fill > 0:
        col_fill.append({
            "column_name"  : col,
            "fill_pct"     : fill,
            "unique_values": unique,
            "is_numeric"   : is_num,
            "sample_1"     : samples[0],
            "sample_2"     : samples[1],
            "sample_3"     : samples[2],
        })

sensor_cols_df = (pd.DataFrame(col_fill)
                  .sort_values("fill_pct", ascending=False)
                  .reset_index(drop=True))

print(f"  Columns with data : {len(sensor_cols_df)}")

# ── D: numeric stats for this measurement ─────────────────────────────────────
numeric_cols = [
    c["column_name"] for _, c in sensor_cols_df.iterrows()
    if c["is_numeric"] and c["fill_pct"] >= 30
]
if numeric_cols:
    num_stats = (sub_df[numeric_cols]
                 .describe(percentiles=[.25,.5,.75,.90,.95])
                 .T.reset_index()
                 .rename(columns={"index":"column"}))
    num_stats["fill_pct"] = [
        round(sub_df[c].notna().mean()*100,1)
        for c in numeric_cols
    ]
    num_stats = num_stats.sort_values("fill_pct", ascending=False)
else:
    num_stats = pd.DataFrame()

# ── E: per date breakdown ──────────────────────────────────────────────────────
date_breakdown = pd.DataFrame()
if "_date_folder" in df_valid.columns:
    date_breakdown = (df_valid
        .groupby(["_date_folder", matched_col])
        .size()
        .reset_index(name="row_count")
        .sort_values(["_date_folder","row_count"], ascending=[True,False]))

# ── F: per mile bucket breakdown ──────────────────────────────────────────────
mile_breakdown = pd.DataFrame()
if "_mile_bucket" in df_valid.columns:
    mile_breakdown = (df_valid
        .groupby(["_mile_bucket", matched_col])
        .size()
        .reset_index(name="row_count")
        .sort_values(["_mile_bucket","row_count"], ascending=[True,False]))

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — write all analysis outputs
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 4 — writing analysis outputs to S3")
print("━"*60)

write_csv_s3(
    value_counts,
    "01_measurement_name_counts.csv",
    "all measurement names + row counts"
)
write_csv_s3(
    coverage_df,
    "02_measurement_column_coverage.csv",
    "per measurement — which columns have data"
)
write_csv_s3(
    sensor_cols_df,
    "03_sensor_signals_columns.csv",
    f"{best_match} — columns with data"
)
write_csv_s3(
    num_stats,
    "04_sensor_numeric_stats.csv",
    "numeric column stats for sensor signals"
)
write_csv_s3(
    df_valid[[matched_col] + [c for c in data_cols
              if c in sensor_cols_df["column_name"].tolist()]],
    "05_sensor_signals_data.csv",
    "sensor signals data (populated columns only)"
)
if not date_breakdown.empty:
    write_csv_s3(
        date_breakdown,
        "06_by_date.csv",
        "measurement counts by date"
    )
if not mile_breakdown.empty:
    write_csv_s3(
        mile_breakdown,
        "07_by_mile_bucket.csv",
        "measurement counts by mile bucket"
    )

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — print client ready summary
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'━'*60}")
print("CLIENT SUMMARY")
print(f"{'━'*60}")

print(f"""
  Column analysed : {matched_col}
  Total records   : {len(df_valid):,}
  Unique names    : {df_valid[matched_col].nunique()}

  Measurement breakdown:""")

for _, row in value_counts.iterrows():
    bar = "█" * int(row["pct_of_total"] / 2)
    print(f"    {row['measurement_name'][:55]:<55} "
          f"{int(row['row_count']):>5} rows  {bar}")

print(f"""
  Deep analysis on : {best_match}
  Rows             : {len(sub_df):,}
  Columns with data: {len(sensor_cols_df):,}
  Numeric columns  : {len(numeric_cols)}

  Top 10 populated columns:
  {'column':<45} {'fill%':>6}  {'unique':>7}  sample""")
print(f"  {'─'*45} {'─'*6}  {'─'*7}  {'─'*20}")
for _, row in sensor_cols_df.head(10).iterrows():
    print(f"  {row['column_name']:<45} "
          f"{row['fill_pct']:>5}%  "
          f"{row['unique_values']:>7,}  "
          f"{str(row['sample_1'])[:30]}")

print(f"\n  Output: s3://{BUCKET}/{OUTPUT_PREFIX}measurement_analysis/")
print(f"{'━'*60}")
