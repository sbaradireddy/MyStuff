import boto3
import pandas as pd
from io import BytesIO

# ══════════════════════════════════════════════════════════════════════════════
# ⚙️  CHANGE ONLY THESE LINES
# ══════════════════════════════════════════════════════════════════════════════
BUCKET        = "pske-stg-maintenance"
CSV_KEY       = "projects/ard-iot-data-analysis/quicksight-dataset/ps.NA_PENSKE_TRIAL.measurements.results/unified_vehicle_data_20260520_191224.csv"
OUTPUT_PREFIX = "projects/ard-iot-data-analysis/quicksight-dataset/ps.NA_PENSKE_TRIAL.measurements.results/measurement_analysis/"
TARGET_COL    = "metadata_measurementconfigurationname"
# ══════════════════════════════════════════════════════════════════════════════

s3 = boto3.client("s3")

# ──────────────────────────────────────────────────────────────────────────────
# WRITE
# ──────────────────────────────────────────────────────────────────────────────
def write_csv_s3(df, filename, desc=""):
    if df is None or len(df) == 0:
        print(f"  SKIP {filename} — empty")
        return None
    key = f"{OUTPUT_PREFIX}{filename}"
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

# ──────────────────────────────────────────────────────────────────────────────
# SAFE COLUMN STATS — fixes the length mismatch error
# ──────────────────────────────────────────────────────────────────────────────
def safe_col_stats(df, cols):
    df   = df.reset_index(drop=True)
    rows = []
    for col in cols:
        if col not in df.columns:
            continue
        try:
            series  = df[col]
            fill    = round(series.notna().mean() * 100, 1)
            unique  = series.nunique()
            is_num  = pd.api.types.is_numeric_dtype(series)
            samples = (series.dropna()
                       .astype(str).str.strip()
                       .loc[lambda x: (x != "nan") & (x != "")]
                       .head(5).tolist())
            samples = (samples + ["","","","",""])[:5]
            rows.append({
                "column_name"  : col,
                "fill_pct"     : fill,
                "unique_values": unique,
                "is_numeric"   : is_num,
                "sample_1"     : samples[0],
                "sample_2"     : samples[1],
                "sample_3"     : samples[2],
                "sample_4"     : samples[3],
                "sample_5"     : samples[4],
            })
        except Exception as e:
            print(f"    SKIP col {col}: {e}")
    return pd.DataFrame(rows).sort_values("fill_pct", ascending=False)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — load CSV from S3
# ══════════════════════════════════════════════════════════════════════════════
print("━"*60)
print("STEP 1 — loading CSV")
print("━"*60)

obj = s3.get_object(Bucket=BUCKET, Key=CSV_KEY)
df  = pd.read_csv(BytesIO(obj["Body"].read()))
df  = df.reset_index(drop=True)   # reset immediately after load
print(f"  ✓ Loaded : {len(df):,} rows x {len(df.columns):,} cols")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — find yellow column
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 2 — finding target column")
print("━"*60)

matched_col = None
for col in df.columns:
    if col.strip().lower() == TARGET_COL.strip().lower():
        matched_col = col
        break
if not matched_col:
    for col in df.columns:
        if "measurementconfigurationname" in col.lower():
            matched_col = col
            break

if not matched_col:
    print("  ✗ Column not found. Columns containing 'measurement':")
    for c in df.columns:
        if "measurement" in c.lower():
            print(f"    {c}")
    raise RuntimeError(f"Column not found: {TARGET_COL}")

print(f"  ✓ Column    : {matched_col}")
print(f"  Total rows  : {len(df):,}")
print(f"  Non-null    : {df[matched_col].notna().sum():,}")
print(f"  Fill %      : {round(df[matched_col].notna().mean()*100,1)}%")
print(f"  Unique vals : {df[matched_col].nunique()}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — filter rows that have a value + reset index
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 3 — filtering valid rows")
print("━"*60)

df_valid = (df[df[matched_col].notna()]
            .copy()
            .reset_index(drop=True))          # ← reset index here
df_valid[matched_col] = df_valid[matched_col].astype(str).str.strip()
print(f"  Rows with data : {len(df_valid):,}")

# value counts
value_counts = (df_valid[matched_col]
                .value_counts()
                .reset_index())
value_counts.columns   = ["measurement_name","row_count"]
value_counts["pct"]    = round(
    value_counts["row_count"] / len(df_valid) * 100, 1
)
value_counts["rank"]   = range(1, len(value_counts)+1)

print(f"\n  Measurement names found:")
print(f"  {'#':<4} {'rows':>6}  {'%':>5}  name")
print(f"  {'─'*4} {'─'*6}  {'─'*5}  {'─'*50}")
for _, row in value_counts.iterrows():
    bar = "█" * int(row["pct"] / 2)
    print(f"  {int(row['rank']):<4} "
          f"{int(row['row_count']):>6}  "
          f"{row['pct']:>4}%  "
          f"{row['measurement_name'][:55]}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — pick SensorSignals measurement (yellow highlighted row)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 4 — deep analysis on SensorSignals")
print("━"*60)

# find SensorSignals automatically
best_match = None
for mname in value_counts["measurement_name"]:
    if "sensorsignal" in mname.lower().replace("_",""):
        best_match = mname
        break
if not best_match:
    best_match = value_counts["measurement_name"].iloc[0]

print(f"  Target : {best_match}")

# filter + reset index immediately
sub_df = (df_valid[df_valid[matched_col] == best_match]
          .copy()
          .reset_index(drop=True))            # ← reset index here too
print(f"  Rows   : {len(sub_df):,}")

# define columns
meta_cols    = ["_date_folder","_mile_bucket"]
exclude_cols = [matched_col] + meta_cols
data_cols    = [c for c in sub_df.columns if c not in exclude_cols]

# safe column stats — no length mismatch
sensor_cols_df = safe_col_stats(sub_df, data_cols)
print(f"  Columns with data : {len(sensor_cols_df):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — numeric stats
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 5 — numeric stats")
print("━"*60)

numeric_cols = sensor_cols_df[
    (sensor_cols_df["is_numeric"] == True) &
    (sensor_cols_df["fill_pct"]   >= 10)
]["column_name"].tolist()

print(f"  Numeric columns : {len(numeric_cols)}")

if numeric_cols:
    num_stats = (sub_df[numeric_cols]
                 .reset_index(drop=True)
                 .describe(percentiles=[.25,.5,.75,.90,.95])
                 .T
                 .reset_index()
                 .rename(columns={"index":"column"}))
    num_stats["fill_pct"] = [
        round(sub_df[c].notna().mean()*100,1)
        for c in numeric_cols
    ]
    num_stats = num_stats.sort_values("fill_pct", ascending=False)
else:
    num_stats = pd.DataFrame(
        columns=["column","count","mean","std",
                 "min","25%","50%","75%","90%","95%","max","fill_pct"]
    )

# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — date and mile breakdown
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 6 — date and mile breakdown")
print("━"*60)

if "_date_folder" in df_valid.columns:
    date_breakdown = (df_valid
        .groupby(["_date_folder", matched_col], as_index=False)
        .size()
        .rename(columns={"size":"row_count"})
        .sort_values(["_date_folder","row_count"],
                      ascending=[True,False])
        .reset_index(drop=True))
    print(f"  Date breakdown rows : {len(date_breakdown):,}")
else:
    date_breakdown = pd.DataFrame()
    print("  No _date_folder column found")

if "_mile_bucket" in df_valid.columns:
    mile_breakdown = (df_valid
        .groupby(["_mile_bucket", matched_col], as_index=False)
        .size()
        .rename(columns={"size":"row_count"})
        .sort_values(["_mile_bucket","row_count"],
                      ascending=[True,False])
        .reset_index(drop=True))
    print(f"  Mile breakdown rows : {len(mile_breakdown):,}")
else:
    mile_breakdown = pd.DataFrame()
    print("  No _mile_bucket column found")

# value distribution for top text columns
text_cols = sensor_cols_df[
    (sensor_cols_df["is_numeric"] == False) &
    (sensor_cols_df["fill_pct"]   >= 10)
]["column_name"].head(15).tolist()

dist_rows = []
for col in text_cols:
    vc = sub_df[col].value_counts().head(10)
    for val, cnt in vc.items():
        dist_rows.append({
            "column" : col,
            "value"  : str(val)[:120],
            "count"  : cnt,
            "pct"    : round(cnt / len(sub_df) * 100, 1),
        })
dist_df = pd.DataFrame(dist_rows).reset_index(drop=True)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — write all outputs
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 7 — writing to S3")
print("━"*60)

write_csv_s3(value_counts,
    "01_measurement_name_counts.csv",
    "all measurement names + counts")

write_csv_s3(sensor_cols_df,
    "02_sensor_signals_columns.csv",
    "SensorSignals columns with fill% + samples")

write_csv_s3(sub_df[[matched_col] +
             sensor_cols_df[sensor_cols_df["fill_pct"]>0]
             ["column_name"].tolist()],
    "03_sensor_signals_data.csv",
    "SensorSignals actual data")

write_csv_s3(num_stats,
    "04_numeric_stats.csv",
    "numeric column statistics")

write_csv_s3(dist_df,
    "05_value_distributions.csv",
    "top value distributions")

if not date_breakdown.empty:
    write_csv_s3(date_breakdown,
        "06_by_date.csv",
        "row counts by date")

if not mile_breakdown.empty:
    write_csv_s3(mile_breakdown,
        "07_by_mile_bucket.csv",
        "row counts by mile bucket")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 8 — print client summary
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'━'*60}")
print("CLIENT SUMMARY")
print(f"{'━'*60}")
print(f"""
  Column    : {matched_col}
  Total rows: {len(df_valid):,}
  Unique    : {df_valid[matched_col].nunique()}

  Measurement breakdown:""")

for _, row in value_counts.iterrows():
    bar = "█" * int(row["pct"] / 2)
    print(f"    {row['measurement_name'][:55]:<55} "
          f"{int(row['row_count']):>5} rows  {row['pct']}%  {bar}")

print(f"""
  Deep analysis : {best_match}
  Rows          : {len(sub_df):,}
  Cols with data: {len(sensor_cols_df):,}
  Numeric cols  : {len(numeric_cols)}

  Top 15 columns:
  {'column':<45} {'fill%':>6}  {'unique':>7}  sample""")
print(f"  {'─'*45} {'─'*6}  {'─'*7}  {'─'*25}")
for _, row in sensor_cols_df.head(15).iterrows():
    print(f"  {row['column_name']:<45} "
          f"{row['fill_pct']:>5}%  "
          f"{row['unique_values']:>7,}  "
          f"{str(row['sample_1'])[:30]}")

print(f"\n  Output: s3://{BUCKET}/{OUTPUT_PREFIX}")
print(f"{'━'*60}")
