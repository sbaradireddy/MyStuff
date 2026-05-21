import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from io import BytesIO
from collections import defaultdict
from datetime import datetime

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
BUCKET        = "pske-stg-maintenance"
SOURCE_PREFIX = "projects/ard-iot-data/ps.NA_PENSKE_TRIAL.measurements.results/"
OUTPUT_PREFIX = "projects/ard-iot-data/reporting/analysis/"
TARGET_SCHEMA = "SchemaType1"
DATE_FROM     = "20260513"
DATE_TO       = "20260519"
ROWS_PER_FILE = 100

# ── reporting thresholds — tune these ────────────────────────────────────────
MIN_FILL_PCT   = 30.0   # column must be 30%+ populated
MIN_UNIQUE     = 2      # column must have at least 2 unique values
TOP_N_COLS     = 200    # max columns to keep for reporting

RUN_TS = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
s3     = boto3.client("s3")

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
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
        print(f"  ✓ {desc:45s} ({size:,} bytes)")
        return f"s3://{BUCKET}/{key}"
    except Exception as e:
        print(f"  ✗ FAILED {filename}: {e}")
        return None

def safe(name):
    return (name.replace("[]","_arr").replace("[key]","_key")
                .replace("[value]","_val").replace(".","_")
                .replace(" ","_").lower())

def flatten_arrow(schema, prefix=""):
    cols = []
    for field in schema:
        path  = f"{prefix}.{field.name}" if prefix else field.name
        dtype = field.type
        if pa.types.is_struct(dtype):
            cols.extend(flatten_arrow(dtype, prefix=path))
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            elem = dtype.value_type
            if pa.types.is_struct(elem):
                cols.extend(flatten_arrow(elem, prefix=f"{path}[]"))
            else:
                cols.append((f"{path}[]", str(elem)))
        elif pa.types.is_map(dtype):
            cols.append((f"{path}[key]",   str(dtype.key_type)))
            cols.append((f"{path}[value]", str(dtype.item_type)))
        else:
            cols.append((path, str(dtype)))
    return cols

def flatten_record(record, prefix=""):
    flat = {}
    for k, v in record.items():
        key = f"{prefix}_{k}" if prefix else k
        key = safe(key)
        if isinstance(v, dict):
            flat.update(flatten_record(v, prefix=key))
        elif isinstance(v, list):
            flat[f"{key}_arr"] = str(v)[:200]
        else:
            flat[key] = v
    return flat

def read_file(bucket, key, nrows=100):
    """Try 4 methods — return df or None."""
    try:
        raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if len(raw) < 100:
            return None
        buf = BytesIO(raw)
        # method 1
        try:
            buf.seek(0)
            return pq.read_table(buf).to_pandas().head(nrows)
        except Exception:
            pass
        # method 2
        try:
            buf.seek(0)
            pf    = pq.ParquetFile(buf)
            batch = next(pf.iter_batches(batch_size=nrows))
            return batch.to_pandas()
        except Exception:
            pass
        # method 3
        try:
            buf.seek(0)
            return pd.read_parquet(buf).head(nrows)
        except Exception:
            pass
        # method 4 — read column by column (handles bad nested cols)
        try:
            buf.seek(0)
            table = pq.read_table(buf)
            rows  = {}
            for col in table.column_names:
                try:
                    rows[safe(col)] = table.column(col).to_pylist()
                except Exception:
                    rows[safe(col)] = [None] * table.num_rows
            return pd.DataFrame(rows).head(nrows)
        except Exception:
            pass
        return None
    except Exception as e:
        print(f"    SKIP {key.split('/')[-1]}: {e}")
        return None

TYPE_RANK = {
    "bool":1,"int8":2,"int16":3,"int32":4,"int64":5,
    "uint8":2,"uint16":3,"uint32":4,"uint64":5,
    "float":6,"float16":6,"float32":7,"float64":8,"double":8,
    "date32":9,"date64":9,
    "timestamp[ms]":10,"timestamp[us]":10,"timestamp[ns]":10,
    "string":11,"utf8":11,"large_utf8":11,
}

def resolve_type(dtypes):
    unique = list(set(dtypes))
    if len(unique) == 1: return unique[0]
    return sorted(unique,
        key=lambda t: TYPE_RANK.get(t.lower().strip(), 99),
        reverse=True)[0]

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — S3 listing
# ══════════════════════════════════════════════════════════════════════════════
print("━"*60)
print(f"STEP 1 — listing [{TARGET_SCHEMA}] [{DATE_FROM}→{DATE_TO}]")
print("━"*60)

seen_slots  = set()
target_keys = []
paginator   = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=BUCKET, Prefix=SOURCE_PREFIX):
    for obj in page.get("Contents", []):
        key  = obj["Key"]
        size = obj.get("Size", 0)
        if not key.endswith(".parquet") or size < 100:
            continue
        parts = key.split("/")
        try:
            date_folder = parts[-3]
            mile_bucket = parts[-2]
            fname       = parts[-1]
        except IndexError:
            continue
        if not date_folder.isdigit():
            continue
        if not (DATE_FROM <= date_folder <= DATE_TO):
            continue
        if fname.split("-")[0] != TARGET_SCHEMA:
            continue
        slot = (date_folder, mile_bucket)
        if slot in seen_slots:
            continue
        seen_slots.add(slot)
        target_keys.append((key, date_folder, mile_bucket))

print(f"  Files to process : {len(target_keys):,}")
if not target_keys:
    raise RuntimeError("0 files found — check SOURCE_PREFIX / dates / schema name")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — read files
# ══════════════════════════════════════════════════════════════════════════════
print(f"\nSTEP 2 — reading {len(target_keys)} files")
print("━"*60)

col_registry  = defaultdict(lambda: {
    "dtypes": [], "date_seen": set()
})
all_data_rows = []
read_ok = read_fail = 0

for i, (key, date_folder, mile_bucket) in enumerate(target_keys, 1):
    if i % 10 == 0 or i == 1:
        print(f"  [{i}/{len(target_keys)}] {date_folder}/{mile_bucket} "
              f"rows={len(all_data_rows):,}")
    df = read_file(BUCKET, key, ROWS_PER_FILE)
    if df is None or df.empty:
        read_fail += 1
        continue
    for record in df.to_dict("records"):
        flat = flatten_record(record)
        flat["_date_folder"] = date_folder
        flat["_mile_bucket"] = mile_bucket
        all_data_rows.append(flat)
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        pf  = pq.ParquetFile(BytesIO(raw))
        for col_name, dtype_str in flatten_arrow(pf.schema_arrow):
            col_registry[col_name]["dtypes"].append(dtype_str)
            col_registry[col_name]["date_seen"].add(date_folder)
    except Exception:
        pass
    read_ok += 1

print(f"\n  Read OK        : {read_ok}")
print(f"  Read failed    : {read_fail}")
print(f"  Rows collected : {len(all_data_rows):,}")
print(f"  Raw columns    : {len(col_registry):,}")

if not all_data_rows:
    raise RuntimeError("0 rows — run deep diagnostic on first key in target_keys")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — column scoring and filtering
#           16000 columns → top 200 reporting columns
# ══════════════════════════════════════════════════════════════════════════════
print(f"\nSTEP 3 — scoring {len(col_registry):,} columns → top {TOP_N_COLS}")
print("━"*60)

raw_df     = pd.DataFrame(all_data_rows)
meta_cols  = ["_date_folder","_mile_bucket"]
data_cols  = [c for c in raw_df.columns if c not in meta_cols]

# deduplicate rows
raw_df = raw_df.drop_duplicates(subset=data_cols)
print(f"  Rows after dedup : {len(raw_df):,}")
print(f"  Raw columns      : {len(raw_df.columns):,}")

# ── score every column ────────────────────────────────────────────────────────
col_scores = []
for col in data_cols:
    series      = raw_df[col]
    fill_pct    = round(series.notna().mean() * 100, 1)
    unique_vals = series.nunique()
    is_numeric  = pd.api.types.is_numeric_dtype(series)
    is_ts       = "timestamp" in str(series.dtype).lower()
    is_constant = unique_vals <= 1
    is_id_like  = (
        unique_vals == len(raw_df) and
        not is_numeric
    )   # every value unique → probably an ID field, less useful for agg

    # get real samples
    samples = (series.dropna().astype(str)
               .str.strip()
               .loc[lambda x: x != "nan"]
               .head(5).tolist())
    samples = (samples + ["","","","",""])[:5]

    # get dtype from registry (original nested name)
    orig_name = col   # safe name, find in registry
    dtype_str = "string"
    for orig, info in col_registry.items():
        if safe(orig) == col:
            dtype_str = resolve_type(info["dtypes"])
            break

    # ── scoring logic ─────────────────────────────────────────────────────
    score = 0
    score += fill_pct * 2          # fill rate is most important
    score += min(unique_vals, 100) # diversity of values
    if is_numeric: score += 30     # numeric cols are reportable
    if is_ts:      score += 50     # timestamps always useful
    if is_constant: score -= 200   # constant cols useless
    if is_id_like:  score -= 50    # pure IDs less useful for aggregation

    # ── category tag ──────────────────────────────────────────────────────
    if fill_pct == 0:
        category = "EMPTY"
    elif is_constant:
        category = "CONSTANT"
    elif is_ts:
        category = "TIMESTAMP"
    elif is_numeric and fill_pct >= MIN_FILL_PCT:
        category = "NUMERIC_GOOD"
    elif is_numeric:
        category = "NUMERIC_SPARSE"
    elif fill_pct >= MIN_FILL_PCT and unique_vals >= MIN_UNIQUE:
        category = "TEXT_GOOD"
    elif fill_pct >= MIN_FILL_PCT:
        category = "TEXT_SINGLE_VAL"
    else:
        category = "SPARSE"

    col_scores.append({
        "column_name"  : col,
        "data_type"    : dtype_str,
        "pandas_dtype" : str(series.dtype),
        "fill_pct"     : fill_pct,
        "unique_values": unique_vals,
        "is_numeric"   : is_numeric,
        "is_timestamp" : is_ts,
        "is_constant"  : is_constant,
        "category"     : category,
        "score"        : round(score, 1),
        "recommended"  : "YES" if (
            fill_pct >= MIN_FILL_PCT and
            unique_vals >= MIN_UNIQUE and
            not is_constant
        ) else "NO",
        "sample_1"     : samples[0],
        "sample_2"     : samples[1],
        "sample_3"     : samples[2],
        "sample_4"     : samples[3],
        "sample_5"     : samples[4],
    })

# sort by score descending
scores_df = (pd.DataFrame(col_scores)
             .sort_values("score", ascending=False)
             .reset_index(drop=True))

# summary of what we found
cat_summary = scores_df["category"].value_counts()
print(f"\n  Column categories:")
for cat, cnt in cat_summary.items():
    print(f"    {cat:25s} : {cnt:,}")

# ── top reporting columns ─────────────────────────────────────────────────────
top_cols_df = scores_df[scores_df["recommended"] == "YES"].head(TOP_N_COLS)
top_col_names = top_cols_df["column_name"].tolist()

print(f"\n  Total columns        : {len(scores_df):,}")
print(f"  Recommended cols     : {len(scores_df[scores_df['recommended']=='YES']):,}")
print(f"  Top {TOP_N_COLS} for reporting  : {len(top_cols_df):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — build reporting DataFrames from top columns only
# ══════════════════════════════════════════════════════════════════════════════
print(f"\nSTEP 4 — building reporting DataFrames")
print("━"*60)

# reporting data — only top columns
report_cols  = top_col_names + meta_cols
report_df    = raw_df[[c for c in report_cols if c in raw_df.columns]]
print(f"  Reporting data   : {len(report_df):,} rows x {len(report_df.columns)} cols")

# numeric stats for top numeric cols
numeric_top = [c for c in top_col_names
               if pd.api.types.is_numeric_dtype(raw_df[c])]
if numeric_top:
    stats_df = (raw_df[numeric_top]
                .describe(percentiles=[.25,.5,.75,.90,.95])
                .T.reset_index()
                .rename(columns={"index":"column"}))
    stats_df["fill_pct"] = [
        round(raw_df[c].notna().mean()*100,1)
        for c in numeric_top
    ]
    stats_df = stats_df.sort_values("fill_pct", ascending=False)
    print(f"  Numeric stats    : {len(stats_df):,} columns")
else:
    stats_df = pd.DataFrame()

# date x mile coverage
coverage_rows = []
for date in sorted(raw_df["_date_folder"].dropna().unique()):
    for mile in sorted(raw_df["_mile_bucket"].dropna().unique()):
        slot_df = raw_df[
            (raw_df["_date_folder"] == date) &
            (raw_df["_mile_bucket"] == mile)
        ]
        if len(slot_df) == 0:
            continue
        coverage_rows.append({
            "date_folder"     : date,
            "mile_bucket"     : mile,
            "row_count"       : len(slot_df),
            "cols_populated"  : int(slot_df[top_col_names].notna()
                                    .any(axis=0).sum()),
        })
coverage_df = pd.DataFrame(coverage_rows)
print(f"  Coverage slots   : {len(coverage_df):,}")

# value distribution for top text columns
text_top = [c for c in top_col_names[:20]
            if not pd.api.types.is_numeric_dtype(raw_df[c])]
dist_rows = []
for col in text_top:
    vc = raw_df[col].value_counts().head(10)
    for val, cnt in vc.items():
        dist_rows.append({
            "column" : col,
            "value"  : str(val)[:100],
            "count"  : cnt,
            "pct"    : round(cnt / len(raw_df) * 100, 1),
        })
dist_df = pd.DataFrame(dist_rows)
print(f"  Value dist rows  : {len(dist_df):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — write all outputs
# ══════════════════════════════════════════════════════════════════════════════
print(f"\nSTEP 5 — writing to S3")
print("━"*60)

tag = f"{TARGET_SCHEMA}_{DATE_FROM}_{DATE_TO}"

# 1 — ALL columns scored (all 16000 — for reference)
write_csv_s3(
    scores_df,
    f"01_all_columns_scored_{tag}.csv",
    "all 16k columns with scores"
)

# 2 — top reporting columns only
write_csv_s3(
    top_cols_df,
    f"02_top_reporting_columns_{tag}.csv",
    f"top {TOP_N_COLS} recommended columns"
)

# 3 — reporting data (top columns + data)
write_csv_s3(
    report_df,
    f"03_reporting_data_{tag}.csv",
    "reporting data (top cols only)"
)

# 4 — numeric stats
write_csv_s3(
    stats_df,
    f"04_numeric_stats_{tag}.csv",
    "numeric column statistics"
)

# 5 — date x mile coverage
write_csv_s3(
    coverage_df,
    f"05_coverage_{tag}.csv",
    "date x mile slot coverage"
)

# 6 — value distributions
write_csv_s3(
    dist_df,
    f"06_value_distributions_{tag}.csv",
    "top value distributions"
)

# 7 — empty / constant / useless columns (to exclude from QuickSight)
exclude_df = scores_df[scores_df["recommended"] == "NO"]
write_csv_s3(
    exclude_df,
    f"07_excluded_columns_{tag}.csv",
    "excluded columns with reason"
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — print client-ready summary
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'━'*60}")
print(f"CLIENT SUMMARY — {TARGET_SCHEMA}")
print(f"{'━'*60}")

print(f"""
  Data overview:
    Date range      : {DATE_FROM} → {DATE_TO}
    Files read      : {read_ok:,}
    Total rows      : {len(raw_df):,}
    Total columns   : {len(scores_df):,}

  Column breakdown:
    Empty (0% fill)       : {len(scores_df[scores_df['category']=='EMPTY']):,}
    Constant (1 value)    : {len(scores_df[scores_df['category']=='CONSTANT']):,}
    Sparse (<{MIN_FILL_PCT}% fill)    : {len(scores_df[scores_df['category']=='SPARSE']):,}
    Good numeric          : {len(scores_df[scores_df['category']=='NUMERIC_GOOD']):,}
    Good text             : {len(scores_df[scores_df['category']=='TEXT_GOOD']):,}
    Timestamp             : {len(scores_df[scores_df['category']=='TIMESTAMP']):,}

  Recommended for reporting : {len(top_cols_df):,} columns
""")

print(f"  TOP 20 COLUMNS FOR REPORTING:")
print(f"  {'column':<45} {'dtype':<15} {'fill%':>6}  {'unique':>7}  sample")
print(f"  {'─'*45} {'─'*15} {'─'*6}  {'─'*7}  {'─'*20}")
for _, row in top_cols_df.head(20).iterrows():
    print(f"  {row['column_name']:<45} "
          f"{row['data_type']:<15} "
          f"{row['fill_pct']:>5}%  "
          f"{row['unique_values']:>7,}  "
          f"{row['sample_1'][:30]}")

print(f"\n{'━'*60}")
print(f"  Output: s3://{BUCKET}/{OUTPUT_PREFIX}")
print(f"{'━'*60}")
