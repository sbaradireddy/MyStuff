import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from io import BytesIO
from collections import defaultdict
from datetime import datetime

# ══════════════════════════════════════════════════════════════════════════════
# ⚙️  CHANGE ONLY THESE LINES
# ══════════════════════════════════════════════════════════════════════════════
BUCKET           = "pske-stg-maintenance"
SOURCE_PREFIX    = "projects/ard-iot-data/ps.NA_PENSKE_TRIAL.measurements.results/"
OUTPUT_PREFIX    = "projects/ard-iot-data/reporting/schematype1/"
TARGET_SCHEMA    = "SchemaType1"   # ← change to SchemaType2, SchemaType3 etc
DATE_FROM        = "20260513"      # ← week start YYYYMMDD
DATE_TO          = "20260519"      # ← week end   YYYYMMDD
FILES_PER_FOLDER = 1               # 1 file per date/mile folder (avoids duplicates)
ROWS_PER_FILE    = 100             # sample rows per file
# ══════════════════════════════════════════════════════════════════════════════

RUN_TS = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
s3     = boto3.client("s3")

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
        s3.put_object(
            Bucket      = BUCKET,
            Key         = key,
            Body        = buf.read(),
            ContentType = "text/csv; charset=utf-8",
        )
        size = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        print(f"  ✓ {desc:35s} → s3://{BUCKET}/{key}  ({size:,} bytes)")
        return f"s3://{BUCKET}/{key}"
    except Exception as e:
        print(f"  ✗ FAILED {filename}: {e}")
        return None

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────
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
            flat[f"{key}_arr"] = str(v)[:300]
        else:
            flat[key] = v
    return flat

def read_parquet_safe(bucket, key, nrows=100):
    try:
        raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if len(raw) < 100:
            return None
        pf = pq.ParquetFile(BytesIO(raw))
        if pf.metadata.num_rows == 0:
            return None
        batch = next(pf.iter_batches(batch_size=nrows))
        df    = batch.to_pandas()
        return None if df.empty else df
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
    if len(unique) == 1:
        return unique[0]
    return sorted(unique,
                  key=lambda t: TYPE_RANK.get(t.lower().strip(), 99),
                  reverse=True)[0]

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — S3 listing
#           one file per unique (date_folder / mile_bucket) combination
#           prevents reading the same logical slot multiple times
# ══════════════════════════════════════════════════════════════════════════════
print("━"*60)
print(f"STEP 1 — S3 listing  [{TARGET_SCHEMA}]  [{DATE_FROM} → {DATE_TO}]")
print("━"*60)

# key = (date_folder, mile_bucket) → ensures one file per unique slot
seen_slots   = set()
target_keys  = []           # final list of keys to read
total_seen   = 0
skipped_date = 0
skipped_dupe = 0
paginator    = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=BUCKET, Prefix=SOURCE_PREFIX):
    for obj in page.get("Contents", []):
        key  = obj["Key"]
        size = obj.get("Size", 0)

        if not key.endswith(".parquet") or size < 100:
            continue

        total_seen += 1

        # ── filter 1: date range ──────────────────────────────────────────
        try:
            parts       = key.split("/")
            date_folder = parts[-3]     # e.g. 20260513
            mile_bucket = parts[-2]     # e.g. 010000
            fname       = parts[-1]     # e.g. SchemaType1-177919...parquet
            if not date_folder.isdigit():
                continue
            if not (DATE_FROM <= date_folder <= DATE_TO):
                skipped_date += 1
                continue
        except IndexError:
            continue

        # ── filter 2: target schema type only ────────────────────────────
        schema_name = fname.split("-")[0]
        if schema_name != TARGET_SCHEMA:
            continue

        # ── filter 3: one file per (date, mile) slot — no duplicates ─────
        slot = (date_folder, mile_bucket)
        if slot in seen_slots:
            skipped_dupe += 1
            continue
        seen_slots.add(slot)
        target_keys.append({
            "key":         key,
            "date_folder": date_folder,
            "mile_bucket": mile_bucket,
        })

print(f"  Total files scanned     : {total_seen:,}")
print(f"  Skipped (outside dates) : {skipped_date:,}")
print(f"  Skipped (duplicate slot): {skipped_dupe:,}")
print(f"  Files to process        : {len(target_keys):,}")
print(f"  Unique date folders     : {len(set(k['date_folder'] for k in target_keys))}")
print(f"  Unique mile buckets     : {len(set(k['mile_bucket'] for k in target_keys))}")

if not target_keys:
    raise RuntimeError(
        f"No {TARGET_SCHEMA} files found between {DATE_FROM} and {DATE_TO}.\n"
        f"Check TARGET_SCHEMA name matches exactly what is in your S3 filenames."
    )

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — read one file per slot via PyArrow
#           collects schema + sample data in one pass
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print(f"STEP 2 — reading {len(target_keys)} files (PyArrow, driver only)")
print("━"*60)

col_registry  = defaultdict(lambda: {
    "dtypes":    [],
    "samples":   [],
    "date_seen": set(),
    "mile_seen": set(),
})
all_data_rows = []

for i, item in enumerate(target_keys, 1):
    key         = item["key"]
    date_folder = item["date_folder"]
    mile_bucket = item["mile_bucket"]

    if i % 20 == 0 or i == 1:
        print(f"  [{i}/{len(target_keys)}] {date_folder}/{mile_bucket} ...")

    raw_df = read_parquet_safe(BUCKET, key, ROWS_PER_FILE)

    if raw_df is not None:
        for record in raw_df.to_dict("records"):
            flat = flatten_record(record)
            flat["_date_folder"] = date_folder
            flat["_mile_bucket"] = mile_bucket
            all_data_rows.append(flat)

    # schema discovery
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        pf  = pq.ParquetFile(BytesIO(raw))
        for col_name, dtype_str in flatten_arrow(pf.schema_arrow):
            col_registry[col_name]["dtypes"].append(dtype_str)
            col_registry[col_name]["date_seen"].add(date_folder)
            col_registry[col_name]["mile_seen"].add(mile_bucket)
    except Exception:
        pass

print(f"\n  Rows collected  : {len(all_data_rows):,}")
print(f"  Columns found   : {len(col_registry):,}")

if not all_data_rows:
    raise RuntimeError("0 rows collected — check SOURCE_PREFIX and file format")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — build unified single schema + DataFrames
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 3 — building unified schema")
print("━"*60)

# unified data — all rows, all columns
unified_df = pd.DataFrame(all_data_rows)
meta_cols  = ["_date_folder","_mile_bucket"]
data_cols  = [c for c in unified_df.columns if c not in meta_cols]
unified_df = unified_df.drop_duplicates(subset=data_cols)
print(f"  Rows after dedup  : {len(unified_df):,}")
print(f"  Total columns     : {len(unified_df.columns):,}")

# ── OUTPUT A: unified schema map ─────────────────────────────────────────────
# one row per column — name, dtype, sample values, coverage
schema_rows = []
for col_name, info in col_registry.items():
    s       = safe(col_name)
    unified = resolve_type(info["dtypes"])

    # pull real sample values from actual data
    if s in unified_df.columns:
        samples = (unified_df[s]
                   .dropna()
                   .astype(str)
                   .str.strip()
                   .loc[lambda x: x != ""]
                   .head(5)
                   .tolist())
    else:
        samples = []
    samples = (samples + ["","","","",""])[:5]

    schema_rows.append({
        "column_original" : col_name,
        "column_safe"     : s,
        "unified_dtype"   : unified,
        "raw_dtypes"      : " | ".join(sorted(set(info["dtypes"]))),
        "date_folders"    : len(info["date_seen"]),
        "mile_buckets"    : len(info["mile_seen"]),
        "fill_pct"        : round(
            unified_df[s].notna().mean() * 100, 1
        ) if s in unified_df.columns else 0.0,
        "unique_values"   : unified_df[s].nunique()
                            if s in unified_df.columns else 0,
        "sample_1"        : samples[0],
        "sample_2"        : samples[1],
        "sample_3"        : samples[2],
        "sample_4"        : samples[3],
        "sample_5"        : samples[4],
    })

schema_map_df = (pd.DataFrame(schema_rows)
                 .sort_values(["fill_pct","column_original"],
                               ascending=[False, True]))
print(f"  schema_map rows   : {len(schema_map_df):,}")

# ── OUTPUT B: column analysis — best columns for reporting ───────────────────
# recommended = fill > 50% and more than 1 unique value
analysis_df = schema_map_df.copy()
analysis_df["recommended"] = (
    (analysis_df["fill_pct"] >= 50) &
    (analysis_df["unique_values"] > 1)
).map({True: "YES", False: "NO"})
analysis_df["reporting_note"] = analysis_df.apply(
    lambda r: (
        "good — high fill, many values"  if r["fill_pct"] >= 80 and r["unique_values"] > 5
        else "ok — moderate fill"        if r["fill_pct"] >= 50
        else "low fill — sparse data"    if r["fill_pct"] > 0
        else "empty — skip"
    ), axis=1
)

# ── OUTPUT C: date × mile coverage pivot ─────────────────────────────────────
# shows which (date, mile) slots have data — good for spotting gaps
coverage_rows = []
for item in target_keys:
    slot_df = unified_df[
        (unified_df["_date_folder"] == item["date_folder"]) &
        (unified_df["_mile_bucket"] == item["mile_bucket"])
    ]
    coverage_rows.append({
        "date_folder"  : item["date_folder"],
        "mile_bucket"  : item["mile_bucket"],
        "row_count"    : len(slot_df),
        "col_populated": int((slot_df[data_cols].notna().sum(axis=1) > 0).sum()),
    })
coverage_df = (pd.DataFrame(coverage_rows)
               .sort_values(["date_folder","mile_bucket"]))

# ── OUTPUT D: summary stats per column ───────────────────────────────────────
# min / max / mean for numeric columns
numeric_cols = [
    c for c in data_cols
    if pd.api.types.is_numeric_dtype(unified_df[c])
]
if numeric_cols:
    stats_df = (unified_df[numeric_cols]
                .describe()
                .T
                .reset_index()
                .rename(columns={"index":"column_name"}))
    stats_df["fill_pct"] = [
        round(unified_df[c].notna().mean()*100, 1)
        for c in numeric_cols
    ]
    stats_df = stats_df.sort_values("fill_pct", ascending=False)
else:
    stats_df = pd.DataFrame(
        columns=["column_name","count","mean","std","min","25%","50%","75%","max","fill_pct"]
    )

print(f"  analysis rows     : {len(analysis_df):,}")
print(f"  coverage rows     : {len(coverage_df):,}")
print(f"  numeric stats rows: {len(stats_df):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — write all outputs to S3
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 4 — writing CSVs to S3")
print("━"*60)

tag = f"{TARGET_SCHEMA}_{DATE_FROM}_{DATE_TO}"

write_csv_s3(schema_map_df,
    f"01_schema_map_{tag}.csv",
    "schema map (all columns + dtypes + samples)")

write_csv_s3(analysis_df,
    f"02_column_analysis_{tag}.csv",
    "column analysis (fill rate + recommended)")

write_csv_s3(unified_df,
    f"03_unified_data_{tag}.csv",
    "unified data (all rows merged)")

write_csv_s3(coverage_df,
    f"04_date_mile_coverage_{tag}.csv",
    "date x mile slot coverage")

write_csv_s3(stats_df,
    f"05_numeric_stats_{tag}.csv",
    "numeric column statistics")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — confirm + print summary
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 5 — confirming S3 output")
print("━"*60)

found = []
for page in s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET, Prefix=OUTPUT_PREFIX
):
    for obj in page.get("Contents", []):
        name = obj["Key"].split("/")[-1]
        if tag in name:
            found.append(f"  ✓ {name}  ({round(obj['Size']/1024,1)} KB)")

for f in found:
    print(f)

# print top recommended columns for client
print("\n" + "━"*60)
print(f"TOP RECOMMENDED COLUMNS FOR REPORTING ({TARGET_SCHEMA})")
print("━"*60)
top = (analysis_df[analysis_df["recommended"]=="YES"]
       .head(20)[["column_safe","unified_dtype","fill_pct",
                   "unique_values","reporting_note","sample_1","sample_2"]])
print(top.to_string(index=False))

print(f"""
{"━"*60}
  DONE  {RUN_TS}
  Schema        : {TARGET_SCHEMA}
  Date range    : {DATE_FROM} → {DATE_TO}
  Files read    : {len(target_keys):,}
  Unique slots  : {len(seen_slots):,}
  Columns found : {len(col_registry):,}
  Data rows     : {len(unified_df):,}
  Output        : s3://{BUCKET}/{OUTPUT_PREFIX}
{"━"*60}
""")
