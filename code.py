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
BUCKET           = "pske-stg-maintenance"
SOURCE_PREFIX    = "projects/ard-iot-data/ps.NA_PENSKE_TRIAL.measurements.results/"
OUTPUT_PREFIX    = "projects/ard-iot-data/reporting/csv/"
ROWS_PER_FILE    = 50       # sample rows per file — enough for analysis
FILES_PER_SCHEMA = 3        # files per SchemaType
RUN_TS           = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
s3               = boto3.client("s3")

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
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
    """Flatten one dict row — handles nested struct/list/map."""
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

def write_csv_s3(df, filename):
    """Write pandas DataFrame as CSV directly to S3."""
    buf = df.to_csv(index=False)
    key = f"{OUTPUT_PREFIX}{filename}"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buf.encode("utf-8"),
        ContentType="text/csv"
    )
    path = f"s3://{BUCKET}/{key}"
    print(f"  ✓ → {path}  ({len(df):,} rows)")
    return path

def read_parquet_safe(bucket, key, nrows=50):
    """Read first nrows from parquet. Returns pandas df or None."""
    try:
        raw  = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if len(raw) < 100:
            return None
        pf   = pq.ParquetFile(BytesIO(raw))
        if pf.metadata.num_rows == 0:
            return None
        batch = next(pf.iter_batches(batch_size=nrows))
        df    = batch.to_pandas()
        if df.empty:
            return None
        return df
    except Exception as e:
        print(f"    SKIP {key.split('/')[-1]}: {e}")
        return None

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — S3 listing (zero data read)
# ══════════════════════════════════════════════════════════════════════════════
print("━"*55)
print("STEP 1 — S3 listing")
print("━"*55)

schema_files = defaultdict(list)
total        = 0
paginator    = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=BUCKET, Prefix=SOURCE_PREFIX):
    for obj in page.get("Contents", []):
        key  = obj["Key"]
        size = obj.get("Size", 0)
        if not key.endswith(".parquet") or size < 100:
            continue
        total += 1
        sname  = key.split("/")[-1].split("-")[0]
        if len(schema_files[sname]) < FILES_PER_SCHEMA:
            schema_files[sname].append(key)

print(f"  Total valid files : {total:,}")
print(f"  SchemaTypes       : {sorted(schema_files.keys())}")
print(f"  Files to read     : {sum(len(v) for v in schema_files.values())}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — read sample rows + build schema (PyArrow → Pandas, driver only)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*55)
print("STEP 2 — reading sample data (PyArrow, no Spark)")
print("━"*55)

# results collected here
schema_map_rows = []    # schema map report
all_data_rows   = []    # actual data rows for analysis
col_registry    = defaultdict(lambda: {
    "dtypes": [], "schemas": set(), "samples": []
})

for sname, keys in sorted(schema_files.items()):
    print(f"  {sname} ...")
    schema_data_rows = []

    for key in keys:
        raw_df = read_parquet_safe(BUCKET, key, ROWS_PER_FILE)
        if raw_df is None:
            continue

        # flatten each row
        for record in raw_df.to_dict("records"):
            flat = flatten_record(record)
            flat["_schema_type"] = sname
            flat["_date_folder"] = key.split("/")[-3]
            flat["_mile_bucket"] = key.split("/")[-2]
            schema_data_rows.append(flat)

        # collect schema info via PyArrow
        try:
            raw  = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            pf   = pq.ParquetFile(BytesIO(raw))
            for col_name, dtype_str in flatten_arrow(pf.schema_arrow):
                col_registry[col_name]["dtypes"].append(dtype_str)
                col_registry[col_name]["schemas"].add(sname)

        except Exception:
            pass

    print(f"    rows collected: {len(schema_data_rows):,}")
    all_data_rows.extend(schema_data_rows)

if not all_data_rows:
    raise RuntimeError(
        "0 rows collected. Check SOURCE_PREFIX and file sizes."
    )

print(f"\n  Total rows collected : {len(all_data_rows):,}")
print(f"  Unique columns found : {len(col_registry):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — build unified schema map (pure Python, instant)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*55)
print("STEP 3 — unified schema map")
print("━"*55)

# build unified flat DataFrame first
unified_df = pd.DataFrame(all_data_rows)

# fill missing columns with None
for col_name in col_registry:
    s = safe(col_name)
    if s not in unified_df.columns:
        unified_df[s] = None

# deduplicate
meta_cols  = ["_schema_type","_date_folder","_mile_bucket"]
data_cols  = [c for c in unified_df.columns if c not in meta_cols]
unified_df = unified_df.drop_duplicates(subset=data_cols)

print(f"  Rows after dedup : {len(unified_df):,}")
print(f"  Total columns    : {len(unified_df.columns):,}")

# ── OUTPUT 1: schema map ──────────────────────────────────────────────────────
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

for col_name, info in col_registry.items():
    s       = safe(col_name)
    unified = resolve_type(info["dtypes"])
    present = sorted(info["schemas"])
    # pull up to 3 real sample values from the data
    if s in unified_df.columns:
        samples = (unified_df[s]
                   .dropna()
                   .astype(str)
                   .head(3)
                   .tolist())
    else:
        samples = []
    samples = (samples + ["","",""])[:3]

    schema_map_rows.append({
        "original_column":  col_name,
        "safe_column":      s,
        "unified_dtype":    unified,
        "raw_dtypes":       " | ".join(sorted(set(info["dtypes"]))),
        "present_in":       ", ".join(present),
        "schema_count":     len(present),
        "common_to_all":    len(present) == len(schema_files),
        "sample_1":         samples[0],
        "sample_2":         samples[1],
        "sample_3":         samples[2],
    })

schema_map_df = (pd.DataFrame(schema_map_rows)
                 .sort_values(["schema_count","original_column"],
                               ascending=[False, True]))

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — write all CSVs to S3 (pandas → S3, no Spark needed)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*55)
print("STEP 4 — writing CSVs to S3")
print("━"*55)

# 1 — schema map
write_csv_s3(schema_map_df,
             f"schema_map_{RUN_TS}.csv")

# 2 — unified data (all schemas merged)
write_csv_s3(unified_df,
             f"unified_vehicle_data_{RUN_TS}.csv")

# 3 — schema summary (row count per SchemaType)
summary = (unified_df
           .groupby("_schema_type")
           .agg(
               row_count    =("_schema_type","count"),
               date_count   =("_date_folder","nunique"),
               earliest_date=("_date_folder","min"),
               latest_date  =("_date_folder","max"),
               mile_buckets =("_mile_bucket","nunique"),
           )
           .reset_index()
           .sort_values("_schema_type"))
write_csv_s3(summary,
             f"schema_summary_{RUN_TS}.csv")

# 4 — column fill rate (% populated per column)
fill = pd.DataFrame({
    "column_name": data_cols,
    "fill_pct":    [
        round(unified_df[c].notna().mean() * 100, 1)
        for c in data_cols
    ],
    "unique_values": [
        unified_df[c].nunique() for c in data_cols
    ],
    "example_value": [
        str(unified_df[c].dropna().iloc[0])
        if unified_df[c].notna().any() else ""
        for c in data_cols
    ],
}).sort_values("fill_pct", ascending=False)
write_csv_s3(fill,
             f"column_fill_rates_{RUN_TS}.csv")

# 5 — per-schema column pivot (which columns exist in which schema)
pivot_rows = []
all_schemas = sorted(schema_files.keys())
for col_name, info in sorted(col_registry.items()):
    row = {"column": safe(col_name)}
    for sname in all_schemas:
        row[sname] = "✓" if sname in info["schemas"] else ""
    pivot_rows.append(row)
pivot_df = pd.DataFrame(pivot_rows)
write_csv_s3(pivot_df,
             f"column_schema_pivot_{RUN_TS}.csv")

# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
print(f"""
{"━"*55}
  DONE  ({RUN_TS})
  Files processed  : {sum(len(v) for v in schema_files.values())}
  SchemaTypes      : {len(schema_files)}
  Unique columns   : {len(col_registry):,}
  Sample rows      : {len(unified_df):,}

  S3 outputs (open these for your client):
  📄 schema_map_{RUN_TS}.csv
  📄 unified_vehicle_data_{RUN_TS}.csv
  📄 schema_summary_{RUN_TS}.csv
  📄 column_fill_rates_{RUN_TS}.csv
  📄 column_schema_pivot_{RUN_TS}.csv

  All at: s3://{BUCKET}/{OUTPUT_PREFIX}
{"━"*55}
""")
