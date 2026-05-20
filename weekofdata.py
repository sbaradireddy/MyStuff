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
OUTPUT_PREFIX    = "projects/ard-iot-data/reporting/csv/"
DATE_FROM        = "20260513"    # YYYYMMDD — start of week
DATE_TO          = "20260519"    # YYYYMMDD — end of week
# ══════════════════════════════════════════════════════════════════════════════

ROWS_PER_FILE    = 50
FILES_PER_SCHEMA = 3
RUN_TS           = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
s3               = boto3.client("s3")

# ──────────────────────────────────────────────────────────────────────────────
# WRITE FUNCTION
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
        print(f"  ✓ {desc:30s} → s3://{BUCKET}/{key}  ({size:,} bytes)")
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
            flat[f"{key}_arr"] = str(v)[:200]
        else:
            flat[key] = v
    return flat

def read_parquet_safe(bucket, key, nrows=50):
    try:
        raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if len(raw) < 100:
            return None
        pf  = pq.ParquetFile(BytesIO(raw))
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
# STEP 1 — S3 listing with date filter
# ══════════════════════════════════════════════════════════════════════════════
print("━"*55)
print(f"STEP 1 — S3 listing  [{DATE_FROM} → {DATE_TO}]")
print("━"*55)

schema_files  = defaultdict(list)
total         = 0
skipped_date  = 0
paginator     = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=BUCKET, Prefix=SOURCE_PREFIX):
    for obj in page.get("Contents", []):
        key  = obj["Key"]
        size = obj.get("Size", 0)

        if not key.endswith(".parquet") or size < 100:
            continue

        # date filter — folder name is YYYYMMDD
        try:
            date_folder = key.split("/")[-3]
            if not date_folder.isdigit():
                continue
            if not (DATE_FROM <= date_folder <= DATE_TO):
                skipped_date += 1
                continue
        except IndexError:
            continue

        total += 1
        sname  = key.split("/")[-1].split("-")[0]
        if len(schema_files[sname]) < FILES_PER_SCHEMA:
            schema_files[sname].append(key)

print(f"  Date range        : {DATE_FROM} → {DATE_TO}")
print(f"  Files in range    : {total:,}")
print(f"  Files out of range: {skipped_date:,}")
print(f"  SchemaTypes found : {sorted(schema_files.keys())}")
print(f"  Files to process  : {sum(len(v) for v in schema_files.values())}")

if not schema_files:
    raise RuntimeError(
        f"No files found between {DATE_FROM} and {DATE_TO}. "
        f"Check dates match folder names in S3."
    )

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — read sample data
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*55)
print("STEP 2 — reading sample data")
print("━"*55)

col_registry  = defaultdict(lambda: {"dtypes":[], "schemas":set()})
all_data_rows = []

for sname, keys in sorted(schema_files.items()):
    print(f"  {sname} ...")
    for key in keys:
        raw_df = read_parquet_safe(BUCKET, key, ROWS_PER_FILE)
        if raw_df is not None:
            for record in raw_df.to_dict("records"):
                flat = flatten_record(record)
                flat["_schema_type"] = sname
                flat["_date_folder"] = key.split("/")[-3]
                flat["_mile_bucket"] = key.split("/")[-2]
                all_data_rows.append(flat)
        try:
            raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            pf  = pq.ParquetFile(BytesIO(raw))
            for col_name, dtype_str in flatten_arrow(pf.schema_arrow):
                col_registry[col_name]["dtypes"].append(dtype_str)
                col_registry[col_name]["schemas"].add(sname)
        except Exception:
            pass
    print(f"    rows collected so far: {len(all_data_rows):,}")

if not all_data_rows:
    raise RuntimeError("0 rows collected — check dates and SOURCE_PREFIX")

print(f"\n  Total rows    : {len(all_data_rows):,}")
print(f"  Total columns : {len(col_registry):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — build DataFrames
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*55)
print("STEP 3 — building DataFrames")
print("━"*55)

unified_df = pd.DataFrame(all_data_rows)
meta_cols  = ["_schema_type","_date_folder","_mile_bucket"]
data_cols  = [c for c in unified_df.columns if c not in meta_cols]
unified_df = unified_df.drop_duplicates(subset=data_cols)
print(f"  unified_df  : {len(unified_df):,} rows x {len(unified_df.columns)} cols")

# schema map
schema_map_rows = []
for col_name, info in col_registry.items():
    s       = safe(col_name)
    unified = resolve_type(info["dtypes"])
    present = sorted(info["schemas"])
    samples = (
        unified_df[s].dropna().astype(str).head(3).tolist()
        if s in unified_df.columns else []
    )
    samples = (samples + ["","",""])[:3]
    schema_map_rows.append({
        "original_column" : col_name,
        "safe_column"     : s,
        "unified_dtype"   : unified,
        "raw_dtypes"      : " | ".join(sorted(set(info["dtypes"]))),
        "present_in"      : ", ".join(present),
        "schema_count"    : len(present),
        "common_to_all"   : str(len(present) == len(schema_files)),
        "sample_1"        : samples[0],
        "sample_2"        : samples[1],
        "sample_3"        : samples[2],
    })
schema_map_df = (pd.DataFrame(schema_map_rows)
                 .sort_values(["schema_count","original_column"],
                               ascending=[False,True]))
print(f"  schema_map  : {len(schema_map_df):,} rows")

summary_df = (unified_df
    .groupby("_schema_type", as_index=False)
    .agg(
        row_count    =("_schema_type","count"),
        date_count   =("_date_folder","nunique"),
        earliest_date=("_date_folder","min"),
        latest_date  =("_date_folder","max"),
        mile_buckets =("_mile_bucket","nunique"),
    )
    .sort_values("_schema_type"))
print(f"  summary_df  : {len(summary_df):,} rows")

fill_df = pd.DataFrame({
    "column_name"   : data_cols,
    "fill_pct"      : [round(unified_df[c].notna().mean()*100,1) for c in data_cols],
    "unique_values" : [unified_df[c].nunique() for c in data_cols],
    "example_value" : [
        str(unified_df[c].dropna().iloc[0])
        if unified_df[c].notna().any() else ""
        for c in data_cols
    ],
}).sort_values("fill_pct", ascending=False)
print(f"  fill_df     : {len(fill_df):,} rows")

all_schemas = sorted(schema_files.keys())
pivot_rows  = []
for col_name, info in sorted(col_registry.items()):
    row = {
        "column" : safe(col_name),
        "dtype"  : resolve_type(info["dtypes"]),
    }
    for sname in all_schemas:
        row[sname] = "YES" if sname in info["schemas"] else ""
    pivot_rows.append(row)
pivot_df = pd.DataFrame(pivot_rows)
print(f"  pivot_df    : {len(pivot_df):,} rows")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — write all CSVs to S3
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*55)
print("STEP 4 — writing CSVs to S3")
print("━"*55)

write_csv_s3(schema_map_df, f"schema_map_{DATE_FROM}_{DATE_TO}.csv",    "schema map")
write_csv_s3(unified_df,    f"unified_data_{DATE_FROM}_{DATE_TO}.csv",  "unified data")
write_csv_s3(summary_df,    f"summary_{DATE_FROM}_{DATE_TO}.csv",       "summary")
write_csv_s3(fill_df,       f"fill_rates_{DATE_FROM}_{DATE_TO}.csv",    "fill rates")
write_csv_s3(pivot_df,      f"schema_pivot_{DATE_FROM}_{DATE_TO}.csv",  "schema pivot")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — confirm files in S3
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*55)
print("STEP 5 — confirming files in S3")
print("━"*55)

found = []
for page in s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET, Prefix=OUTPUT_PREFIX
):
    for obj in page.get("Contents", []):
        name = obj["Key"].split("/")[-1]
        if DATE_FROM in name and DATE_TO in name:
            found.append({
                "file"    : name,
                "size_kb" : round(obj["Size"]/1024, 1),
            })

if found:
    print(f"  {len(found)} files written for {DATE_FROM} → {DATE_TO}:")
    for f in found:
        print(f"    ✓ {f['file']}  ({f['size_kb']} KB)")
else:
    print(f"  ✗ No files found — check S3 console at:")
    print(f"    s3://{BUCKET}/{OUTPUT_PREFIX}")

print(f"""
{"━"*55}
  DONE
  Date range : {DATE_FROM} → {DATE_TO}
  Output     : s3://{BUCKET}/{OUTPUT_PREFIX}
{"━"*55}
""")
