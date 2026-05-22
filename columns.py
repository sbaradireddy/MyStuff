import boto3
import pandas as pd
from io import BytesIO

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
BUCKET        = "pske-stg-maintenance"
CSV_KEY       = "projects/ard-iot-data-analysis/quicksight-dataset/ps.NA_PENSKE_TRIAL.measurements.results/unified_vehicle_data_20260520_191224.csv"
OUTPUT_PREFIX = "projects/ard-iot-data-analysis/quicksight-dataset/ps.NA_PENSKE_TRIAL.measurements.results/consolidated/"
s3            = boto3.client("s3")

# ══════════════════════════════════════════════════════════════════════════════
# HELPER
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
        print(f"  ✓ {desc:50s} ({size:,} bytes)")
        return f"s3://{BUCKET}/{key}"
    except Exception as e:
        print(f"  ✗ FAILED {filename}: {e}")
        return None

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — load CSV
# ══════════════════════════════════════════════════════════════════════════════
print("━"*60)
print("STEP 1 — loading CSV")
print("━"*60)

obj = s3.get_object(Bucket=BUCKET, Key=CSV_KEY)
df  = pd.read_csv(BytesIO(obj["Body"].read()))
df  = df.reset_index(drop=True)
print(f"  Rows    : {len(df):,}")
print(f"  Columns : {len(df.columns):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — auto detect all numbered column groups
#           finds anything like colname_0, colname_1 ... colname_N
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 2 — detecting numbered column groups")
print("━"*60)

import re
from collections import defaultdict

# group columns by base name
# e.g. resultdata_enginespeed_154 → base = resultdata_enginespeed
groups = defaultdict(list)

for col in df.columns:
    # match pattern: anything ending in _<number>
    match = re.match(r"^(.+?)_(\d+)$", col)
    if match:
        base   = match.group(1)   # resultdata_enginespeed
        number = int(match.group(2))
        groups[base].append((number, col))

# sort each group by number
for base in groups:
    groups[base] = sorted(groups[base], key=lambda x: x[0])

# show what was found
print(f"\n  Numbered column groups found: {len(groups)}")
print(f"\n  {'base_name':<55} {'col_count':>9}")
print(f"  {'─'*55} {'─'*9}")
for base, cols in sorted(groups.items(),
                          key=lambda x: len(x[1]),
                          reverse=True):
    print(f"  {base:<55} {len(cols):>9}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — identify non-numbered columns (keep as-is)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 3 — identifying static columns")
print("━"*60)

all_numbered_cols = set()
for base, col_list in groups.items():
    for _, col in col_list:
        all_numbered_cols.add(col)

static_cols = [c for c in df.columns if c not in all_numbered_cols]
print(f"  Static columns (kept as-is) : {len(static_cols)}")
print(f"  Numbered columns (to merge) : {len(all_numbered_cols)}")
for c in static_cols:
    print(f"    {c}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — consolidate
#           for each row: collect all non-null values from numbered cols
#           into one column as a list or take the first valid value
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 4 — consolidating numbered columns into single columns")
print("━"*60)

# start with static columns
consolidated = df[static_cols].copy().reset_index(drop=True)

for base, col_list in sorted(groups.items()):
    col_names = [col for _, col in col_list]

    # only keep cols that exist in df
    col_names = [c for c in col_names if c in df.columns]
    if not col_names:
        continue

    sub = df[col_names].copy().reset_index(drop=True)

    # ── strategy: for each row take all non-null values ───────────────────
    # if all values same → single value
    # if numeric → store as comma-separated list of unique values
    # result: one clean column per base name

    def consolidate_row(row):
        vals = row.dropna().tolist()
        if not vals:
            return None
        # remove duplicates while keeping order
        seen = []
        for v in vals:
            if v not in seen:
                seen.append(v)
        if len(seen) == 1:
            return seen[0]           # single value — clean
        return seen[0]               # take first valid value for reporting

    consolidated[base] = sub.apply(consolidate_row, axis=1)
    print(f"  ✓ {base:<55} ({len(col_names)} cols → 1)")

print(f"\n  Original columns : {len(df.columns):,}")
print(f"  Final columns    : {len(consolidated.columns):,}")
print(f"  Rows             : {len(consolidated):,}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — build column summary report
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 5 — building column summary")
print("━"*60)

consolidated = consolidated.reset_index(drop=True)
summary_rows = []

for col in consolidated.columns:
    series   = consolidated[col].reset_index(drop=True)
    fill_pct = round(series.notna().mean() * 100, 1)
    unique   = series.nunique()
    is_num   = pd.api.types.is_numeric_dtype(series)
    samples  = (series.dropna().astype(str)
                .str.strip()
                .loc[lambda x: (x != "nan") & (x != "")]
                .head(3).tolist())
    samples  = (samples + ["","",""])[:3]

    # was this col originally numbered
    originally_numbered = col in groups
    original_col_count  = len(groups[col]) if originally_numbered else 0

    summary_rows.append({
        "column_name"      : col,
        "fill_pct"         : fill_pct,
        "unique_values"    : unique,
        "is_numeric"       : is_num,
        "was_numbered"     : originally_numbered,
        "original_col_count": original_col_count,
        "sample_1"         : samples[0],
        "sample_2"         : samples[1],
        "sample_3"         : samples[2],
    })

summary_df = (pd.DataFrame(summary_rows)
              .sort_values("fill_pct", ascending=False)
              .reset_index(drop=True))

print(f"  Summary rows : {len(summary_df):,}")

# numeric stats on consolidated data
num_cols = summary_df[
    (summary_df["is_numeric"] == True) &
    (summary_df["fill_pct"]   >= 10)
]["column_name"].tolist()

if num_cols:
    num_base  = consolidated[num_cols].copy().reset_index(drop=True)
    num_stats = (num_base
                 .describe(percentiles=[.25,.5,.75,.90,.95])
                 .T
                 .reset_index()
                 .rename(columns={"index":"column"})
                 .reset_index(drop=True))
    fill_list = [
        round(num_base[c].notna().mean()*100,1)
        for c in num_stats["column"].tolist()
    ]
    num_stats["fill_pct"] = fill_list
    num_stats = num_stats.sort_values(
        "fill_pct", ascending=False
    ).reset_index(drop=True)
else:
    num_stats = pd.DataFrame()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — write outputs
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 6 — writing to S3")
print("━"*60)

write_csv_s3(
    consolidated,
    "01_consolidated_data.csv",
    "all data — numbered cols merged into single cols"
)
write_csv_s3(
    summary_df,
    "02_column_summary.csv",
    "every column — fill%, unique, samples"
)
write_csv_s3(
    num_stats,
    "03_numeric_stats.csv",
    "numeric column statistics"
)

# group mapping — which original cols map to which base
mapping_rows = []
for base, col_list in sorted(groups.items()):
    for num, col in col_list:
        mapping_rows.append({
            "consolidated_column" : base,
            "original_column"     : col,
            "number_index"        : num,
        })
mapping_df = pd.DataFrame(mapping_rows)
write_csv_s3(
    mapping_df,
    "04_column_mapping.csv",
    "original numbered cols → consolidated col name"
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — print summary
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'━'*60}")
print("SUMMARY")
print(f"{'━'*60}")
print(f"""
  Original columns    : {len(df.columns):,}
  Consolidated columns: {len(consolidated.columns):,}
  Columns reduced by  : {len(df.columns) - len(consolidated.columns):,}
  Rows                : {len(consolidated):,}

  Top 20 consolidated columns:
  {'column':<50} {'fill%':>6}  {'unique':>7}  {'orig_cols':>9}  sample""")
print(f"  {'─'*50} {'─'*6}  {'─'*7}  {'─'*9}  {'─'*20}")

for _, row in summary_df.head(20).iterrows():
    print(f"  {row['column_name']:<50} "
          f"{row['fill_pct']:>5}%  "
          f"{row['unique_values']:>7,}  "
          f"{row['original_col_count']:>9}  "
          f"{str(row['sample_1'])[:25]}")

print(f"\n  Output: s3://{BUCKET}/{OUTPUT_PREFIX}")
print(f"{'━'*60}")
