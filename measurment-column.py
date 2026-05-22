from functools import reduce

# Auto-detect the measurement column instead of hardcoding
meas_candidates = [c for c in df.columns if "measurementconfig" in c.lower()
                   and "name" in c.lower()]
assert meas_candidates, "No measurementconfigurationname column found! Check df.columns"
MEAS_COL = meas_candidates[0]
print("Using measurement column:", MEAS_COL)

# Auto-detect signal columns: try resultdata_, fall back to any numbered col
signal_cols_all = [c for c in df.columns if c.startswith("resultdata_")]
if not signal_cols_all:
    signal_cols_all = [c for c in df.columns if re.search(r"_\d+$", c)
                       and not c.startswith("metadata_")]
print("Signal columns detected:", len(signal_cols_all))
assert signal_cols_all, "No sensor/signal columns found! Inspect df.columns"

groups = [r[0] for r in df.select(MEAS_COL).distinct().collect() if r[0] is not None]
assert groups, "No measurement groups found!"

long_parts = []
for g in groups:
    gdf = df.filter(F.col(MEAS_COL) == g)
    signal_cols = [c for c in signal_cols_all if c in gdf.columns]
    if not signal_cols:
        print(f"  [skip] {g}: no signal columns"); continue

    # null-count check, guarded
    counts = gdf.select([F.count(F.col(c)).alias(c) for c in signal_cols]).first().asDict()
    non_null = [c for c, n in counts.items() if n and n > 0]
    if not non_null:
        print(f"  [skip] {g}: all signal columns null"); continue

    casted = [F.col(c).cast("string").alias(c) for c in non_null]
    keep   = [c for c in (MEAS_COL,) if c in gdf.columns]  # add more metadata cols here
    gdf2   = gdf.select(*keep, *casted)

    pairs = ",".join([f"'{c}', `{c}`" for c in non_null])
    expr  = f"stack({len(non_null)}, {pairs}) as (signal_col, signal_value)"
    part  = (gdf2.select(*keep, F.expr(expr))
                 .filter(F.col("signal_value").isNotNull())
                 .withColumn("signal_name",
                             F.regexp_replace(F.regexp_replace(F.col("signal_col"),
                                              r"^resultdata_", ""), r"_\d+$", ""))
                 .withColumnRenamed(MEAS_COL, "measurement_name"))
    long_parts.append(part)
    print(f"  [ok] {g}: {len(non_null)} signals unpivoted")

assert long_parts, "long_parts is EMPTY — every group was skipped. See messages above."
long_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), long_parts)
