# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — numeric stats (fixed)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "━"*60)
print("STEP 5 — numeric stats")
print("━"*60)

# reset sub_df index before anything
sub_df = sub_df.reset_index(drop=True)

numeric_cols = sensor_cols_df[
    (sensor_cols_df["is_numeric"] == True) &
    (sensor_cols_df["fill_pct"]   >= 10)
]["column_name"].tolist()

# keep only cols that actually exist in sub_df
numeric_cols = [c for c in numeric_cols if c in sub_df.columns]
print(f"  Numeric columns : {len(numeric_cols)}")

if numeric_cols:
    # reset again right before describe — safest approach
    num_base  = sub_df[numeric_cols].copy().reset_index(drop=True)
    num_stats = (num_base
                 .describe(percentiles=[.25,.5,.75,.90,.95])
                 .T
                 .reset_index()
                 .rename(columns={"index":"column"}))

    # build fill_pct as a plain list aligned to num_stats rows
    fill_list = []
    for col in num_stats["column"].tolist():
        fill_val = round(num_base[col].notna().mean() * 100, 1)
        fill_list.append(fill_val)

    num_stats = num_stats.reset_index(drop=True)
    num_stats["fill_pct"] = fill_list          # same length guaranteed
    num_stats = num_stats.sort_values("fill_pct", ascending=False)
    num_stats = num_stats.reset_index(drop=True)

    print(f"  Stats built for : {len(num_stats)} columns")
else:
    num_stats = pd.DataFrame(
        columns=["column","count","mean","std",
                 "min","25%","50%","75%","90%","95%","max","fill_pct"]
    )
    print("  No numeric columns found with fill >= 10%")
