import sys, re
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_prefix', 'output_path', 'target_config', 'run_date'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext); job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.codegen.wholeStage", "false")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.shuffle.partitions", "400")

SRC    = args['source_prefix']    # s3://.../20260521/
OUT    = args['output_path']
COLUMN = "metadata_measurementconfigurationname"
TARGET = args['target_config']

# --- JSON source ---
# Default assumes one JSON object per line (JSONL). If each file is a single
# multi-line JSON object, add .option("multiLine", "true").
df = (spark.read
        .option("multiLine", "false")        # set "true" if each file is one big JSON object
        .json(SRC)
        .filter(F.col(COLUMN) == TARGET))

pat = re.compile(r'^(.*?)(\d+)$')
groups, statics = {}, []
for c in df.columns:
    m = pat.match(c)
    if m: groups.setdefault(m.group(1).strip(), {})[int(m.group(2))] = c
    else: statics.append(c)
series   = {b: m for b, m in groups.items() if len(m) > 1}
statics += [next(iter(m.values())) for b, m in groups.items() if len(m) == 1]
print("series:", len(series), "statics:", len(statics))

for b, idxmap in series.items():
    name = re.sub(r'\s+', '', b)
    idxs = sorted(idxmap)
    parts = []
    for i in idxs:
        parts += [str(i), f"`{idxmap[i]}`"]
    expr = f"stack({len(idxs)}, {', '.join(parts)}) as (sample_index, value)"
    sel = [f"`{c}`" for c in statics] + [expr]
    melted = (df.selectExpr(*sel)
                .withColumn("sensor", F.lit(name))
                .withColumn("date", F.lit(args['run_date'])))
    melted.write.mode("overwrite").partitionBy("date", "sensor").parquet(OUT)
    print("wrote sensor:", name)

job.commit()
