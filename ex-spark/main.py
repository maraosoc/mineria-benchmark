
#!/usr/bin/env python3
import sys, time
from pyspark.sql import SparkSession, functions as F
from common.benchmark_utils import MetricsSampler, ensure_results_dir, write_metrics

def build_spark(app_name="BenchmarkSpark"):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.files.maxPartitionBytes", "256m")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .getOrCreate())

def run(source_uri: str, use_parquet: bool = False):
    spark = build_spark()
    sampler = MetricsSampler()
    t0 = time.time()
    with sampler:
        if use_parquet:
            df = spark.read.parquet(f"{source_uri}/*.parquet")
        else:
            df = spark.read.json(f"{source_uri}/*.json")
        out = (df
               .withColumn("status", F.regexp_extract("message", r"HTTP\s+Status\s+Code:\s*(\d{3})", 1))
               .groupBy("status")
               .count()
               .withColumn("rate", F.col("count") / F.sum("count").over())
               .orderBy("status"))
        rows = out.collect()
    wall = time.time() - t0

    out_path = ensure_results_dir("spark")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("status_code,count,rate\n")
        for r in rows:
            f.write(f"{r['status']},{r['count']},{r['rate']}\n")

    write_metrics("spark", source_uri + (" (parquet)" if use_parquet else " (json)"), wall, sampler.summary())
    print(f"[ex-spark] filas={sum(r['count'] for r in rows)} tiempo={wall:.2f}s -> {out_path}")
    spark.stop()

if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else "s3://REPLACE_ME_BUCKET/logs"
    run(src)
