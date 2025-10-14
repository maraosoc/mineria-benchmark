
#!/usr/bin/env python3
import sys, time
import polars as pl
from common.benchmark_utils import MetricsSampler, ensure_results_dir, write_metrics

def run(source_uri: str, use_parquet: bool = False):
    sampler = MetricsSampler()
    t0 = time.time()
    with sampler:
        if use_parquet:
            scan = pl.scan_parquet(f"{source_uri}/*.parquet")
        else:
            scan = pl.scan_ndjson(f"{source_uri}/*.json")
        df = (
            scan
            .with_columns(
                pl.col("message").str.extract(r"HTTP\s+Status\s+Code:\s*(\d{3})").alias("status")
            )
            .group_by("status")
            .len()
            .rename({"len": "count"})
            .with_columns((pl.col("count") / pl.col("count").sum()).alias("rate"))
            .collect(streaming=True)
        )
    wall = time.time() - t0
    out_path = ensure_results_dir("polars")
    df.write_csv(out_path)
    write_metrics("polars", source_uri + (" (parquet)" if use_parquet else " (json)"), wall, sampler.summary())
    print(f"[ex-polars] filas={int(df['count'].sum())} tiempo={wall:.2f}s -> {out_path}")

if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else "s3://REPLACE_ME_BUCKET/logs"
    run(src)
