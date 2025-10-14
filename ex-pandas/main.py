
#!/usr/bin/env python3
import sys, time
import pandas as pd
from common.benchmark_utils import MetricsSampler, ensure_results_dir, write_metrics

def run(source_uri: str):
    sampler = MetricsSampler()
    t0 = time.time()
    with sampler:
        df_iter = pd.read_json(f"{source_uri}/*.json", lines=True, storage_options=None, chunksize=2_000_000)
        parts = []
        for chunk in df_iter:
            chunk["status"] = chunk["message"].str.extract(r"HTTP\s+Status\s+Code:\s*(\d{3})", expand=False)
            parts.append(chunk[["status"]])
        df = pd.concat(parts, ignore_index=True)
        grp = df.groupby("status", dropna=True).size().rename("count").reset_index()
        total = int(grp["count"].sum())
        grp["rate"] = grp["count"] / total if total else 0.0
    wall = time.time() - t0
    out_path = ensure_results_dir("pandas")
    grp.to_csv(out_path, index=False)
    write_metrics("pandas", source_uri, wall, sampler.summary())
    print(f"[ex-pandas] filas={total} tiempo={wall:.2f}s -> {out_path}")

if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else "s3://REPLACE_ME_BUCKET/logs"
    run(src)
