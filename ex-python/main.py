
#!/usr/bin/env python3
import sys, time, collections, csv
from common.benchmark_utils import iter_s3_json_objects, extract_status, MetricsSampler, ensure_results_dir, write_metrics

def run(source_uri: str):
    counts = collections.Counter()
    total = 0
    sampler = MetricsSampler(interval=0.5)
    t0 = time.time()
    with sampler:
        for obj in iter_s3_json_objects(source_uri):
            code = extract_status(obj.get("message"))
            if code:
                counts[code] += 1
            total += 1
    wall = time.time() - t0
    out_path = ensure_results_dir("python")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["status_code", "count", "rate"])
        for code, cnt in sorted(counts.items()):
            rate = cnt/total if total else 0.0
            w.writerow([code, cnt, f"{rate:.6f}"])
    write_metrics("python", source_uri, wall, sampler.summary())
    print(f"[ex-python] total={total} unicos={len(counts)} tiempo={wall:.2f}s -> {out_path}")

if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else "s3://REPLACE_ME_BUCKET/logs"
    run(src)
