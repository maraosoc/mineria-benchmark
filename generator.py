
#!/usr/bin/env python3
import argparse, json, random, time
import boto3
from concurrent.futures import ThreadPoolExecutor

SERVICES = ["auth", "payments", "orders", "monitoring", "search", "catalog", "shipping"]
STATUS = [200, 201, 204, 301, 400, 401, 403, 404, 409, 429, 500, 502, 503, 504]
STATUS_WEIGHTS = [0.6, 0.05, 0.05, 0.02, 0.05, 0.02, 0.01, 0.06, 0.01, 0.02, 0.07, 0.02, 0.015, 0.005]

def make_event(ts, rng):
  return {
    "service": rng.choice(SERVICES),
    "timestamp": ts,
    "message": f"HTTP Status Code: {rng.choices(STATUS, weights=STATUS_WEIGHTS, k=1)[0]}"
  }

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--bucket", required=True)
  ap.add_argument("--prefix", default="logs")
  ap.add_argument("--target-gb", type=int, default=10, help="Tamaño total aproximado a generar")
  ap.add_argument("--shards", type=int, default=64, help="Archivos por lote")
  args = ap.parse_args()

  s3 = boto3.client("s3")
  total_bytes = args.target_gb * (1024**3)
  bytes_written = 0

  def write_shard(i):
    nonlocal bytes_written
    ts = time.time()
    rng = random.Random(i + ts)
    chunk_size = 5 * 1024 * 1024
    buf = []
    size = 0
    while size < chunk_size:
      ev = make_event(ts + rng.random(), rng)
      line = json.dumps(ev, separators=(",", ":")) + "\n"
      buf.append(line)
      size += len(line.encode("utf-8"))
    body = "".join(buf).encode("utf-8")
    key = f"{args.prefix}/part-{int(ts)}-{i:05d}.json"
    s3.put_object(Bucket=args.bucket, Key=key, Body=body)
    return len(body)

  with ThreadPoolExecutor(max_workers=min(32, args.shards)) as ex:
    while bytes_written < total_bytes:
      futs = [ex.submit(write_shard, i) for i in range(args.shards)]
      for f in futs:
        bytes_written += f.result()
        if bytes_written >= total_bytes:
          break

  print(f"Generados ~{bytes_written/(1024**3):.2f} GB en s3://{args.bucket}/{args.prefix}")

if __name__ == "__main__":
  main()
