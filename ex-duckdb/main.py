
#!/usr/bin/env python3
import sys, time, duckdb
from common.benchmark_utils import MetricsSampler, ensure_results_dir, write_metrics

SQL = '''
INSTALL httpfs;
LOAD httpfs;
SET s3_region='{REGION}';
SET s3_use_ssl=true;
SET s3_url_style='path';

WITH data AS (
  SELECT *
  FROM read_json_auto('{URI}', format='newline_delimited')
),
extracted AS (
  SELECT regexp_extract(message, 'HTTP\\s+Status\\s+Code:\\s*(\\d{{3}})', 1) AS status
  FROM data
)
SELECT status, COUNT(*) AS count, COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS rate
FROM extracted
GROUP BY status
ORDER BY status;
'''

def run(source_uri: str, region: str = "us-east-1"):
    sampler = MetricsSampler()
    t0 = time.time()
    with sampler:
        con = duckdb.connect(database=':memory:')
        query = SQL.replace("{URI}", f"{source_uri}/*.json").replace("{REGION}", region)
        df = con.execute(query).df()
    wall = time.time() - t0
    out_path = ensure_results_dir("duckdb")
    df.to_csv(out_path, index=False)
    write_metrics("duckdb", source_uri, wall, sampler.summary())
    print(f"[ex-duckdb] filas={int(df['count'].sum())} tiempo={wall:.2f}s -> {out_path}")

if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else "s3://REPLACE_ME_BUCKET/logs"
    run(src)
