
import os
import re
import json
import time
import psutil
import threading
from typing import Dict, Iterable, Optional

import boto3

STATUS_RE = re.compile(r"HTTP\s+Status\s+Code:\s*(\d{3})")

def extract_status(message: str) -> Optional[str]:
    if not isinstance(message, str):
        return None
    m = STATUS_RE.search(message)
    return m.group(1) if m else None


class MetricsSampler:
    """Mide CPU%, RSS (MB) y IO a intervalos regulares."""
    def __init__(self, interval: float = 0.5):
        self.interval = interval
        self._stop = threading.Event()
        self._thread = None
        self._cpu = []
        self._rss = []
        self._read_bytes = []
        self._write_bytes = []

    def _sample(self):
        proc = psutil.Process(os.getpid())
        while not self._stop.is_set():
            try:
                self._cpu.append(psutil.cpu_percent(interval=None))
                mem = proc.memory_info().rss / (1024**2)
                self._rss.append(mem)
                io_counters = proc.io_counters()
                self._read_bytes.append(io_counters.read_bytes)
                self._write_bytes.append(io_counters.write_bytes)
            except Exception:
                pass
            time.sleep(self.interval)

    def __enter__(self):
        psutil.cpu_percent(interval=None)  # prime
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._stop.set()
        if self._thread:
            self._thread.join()

    def summary(self) -> Dict:
        read_delta = self._read_bytes[-1] - self._read_bytes[0] if len(self._read_bytes) >= 2 else 0
        write_delta = self._write_bytes[-1] - self._write_bytes[0] if len(self._write_bytes) >= 2 else 0
        return {
            "cpu_percent_avg": (sum(self._cpu)/len(self._cpu)) if self._cpu else 0.0,
            "rss_mb_max": max(self._rss) if self._rss else 0.0,
            "read_bytes": int(read_delta),
            "write_bytes": int(write_delta),
            "samples": len(self._cpu),
        }

def ensure_results_dir(experiment: str) -> str:
    os.makedirs("results", exist_ok=True)
    out = os.path.join("results", f"{experiment}.csv")
    return out

def write_metrics(experiment: str, dataset_label: str, wall_time_s: float, sampler_metrics: Dict):
    os.makedirs("results", exist_ok=True)
    path = os.path.join("results", f"metrics_{experiment}.json")
    record = {
        "dataset": dataset_label,
        "wall_time_s": wall_time_s,
        **sampler_metrics,
        "timestamp": time.time(),
    }
    data = []
    if os.path.exists(path):
        try:
            import json as _json
            with open(path, "r", encoding="utf-8") as f:
                data = _json.load(f)
                if not isinstance(data, list):
                    data = [data]
        except Exception:
            data = []
    data.append(record)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def iter_s3_json_objects(s3_uri: str):
    """Itera JSONs desde S3 (JSONL por línea o un objeto por archivo)."""
    if not s3_uri.startswith("s3://"):
        raise ValueError("Se esperaba un s3://bucket/prefix")

    _, rest = s3_uri.split("s3://", 1)
    bucket, *prefix_parts = rest.split("/", 1)
    prefix = prefix_parts[0] if prefix_parts else ""

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            text = body.decode("utf-8").strip()
            lines = text.splitlines()
            is_jsonl = False
            if len(lines) > 1:
                is_jsonl = True
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        is_jsonl = False
                        break
            if not is_jsonl:
                try:
                    yield json.loads(text)
                except json.JSONDecodeError:
                    continue
