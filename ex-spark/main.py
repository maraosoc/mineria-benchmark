#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Procesamiento con Spark (local[*]):
- Lee NDJSON desde un directorio local con múltiples archivos .json.
- Extrae el código HTTP con regex y calcula el "bucket" = primer dígito (2, 4, 5).
- Devuelve conteos agregados por bucket + tiempo total de ejecución.

Uso:
  python3 main.py --input /ruta/a/directorio_con_json

Salida esperada (para que user_data/run.sh la capture):
  Execution time: X.YYYYYY seconds
  {'2': 123, '4': 45, '5': 6}
"""

import argparse
import os
import sys
import time
import glob

from pyspark.sql import SparkSession, functions as F


def build_spark(app_name: str = "BenchmarkSparkLocal") -> SparkSession:
    """
    Construye una SparkSession local con configs útiles para procesamiento batch.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # ejecuta en la propia EC2 con todos los cores disponibles
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.files.maxPartitionBytes", "256m")
        # Directorio local para shuffle/temp (opcional, por si necesitas controlar discos)
        .config("spark.local.dir", "/tmp/spark")
        .getOrCreate()
    )


def run(input_dir: str) -> None:
    """
    Ejecuta el pipeline:
    - Valida entrada
    - Lee JSON (NDJSON)
    - Extrae status 3 dígitos y bucket = primer dígito
    - Agrega por bucket y recolecta al driver
    - Imprime tiempo + dict {'2':..., '4':..., '5':...}
    """
    t0 = time.time()

    # Validación previa: Spark falla si el patrón no tiene archivos; lo verificamos nosotros.
    if not os.path.isdir(input_dir):
        print(f"[spark] ERROR: '{input_dir}' no es un directorio válido.", file=sys.stderr)
        sys.exit(1)

    pattern = os.path.join(input_dir, "*.json")
    if not glob.glob(pattern):
        print(f"[spark] ERROR: no se encontraron archivos JSON en '{pattern}'.", file=sys.stderr)
        sys.exit(2)

    spark = build_spark()
    try:
        # Lectura de NDJSON (un objeto por línea)
        df = spark.read.json(pattern)

        # Extraer status de 3 dígitos y primer dígito como bucket
        out = (
            df
            .withColumn(
                "status3",
                F.regexp_extract(F.col("message"), r"HTTP\s+Status\s+Code:\s*(\d{3})", 1)
            )
            .withColumn(
                "bucket",
                F.when(F.col("status3") != "", F.substring("status3", 1, 1)).otherwise(F.lit(None))
            )
            .where(F.col("bucket").isNotNull())
            .groupBy("bucket")
            .count()
            .withColumn("rate", F.col("count") / F.sum("count").over())
            .orderBy("bucket")
        )

        rows = out.collect()
    except Exception as e:
        print(f"[spark] ERROR ejecutando el job: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(3)

    elapsed = time.time() - t0

    # Convertir a dict {'2': 0, '4': 0, '5': 0} para mantener compatibilidad con tu run.sh
    buckets = {'2': 0, '4': 0, '5': 0}
    for r in rows:
        b = str(r["bucket"])
        c = int(r["count"])
        if b in buckets:
            buckets[b] = c

    print(f"Execution time: {elapsed:.6f} seconds")
    print(buckets)

    spark.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Directorio local con archivos .json (NDJSON)")
    args = ap.parse_args()
    run(args.input)


if __name__ == "__main__":
    main()
