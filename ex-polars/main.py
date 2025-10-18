#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Procesamiento con Polars (lazy):
- Lee múltiples archivos JSON (NDJSON) desde un directorio local.
- Extrae el status HTTP con regex, calcula el "bucket" (primer dígito: 2, 4, 5).
- Entrega conteos agregados por bucket y el tiempo total de ejecución.

Uso:
  python3 main.py --input /ruta/a/directorio_con_json
Salida estándar (para que user_data la capture):
  Execution time: X.YYYYYY seconds
  {'2': 123, '4': 45, '5': 6}
"""

import argparse
import os
import sys
import time
import polars as pl


def run(input_dir: str) -> None:
    t0 = time.time()

    # Validaciones básicas de entrada
    if not os.path.isdir(input_dir):
        print(f"[polars] ERROR: '{input_dir}' no es un directorio válido.", file=sys.stderr)
        sys.exit(1)

    # Patrón de archivos .json (se asume NDJSON: un objeto por línea)
    pattern = os.path.join(input_dir, "*.json")

    try:
        # Escaneo "lazy" (no carga a memoria hasta collect())
        scan = pl.scan_ndjson(pattern)

        # Pipeline: extraer status (3 dígitos) -> tomar primer dígito como bucket -> agregar
        out = (
            scan
            .with_columns(
                pl.col("message")
                .str.extract(r"HTTP\s+Status\s+Code:\s*(\d{3})", 1)  # captura "200", "404", etc.
                .alias("status3")
            )
            .with_columns(
                # bucket = primer dígito de status3 -> '2', '4' o '5'
                pl.when(pl.col("status3").is_not_null())
                  .then(pl.col("status3").str.slice(0, 1))
                  .otherwise(None)
                  .alias("bucket")
            )
            .filter(pl.col("bucket").is_not_null())
            .group_by("bucket")
            .len()
            .rename({"len": "count"})
            .with_columns(
                (pl.col("count") / pl.col("count").sum()).alias("rate")
            )
            .sort("bucket")
            # collect(streaming=True) activa ejecución por streaming cuando es posible
            .collect(streaming=True)
        )
    except pl.exceptions.ComputeError as e:
        # Suele ocurrir si no hay archivos que coincidan con el patrón o formato inválido
        print(f"[polars] ERROR de lectura/scan ({pattern}): {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"[polars] ERROR ejecutando pipeline: {e}", file=sys.stderr)
        sys.exit(3)

    elapsed = time.time() - t0

    # Convertir resultado a dict {'2': conteo, '4': conteo, '5': conteo}
    buckets = {'2': 0, '4': 0, '5': 0}
    if out.height > 0:
        # out es un DataFrame con columnas: bucket(str), count(i64), rate(f64)
        for row in out.iter_rows(named=True):
            b = str(row["bucket"])
            c = int(row["count"])
            if b in buckets:
                buckets[b] = c

    # Salidas esperadas por tu user_data/run.sh
    print(f"Execution time: {elapsed:.6f} seconds")
    print(buckets)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Directorio local con archivos .json (NDJSON)")
    args = ap.parse_args()
    run(args.input)


if __name__ == "__main__":
    main()
