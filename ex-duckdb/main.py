#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Procesamiento con DuckDB:
- Lee múltiples archivos JSON (NDJSON) desde un directorio local.
- Extrae el status HTTP con regex y lo agrupa por "bucket" (primer dígito: 2, 4, 5).
- Imprime tiempo de ejecución y un dict con los conteos por bucket.

Uso:
  python3 main.py --input /ruta/a/directorio_con_json
"""

import argparse
import os
import sys
import time
import duckdb

def run(input_dir: str) -> None:
    t0 = time.time()

    # Validaciones básicas
    if not os.path.isdir(input_dir):
        print(f"[duckdb] ERROR: '{input_dir}' no es un directorio válido.", file=sys.stderr)
        sys.exit(1)

    # Patrón de archivos .json (NDJSON) dentro del directorio
    pattern = os.path.join(input_dir, "*.json")

    # Consulta SQL:
    # 1) Lee NDJSON (newline-delimited) con read_json_auto
    # 2) Extrae el código HTTP con regexp_extract (3 dígitos)
    # 3) Calcula el "bucket" como el primer dígito del status (2,4,5)
    # 4) Agrega conteos por bucket y calcula tasas
    sql = f"""
    WITH data AS (
      SELECT *
      FROM read_json_auto('{pattern}', format='newline_delimited')
    ),
    extracted AS (
      SELECT
        /* extrae 3 dígitos; si no hay match, NULL */
        regexp_extract(message, 'HTTP\\s+Status\\s+Code:\\s*(\\d{{3}})', 1) AS status3
      FROM data
    ),
    bucketed AS (
      SELECT
        CASE
          WHEN status3 IS NULL THEN NULL
          ELSE substr(status3, 1, 1)  -- '2', '4' o '5'
        END AS bucket
      FROM extracted
    )
    SELECT
      bucket,
      COUNT(*) AS count,
      COUNT(*) * 1.0 / NULLIF(SUM(COUNT(*)) OVER(), 0) AS rate
    FROM bucketed
    WHERE bucket IS NOT NULL
    GROUP BY bucket
    ORDER BY bucket;
    """

    # Ejecutar
    con = duckdb.connect(database=":memory:")
    try:
        df = con.execute(sql).df()
    except duckdb.IOException as e:
        # Suele ocurrir si no hay archivos .json
        print(f"[duckdb] ERROR de lectura de JSON ({pattern}): {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"[duckdb] ERROR ejecutando consulta: {e}", file=sys.stderr)
        sys.exit(3)
    finally:
        con.close()

    elapsed = time.time() - t0

    # Convertir resultado a dict { '2': conteo, '4': conteo, '5': conteo }
    buckets = {'2': 0, '4': 0, '5': 0}
    if not df.empty:
        for _, row in df.iterrows():
            b = str(row['bucket'])
            c = int(row['count'])
            if b in buckets:
                buckets[b] = c

    # Salida estándar: igual formato que tus experimentos previos
    print(f"Execution time: {elapsed:.6f} seconds")
    print(buckets)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Directorio local con archivos .json (NDJSON)")
    args = ap.parse_args()
    run(args.input)

if __name__ == "__main__":
    main()
