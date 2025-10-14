
# Benchmark de Herramientas de Datos

Comparación de rendimiento entre Python Puro, Pandas, Polars, DuckDB y Spark para calcular tasas de códigos HTTP a partir de logs JSON almacenados en S3.

- El objetivo es medir tiempo de proceso, CPU promedio, máximo RSS (MB) y bytes leídos/escritos.
- Escenarios de *5, 10, 15, 20 y 25 GB*.
- Instancia: m5.2xlarge (Ubuntu 22.04).

## 1) Preparacion de Terraform

### Creación del backend
```bash
bash create_backend.sh maraosoc
```
Para crear en AWS:
- Bucket S3: mineria-benchmark-maraosoc-terraform-state
- KMS key para cifrado del estado

> La ejecución de este script mostrará el nombre del bucket y la clave KMS que se deben actualizar en `infrastructure/provider.tf`

### Inicializar terraform
```bash
cd infrastructure
terraform init
terraform apply -auto-approve   -var 'bucket_name=mi-bucket-mineria'   -var 'vpc_id=vpc-...'   -var 'subnet_id=subnet-...'   -var 'key_pair_name=mi-keypair'
```
Salidas: `bucket_name`, `instance_public_ip`.

### Variables personales por defecto
```bash
export AWS_PROFILE=maraosoc
export TF_VAR_profile=maraosoc
export TF_VAR_region=us-east-2
export TF_VAR_owner=maraosoc

## 2) Conexión a la EC2
```bash
ssh -i mi-keypair.pem ubuntu@<instance_public_ip>
cd /opt/mineria-benchmark
```

## 3) Generar datos en S3
```bash
uv run generator.py --bucket <SU_BUCKET> --prefix logs_5GB  --target-gb 5
uv run generator.py --bucket <SU_BUCKET> --prefix logs_10GB --target-gb 10
uv run generator.py --bucket <SU_BUCKET> --prefix logs_15GB --target-gb 15
uv run generator.py --bucket <SU_BUCKET> --prefix logs_20GB --target-gb 20
uv run generator.py --bucket <SU_BUCKET> --prefix logs_25GB --target-gb 25
```

## 4) Ejecutar benchmarks
```bash
./run.sh s3://<SU_BUCKET>/<PREFIJO> <experimento>
# ejemplos
./run.sh s3://<BUCKET>/logs_5GB  ex-python
./run.sh s3://<BUCKET>/logs_10GB ex-pandas
./run.sh s3://<BUCKET>/logs_15GB ex-polars
./run.sh s3://<BUCKET>/logs_20GB ex-duckdb
./run.sh s3://<BUCKET>/logs_25GB ex-spark
```

Resultados:
- CSV por herramienta: `./results/<tool>.csv`
- Métricas: `./results/metrics_<tool>.json`

## 5) Opcional: Parquet
- Polars y Spark incluyen un argumento `use_parquet` (edite `main.py` si los desea probar).

## 6) Tests
```bash
uv run pytest -q
```

## Buenas prácticas
- Use el IAM Role de la instancia para S3 (no suba claves).
- Ajuste particiones/límites según tamaño de objetos.
