
#!/bin/bash
set -euxo pipefail

apt-get update -y
apt-get install -y python3-pip python3-venv openjdk-11-jre-headless git curl

# Instalar uv
curl -LsSf https://astral.sh/uv/install.sh | sh || true

# Clonar repo (ajuste la URL a su GitHub)
if [ ! -d /opt/mineria-benchmark ]; then
  git clone https://github.com/REPLACE_ME/mineria-benchmark.git /opt/mineria-benchmark || true
fi

cd /opt/mineria-benchmark
if command -v uv >/dev/null 2>&1; then
  uv sync
else
  python3 -m venv .venv
  . .venv/bin/activate
  pip install -U pip
  pip install -e .
fi

echo "Instalación lista. Use ./run.sh para correr experimentos."
