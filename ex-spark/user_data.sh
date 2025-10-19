#!/bin/bash
BUCKET="${bucket_name}"
EXPERIMENT="${experiment_name}"
SOURCE="${source_name}"
DATASIZE="${data_size}"

export HOME=/home/ubuntu
wget -qO- https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"


# shellcheck disable=SC1091
source "$HOME"/.local/bin/env
sudo apt update
sudo apt install -y python3 python3-pip awscli
sudo apt install -y openjdk-17-jre-headless

# Configurar JAVA_HOME para PySpark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

pip3 install pyspark

# Descargamos el script de main.py para realizar el test
aws s3 sync s3://$${BUCKET}/scripts/$${EXPERIMENT}/ /home/ubuntu/$${EXPERIMENT}/

aws s3 sync s3://$${BUCKET}/jsondata/$${DATASIZE}/ /home/ubuntu/$${EXPERIMENT}/data/

# Crear directorio temporal para Spark
sudo mkdir -p /tmp/spark
sudo chown ubuntu:ubuntu /tmp/spark


python3 /home/ubuntu/$${EXPERIMENT}/main.py --input /home/ubuntu/$${EXPERIMENT}/data > /home/ubuntu/output.log


aws s3 cp  "/home/ubuntu/output.log"  s3://$${BUCKET}/results/$${EXPERIMENT}/$${DATASIZE}/output.log