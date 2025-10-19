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
pip3 install pandas duckdb

# Descargamos el script de main.py 
aws s3 sync s3://$${BUCKET}/scripts/$${EXPERIMENT}/ /home/ubuntu/$${EXPERIMENT}/

aws s3 sync s3://$${BUCKET}/jsondata/$${DATASIZE}/ /home/ubuntu/$${EXPERIMENT}/data/

python3 /home/ubuntu/$${EXPERIMENT}/main.py --input /home/ubuntu/$${EXPERIMENT}/data > /home/ubuntu/output.log

aws s3 cp  "/home/ubuntu/output.log"  s3://$${BUCKET}/results/$${EXPERIMENT}/$${DATASIZE}/output.log
