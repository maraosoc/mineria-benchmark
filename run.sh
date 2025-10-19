#!/bin/bash
EXPERIMENT=$1
DATASIZE=$2
BUCKET="mineria-benchmark-maraosoc-data"
SOURCE=s3://${BUCKET}/jsondata/${DATASIZE}/

# Absolute path to this script
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/${EXPERIMENT}/user_data.sh"

# Print initial info
echo "============================================================"
echo " Running: ${EXPERIMENT}"
echo " Data size: ${DATASIZE}"
echo " Bucket: ${BUCKET}"
echo " Source: ${SOURCE}"
echo "============================================================"

# copy main.py to s3
aws s3 cp  "./${EXPERIMENT}/main.py"  s3://${BUCKET}/scripts/${EXPERIMENT}/main.py --region us-east-2 --profile maraosoc
# Delete previous results
RESULT_PATH="results/${EXPERIMENT}/${DATASIZE}/output.log"
S3_FILE="s3://${BUCKET}/${RESULT_PATH}"
echo "Verifying previous results at ${S3_FILE} ..."
if aws s3 ls "${S3_FILE}" --region us-east-2 --profile maraosoc >/dev/null 2>&1; then
  aws s3 rm "${S3_FILE}" --region us-east-2 --profile maraosoc
  echo "Previous results deleted."
else
  echo "No previous results found."
fi
# Delete previous infrastructure folder
rm -rf ${EXPERIMENT}/infrastructure
# Create infrastructure folder
mkdir -p ${EXPERIMENT}/infrastructure
# Copy infrastructure files
cp -r ./infrastructure/ec2/. ${EXPERIMENT}/infrastructure/

cd "${EXPERIMENT}/infrastructure"

export AWS_PROFILE="maraosoc"
export TF_VAR_profile="maraosoc"
export TF_VAR_region="us-east-2"
export TF_VAR_owner="maraosoc"

terraform init  -backend-config="key=${EXPERIMENT}/backend.tfstate"

terraform apply -var="script_path=${SCRIPT_PATH}"\
                -var="source_name=${SOURCE}"\
                -var="bucket_name=${BUCKET}"\
                -var="experiment_name=${EXPERIMENT}"\
                -var="data_size=${DATASIZE}"\
                -auto-approve 

# Wait for EC2 to upload results
RESULT_PATH="results/${EXPERIMENT}/${DATASIZE}/output.log"
S3_FILE="s3://${BUCKET}/${RESULT_PATH}"
echo
echo "Waiting for EC2 to upload results to ${S3_FILE} ..."

# Wait up to 10 minutes
for i in {1..20}; do
  if aws s3 ls "${S3_FILE}" >/dev/null 2>&1; then
    echo "File found in S3."
    break
  fi
  echo "Waiting for results... attempt $i/20"
  sleep 60
done

# Show results
if aws s3 ls "${S3_FILE}" >/dev/null 2>&1; then
  echo
  echo "Results of (${EXPERIMENT}, ${DATASIZE}):"
  echo "------------------------------------------------------------"
  aws s3 cp "${S3_FILE}" - | cat
  echo "------------------------------------------------------------"
 
else
  echo "No results found after waiting."
fi

# Finish execution and destroy infrastructure
echo
echo "Destroying infrastructure..."
terraform destroy -var="script_path=${SCRIPT_PATH}"\
                -var="source_name=${SOURCE}"\
                -var="bucket_name=${BUCKET}"\
                -var="experiment_name=${EXPERIMENT}"\
                -var="data_size=${DATASIZE}"\
                -auto-approve || true

echo
echo "Execution finished. Press [any key] to exit..."
read

# uv run $EXPERIMENT/main.py $SOURCE
