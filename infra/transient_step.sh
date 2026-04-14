#!/usr/bin/env bash
# ============================================================
# Submit a Spark job on a TRANSIENT EMR cluster.
#
# The cluster starts, runs the step, and auto-terminates.
# This is the "batch inference" pattern: no SSH, no interactive
# session. You submit a script, it runs, results go to S3,
# the cluster shuts down. You pay only for compute time.
#
# Textbook reference:
#   Theory > Transient clusters
#   https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w4_theory.html
#
# Before running:
#   1. Upload the batch script to S3:
#      aws s3 cp infra/batch_user_summary.py s3://dsci525-data-2026/scripts/ \
#        --profile ilya-ubc-aws-student --region ca-central-1
#   2. Authenticate: aws sso login --profile ilya-ubc-aws-student
#
# To use on your own account:
#   1. Change PROFILE to your AWS CLI profile name
#   2. Change KEY_NAME to your EC2 key pair name
#   3. Upload batch_user_summary.py to your own S3 bucket
#      and update SCRIPT_S3 below
# ============================================================

set -euo pipefail

PROFILE="ilya-ubc-aws-student"
REGION="ca-central-1"
KEY_NAME="mds-ilya-ec2"
LOG_BUCKET="s3://dsci525-data-2026/emr-logs/"

# The PySpark script to run (must already be on S3)
SCRIPT_S3="s3://dsci525-data-2026/scripts/batch_user_summary.py"

# --- Auto-discover a public subnet ---
if [ -z "${SUBNET_ID:-}" ]; then
    echo "Discovering public subnets..."
    SUBNETS=$(aws ec2 describe-subnets \
        --filters "Name=tag:Name,Values=*public*,*Public*" \
        --query "Subnets[*].[SubnetId,Tags[?Key=='Name'].Value|[0],AvailabilityZone]" \
        --output text \
        --profile "${PROFILE}" --region "${REGION}")

    if [ -z "${SUBNETS}" ]; then
        echo "ERROR: No subnets with 'public' in their name found."
        echo "Set SUBNET_ID manually: SUBNET_ID=subnet-xxx bash infra/transient_step.sh"
        exit 1
    fi

    echo "Available public subnets:"
    echo "${SUBNETS}"
    SUBNET_ID=$(echo "${SUBNETS}" | head -1 | awk '{print $1}')
    echo "Using: ${SUBNET_ID}"
fi

echo ""
echo "Launching transient cluster with step: ${SCRIPT_S3}"
CLUSTER_ID=$(aws emr create-cluster \
  --name "sql-demo-transient" \
  --release-label emr-7.8.0 \
  --applications Name=Spark \
  --instance-groups '[
    {"InstanceGroupType":"MASTER","InstanceType":"m6a.xlarge","InstanceCount":1,
     "EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp3","SizeInGB":32},"VolumesPerInstance":1}]}},
    {"InstanceGroupType":"CORE","InstanceType":"m6a.xlarge","InstanceCount":2,
     "EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp3","SizeInGB":32},"VolumesPerInstance":1}]}}
  ]' \
  --ec2-attributes "KeyName=${KEY_NAME},SubnetId=${SUBNET_ID}" \
  --log-uri "${LOG_BUCKET}" \
  --steps "[{
    \"Type\":\"Spark\",
    \"Name\":\"User Summary\",
    \"ActionOnFailure\":\"TERMINATE_CLUSTER\",
    \"Args\":[\"${SCRIPT_S3}\"]
  }]" \
  --auto-terminate \
  --profile "${PROFILE}" \
  --region "${REGION}" \
  --query 'ClusterId' --output text)

echo ""
echo "Cluster ID: ${CLUSTER_ID}"
echo ""
echo "The cluster will:"
echo "  1. Provision 3 nodes (~5 min)"
echo "  2. Run batch_user_summary.py"
echo "  3. Auto-terminate when done"
echo ""
echo "Monitor:"
echo "  aws emr describe-cluster --cluster-id ${CLUSTER_ID} --query 'Cluster.Status.State' --profile ${PROFILE} --region ${REGION}"
echo ""
echo "Step logs (after completion):"
echo "  aws emr list-steps --cluster-id ${CLUSTER_ID} --profile ${PROFILE} --region ${REGION}"
