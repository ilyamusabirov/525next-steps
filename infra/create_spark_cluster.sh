#!/usr/bin/env bash
# ============================================================
# Create an interactive EMR cluster for the SparkSQL demo.
#
# This creates a LONG-RUNNING cluster so you can SSH in and
# run Spark queries interactively. Remember to terminate it
# when you're done to avoid charges.
#
# Textbook reference:
#   SQL on the Cluster > SparkSQL: distributed SQL on the cluster
#   https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w4c_sql_on_cluster.html#sparksql
#
# Usage:
#   bash infra/create_spark_cluster.sh
#
# Prerequisites:
#   aws sso login --profile ilya-ubc-aws-student
#
# To use on your own account:
#   1. Change PROFILE to your AWS CLI profile name
#   2. Change KEY_NAME to your EC2 key pair name
#   3. If your public subnets are not tagged with "public" in
#      their name, set SUBNET_ID manually below
# ============================================================

set -euo pipefail

PROFILE="ilya-ubc-aws-student"
REGION="ca-central-1"
KEY_NAME="mds-ilya-ec2"          # your EC2 key pair name
LOG_BUCKET="s3://dsci525-data-2026/emr-logs/"

# --- Auto-discover a public subnet ---
# Looks for subnets with "public" (case-insensitive) in their Name tag.
# Override by setting SUBNET_ID before running the script:
#   SUBNET_ID=subnet-abc123 bash infra/create_spark_cluster.sh
if [ -z "${SUBNET_ID:-}" ]; then
    echo "Discovering public subnets..."
    SUBNETS=$(aws ec2 describe-subnets \
        --filters "Name=tag:Name,Values=*public*,*Public*" \
        --query "Subnets[*].[SubnetId,Tags[?Key=='Name'].Value|[0],AvailabilityZone]" \
        --output text \
        --profile "${PROFILE}" --region "${REGION}")

    if [ -z "${SUBNETS}" ]; then
        echo "ERROR: No subnets with 'public' in their name found."
        echo "Set SUBNET_ID manually: SUBNET_ID=subnet-xxx bash infra/create_spark_cluster.sh"
        exit 1
    fi

    echo "Available public subnets:"
    echo "${SUBNETS}"
    echo ""

    # Pick the first one
    SUBNET_ID=$(echo "${SUBNETS}" | head -1 | awk '{print $1}')
    echo "Using: ${SUBNET_ID}"
fi

echo ""
echo "Creating 3-node EMR cluster (1 primary + 2 core, m6a.xlarge)..."
CLUSTER_ID=$(aws emr create-cluster \
  --name "sql-demo-spark" \
  --release-label emr-7.8.0 \
  --applications Name=Spark Name=JupyterHub Name=Livy \
  --instance-groups '[
    {"InstanceGroupType":"MASTER","InstanceType":"m6a.xlarge","InstanceCount":1,
     "EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp3","SizeInGB":32},"VolumesPerInstance":1}]}},
    {"InstanceGroupType":"CORE","InstanceType":"m6a.xlarge","InstanceCount":2,
     "EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp3","SizeInGB":32},"VolumesPerInstance":1}]}}
  ]' \
  --ec2-attributes "KeyName=${KEY_NAME},SubnetId=${SUBNET_ID}" \
  --log-uri "${LOG_BUCKET}" \
  --managed-scaling-policy '{"ComputeLimit":{"UnitType":"Instances","MinimumCapacityUnits":2,"MaximumCapacityUnits":2}}' \
  --profile "${PROFILE}" \
  --region "${REGION}" \
  --query 'ClusterId' --output text)

echo ""
echo "Cluster ID: ${CLUSTER_ID}"
echo ""
echo "Check status:"
echo "  aws emr describe-cluster --cluster-id ${CLUSTER_ID} --query 'Cluster.Status.State' --profile ${PROFILE} --region ${REGION}"
echo ""
echo "Get primary node DNS (once WAITING):"
echo "  aws emr describe-cluster --cluster-id ${CLUSTER_ID} --query 'Cluster.MasterPublicDnsName' --output text --profile ${PROFILE} --region ${REGION}"
echo ""
echo "SSH in:"
echo "  ssh -i ~/.ssh/${KEY_NAME}.pem hadoop@<primary-dns>"
echo ""
echo "REMEMBER to terminate when done:"
echo "  aws emr terminate-clusters --cluster-ids ${CLUSTER_ID} --profile ${PROFILE} --region ${REGION}"
