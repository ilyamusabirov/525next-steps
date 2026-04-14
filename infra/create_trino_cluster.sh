#!/usr/bin/env bash
# ============================================================
# Create a DEDICATED Trino cluster on EMR.
#
# Trino runs its own JVM process on each node, OUTSIDE of YARN.
# If you install both Spark and Trino on the same cluster, they
# compete for memory at the OS level (both think they own 80%
# of RAM). Run Trino on a separate cluster to avoid this.
#
# Textbook reference:
#   SQL on the Cluster > Trino: interactive distributed SQL
#   https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w4c_sql_on_cluster.html#sql-landscape
#
# Usage:
#   bash infra/create_trino_cluster.sh
#
# Prerequisites:
#   aws sso login --profile ilya-ubc-aws-student
#
# To use on your own account:
#   1. Change PROFILE to your AWS CLI profile name
#   2. Change KEY_NAME to your EC2 key pair name
#   3. Ensure the data bucket is accessible from EMR (either
#      public, or the EMR_EC2_DefaultRole has S3 read access)
# ============================================================

set -euo pipefail

PROFILE="ilya-ubc-aws-student"
REGION="ca-central-1"
KEY_NAME="mds-ilya-ec2"
LOG_BUCKET="s3://dsci525-data-2026/emr-logs/"

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
        echo "Set SUBNET_ID manually: SUBNET_ID=subnet-xxx bash infra/create_trino_cluster.sh"
        exit 1
    fi

    echo "Available public subnets:"
    echo "${SUBNETS}"
    SUBNET_ID=$(echo "${SUBNETS}" | head -1 | awk '{print $1}')
    echo "Using: ${SUBNET_ID}"
fi

echo ""
echo "Creating dedicated Trino cluster (no Spark)..."
CLUSTER_ID=$(aws emr create-cluster \
  --name "sql-demo-trino" \
  --release-label emr-7.8.0 \
  --applications Name=Trino \
  --instance-groups '[
    {"InstanceGroupType":"MASTER","InstanceType":"m6a.xlarge","InstanceCount":1,
     "EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp3","SizeInGB":32},"VolumesPerInstance":1}]}},
    {"InstanceGroupType":"CORE","InstanceType":"m6a.xlarge","InstanceCount":2,
     "EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp3","SizeInGB":32},"VolumesPerInstance":1}]}}
  ]' \
  --ec2-attributes "KeyName=${KEY_NAME},SubnetId=${SUBNET_ID}" \
  --log-uri "${LOG_BUCKET}" \
  --profile "${PROFILE}" \
  --region "${REGION}" \
  --query 'ClusterId' --output text)

echo ""
echo "Cluster ID: ${CLUSTER_ID}"
echo ""
echo "After the cluster reaches WAITING state:"
echo ""
echo "  # Get primary node DNS"
echo "  aws emr describe-cluster --cluster-id ${CLUSTER_ID} --query 'Cluster.MasterPublicDnsName' --output text --profile ${PROFILE} --region ${REGION}"
echo ""
echo "  # SSH to primary node"
echo "  ssh -i ~/.ssh/${KEY_NAME}.pem hadoop@<primary-dns>"
echo ""
echo "  # Connect to Trino CLI"
echo "  trino-cli --catalog hive --schema default"
echo ""
echo "  # Register the parquet data as a Trino table"
echo "  CREATE TABLE IF NOT EXISTS hive.default.amazon_reviews ("
echo "    rating DOUBLE, title VARCHAR, text VARCHAR,"
echo "    asin VARCHAR, parent_asin VARCHAR, user_id VARCHAR,"
echo "    helpful_vote INTEGER, verified_purchase BOOLEAN,"
echo "    category VARCHAR, year INTEGER"
echo "  ) WITH ("
echo "    external_location = 's3://dsci525-data-2026/amazon_reviews/',"
echo "    format = 'PARQUET'"
echo "  );"
echo ""
echo "  # Run the query DuckDB couldn't handle"
echo "  SELECT user_id, COUNT(*) AS n_reviews, ROUND(AVG(rating), 2) AS avg_rating"
echo "  FROM amazon_reviews"
echo "  GROUP BY user_id"
echo "  ORDER BY n_reviews DESC"
echo "  LIMIT 20;"
echo ""
echo "REMEMBER to terminate when done:"
echo "  aws emr terminate-clusters --cluster-ids ${CLUSTER_ID} --profile ${PROFILE} --region ${REGION}"
