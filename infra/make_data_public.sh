#!/usr/bin/env bash
# ============================================================
# Make the Amazon Reviews data publicly readable on S3.
#
# This lets anyone query with DuckDB without AWS credentials,
# which is useful if you want to run the DuckDB demos on a
# machine that does not have an IAM instance profile attached.
#
# Only the amazon_reviews/ prefix becomes public. The rest of
# the bucket stays private.
#
# How it works:
#   1. S3 Block Public Access prevents ANY public policy by
#      default. We selectively lower BlockPublicPolicy and
#      RestrictPublicBuckets while keeping BlockPublicAcls and
#      IgnorePublicAcls enabled (ACLs stay locked down).
#   2. A bucket policy grants s3:GetObject on the prefix and
#      s3:ListBucket with a prefix condition.
#
# Textbook reference:
#   SQL on the Cluster > DuckDB in the cloud
#   https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w4c_sql_on_cluster.html#duckdb-in-cloud
#
# Usage:
#   bash infra/make_data_public.sh
#
# Prerequisites:
#   aws sso login --profile ilya-ubc-aws-student
#
# To use on your own account:
#   1. Change PROFILE and BUCKET to your own values
#   2. Make sure you understand the implications: anyone on the
#      internet can read the files under amazon_reviews/
# ============================================================

set -euo pipefail

PROFILE="ilya-ubc-aws-student"
REGION="ca-central-1"
BUCKET="dsci525-data-2026"

echo "Step 1: Selectively lower Block Public Access"
echo "  Keeping BlockPublicAcls=true, IgnorePublicAcls=true (ACLs stay locked)"
echo "  Setting BlockPublicPolicy=false, RestrictPublicBuckets=false (allow policy-based public access)"
aws s3api put-public-access-block \
  --bucket "${BUCKET}" \
  --public-access-block-configuration \
    'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=false,RestrictPublicBuckets=false' \
  --profile "${PROFILE}" \
  --region "${REGION}"

echo ""
echo "Step 2: Add bucket policy for public read on amazon_reviews/ prefix"
# Two statements because GetObject and ListBucket scope differently:
#   - GetObject uses the object ARN: arn:aws:s3:::bucket/prefix/*
#   - ListBucket uses the bucket ARN with a Condition on s3:prefix
aws s3api put-bucket-policy \
  --bucket "${BUCKET}" \
  --policy "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Sid\": \"PublicGetAmazonReviews\",
        \"Effect\": \"Allow\",
        \"Principal\": \"*\",
        \"Action\": \"s3:GetObject\",
        \"Resource\": \"arn:aws:s3:::${BUCKET}/amazon_reviews/*\"
      },
      {
        \"Sid\": \"PublicListAmazonReviews\",
        \"Effect\": \"Allow\",
        \"Principal\": \"*\",
        \"Action\": \"s3:ListBucket\",
        \"Resource\": \"arn:aws:s3:::${BUCKET}\",
        \"Condition\": {
          \"StringLike\": {
            \"s3:prefix\": [\"amazon_reviews/*\"]
          }
        }
      }
    ]
  }" \
  --profile "${PROFILE}" \
  --region "${REGION}"

echo ""
echo "Done. Public read enabled for s3://${BUCKET}/amazon_reviews/*"
echo ""
echo "Test (no credentials needed):"
echo "  aws s3 ls s3://${BUCKET}/amazon_reviews/ --no-sign-request --region ${REGION}"
echo ""
echo "To revert (re-enable full Block Public Access):"
echo "  aws s3api put-public-access-block --bucket ${BUCKET} --public-access-block-configuration 'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true' --profile ${PROFILE} --region ${REGION}"
echo "  aws s3api delete-bucket-policy --bucket ${BUCKET} --profile ${PROFILE} --region ${REGION}"
