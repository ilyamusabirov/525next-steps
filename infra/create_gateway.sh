#!/usr/bin/env bash
# ============================================================
# Launch the Gateway EC2 instance for the DuckDB demo.
#
# This creates a t3a.xlarge (4 vCPU, 16 GB RAM) Ubuntu instance
# with an instance profile for S3 access. It also serves as the
# SSH jump host for reaching the EMR cluster later.
#
# Textbook reference:
#   AWS CLI > EC2 Compute > Launching an instance
#   https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w3_aws_cli.html#launching-an-instance
#
# Usage:
#   bash infra/create_gateway.sh
#
# Prerequisites:
#   aws sso login --profile ilya-ubc-aws-student
#
# To use on your own account:
#   1. Change PROFILE to your AWS CLI profile name
#   2. Change KEY_NAME to your EC2 key pair name
#   3. Change INSTANCE_PROFILE to your instance profile name
#      (must have s3:GetObject and s3:ListBucket on the data bucket)
#   4. If your public subnets are not tagged with "public" in
#      their name, set SUBNET_ID manually before running
# ============================================================

set -euo pipefail

PROFILE="ilya-ubc-aws-student"
REGION="ca-central-1"
KEY_NAME="mds-ilya-ec2"
INSTANCE_PROFILE="dsci525-ec2-s3-reader"
INSTANCE_TYPE="t3a.xlarge"
SECURITY_GROUP="mds-525-sg"        # must allow inbound SSH (port 22)

# --- Auto-discover the latest Ubuntu 24.04 AMI ---
echo "Finding latest Ubuntu 24.04 AMI..."
AMI_ID=$(aws ec2 describe-images \
    --profile "${PROFILE}" --region "${REGION}" \
    --owners 099720109477 \
    --filters "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*" \
              "Name=state,Values=available" \
    --query 'sort_by(Images,&CreationDate)[-1].ImageId' \
    --output text)
echo "AMI: ${AMI_ID}"

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
        echo "Set SUBNET_ID manually: SUBNET_ID=subnet-xxx bash infra/create_gateway.sh"
        exit 1
    fi

    echo "Available public subnets:"
    echo "${SUBNETS}"
    echo ""

    SUBNET_ID=$(echo "${SUBNETS}" | head -1 | awk '{print $1}')
    echo "Using: ${SUBNET_ID}"
fi

# --- Resolve security group name to ID ---
echo "Resolving security group '${SECURITY_GROUP}'..."
SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${SECURITY_GROUP}" \
    --query 'SecurityGroups[0].GroupId' \
    --output text \
    --profile "${PROFILE}" --region "${REGION}")
if [ "${SG_ID}" = "None" ] || [ -z "${SG_ID}" ]; then
    echo "ERROR: Security group '${SECURITY_GROUP}' not found."
    echo "Create one with inbound SSH (port 22) access, or set SECURITY_GROUP before running."
    exit 1
fi
echo "Security group: ${SG_ID}"

# --- Launch the instance ---
echo ""
echo "Launching ${INSTANCE_TYPE} (4 vCPU, 16 GB RAM)..."
INSTANCE_ID=$(aws ec2 run-instances \
    --profile "${PROFILE}" --region "${REGION}" \
    --image-id "${AMI_ID}" \
    --instance-type "${INSTANCE_TYPE}" \
    --key-name "${KEY_NAME}" \
    --subnet-id "${SUBNET_ID}" \
    --security-group-ids "${SG_ID}" \
    --associate-public-ip-address \
    --iam-instance-profile "Name=${INSTANCE_PROFILE}" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":32,"VolumeType":"gp3"}}]' \
    --tag-specifications \
        "ResourceType=instance,Tags=[{Key=Name,Value=sql-demo-gateway}]" \
    --query 'Instances[0].InstanceId' \
    --output text)

echo "Instance ID: ${INSTANCE_ID}"

# --- Wait for running state ---
echo ""
echo "Waiting for instance to start..."
aws ec2 wait instance-running \
    --instance-ids "${INSTANCE_ID}" \
    --profile "${PROFILE}" --region "${REGION}"

# --- Get the public IP ---
PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids "${INSTANCE_ID}" \
    --profile "${PROFILE}" --region "${REGION}" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text)

echo ""
echo "========================================="
echo "Gateway EC2 is running"
echo "========================================="
echo "Instance ID:  ${INSTANCE_ID}"
echo "Public IP:    ${PUBLIC_IP}"
echo ""
KEY_PATH="${KEY_DIR:-\$HOME/mds}/${KEY_NAME}.pem"
echo "Step 2: SSH in, clone, and run setup:"
echo "  ssh -i ${KEY_PATH} ubuntu@${PUBLIC_IP}"
echo "  sudo apt-get update && sudo apt-get install -y git"
echo "  git clone https://github.com/ilyamusabirov/525next-steps.git"
echo "  cd 525next-steps/demos/sql-on-cluster && bash setup.sh"
echo ""
echo "Step 3: Add to ~/.ssh/config, then connect VS Code Remote SSH to 'gateway':"
echo "    Host gateway"
echo "        HostName ${PUBLIC_IP}"
echo "        User ubuntu"
echo "        IdentityFile ${KEY_PATH}"
echo ""
echo "REMEMBER to terminate when done (\$0.17/hr):"
echo "  aws ec2 terminate-instances --instance-ids ${INSTANCE_ID} --profile ${PROFILE} --region ${REGION}"
