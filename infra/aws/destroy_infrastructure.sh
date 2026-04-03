#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# JioHotStar – AWS Infrastructure Teardown
# Region : ap-south-1
# Account: 868896905478
###############################################################################

REGION="ap-south-1"
ACCOUNT_ID="868896905478"
BUCKET_NAME="jiohotstar-lakehouse-${ACCOUNT_ID}"
ROLE_NAME="jiohotstar-ec2-role"
POLICY_NAME="jiohotstar-s3-access"
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
INSTANCE_PROFILE_NAME="jiohotstar-ec2-profile"
SECURITY_GROUP_NAME="jiohotstar-sg"
KEY_PAIR_NAME="jiohotstar-key"
TAG_NAME="jiohotstar-spark"

echo "============================================="
echo "  AWS Infrastructure Teardown"
echo "  Region : ${REGION}"
echo "  Account: ${ACCOUNT_ID}"
echo "============================================="
echo ""
read -p "This will destroy ALL JioHotStar infrastructure. Continue? (yes/no): " CONFIRM
if [[ "${CONFIRM}" != "yes" ]]; then
    echo "Aborted."
    exit 0
fi
echo ""

# -------------------------------------------------------
# [1/8] Terminate EC2 instance
# -------------------------------------------------------
echo "[1/8] Terminating EC2 instance (Name=${TAG_NAME})..."
INSTANCE_ID=$(aws ec2 describe-instances \
    --region "${REGION}" \
    --filters "Name=tag:Name,Values=${TAG_NAME}" "Name=instance-state-name,Values=running,stopped,pending,stopping" \
    --query "Reservations[*].Instances[*].InstanceId" \
    --output text 2>/dev/null || true)

if [[ -n "${INSTANCE_ID}" ]]; then
    echo "  Found instance: ${INSTANCE_ID}"
    aws ec2 terminate-instances --region "${REGION}" --instance-ids "${INSTANCE_ID}" 2>/dev/null || true
    echo "  Waiting for instance to terminate..."
    aws ec2 wait instance-terminated --region "${REGION}" --instance-ids "${INSTANCE_ID}" 2>/dev/null || true
    echo "  Instance terminated."
else
    echo "  No running instance found. Skipping."
fi
echo ""

# -------------------------------------------------------
# [2/8] Delete key pair and local .pem file
# -------------------------------------------------------
echo "[2/8] Deleting key pair (${KEY_PAIR_NAME})..."
aws ec2 delete-key-pair --region "${REGION}" --key-name "${KEY_PAIR_NAME}" 2>/dev/null || true
if [[ -f "${KEY_PAIR_NAME}.pem" ]]; then
    rm -f "${KEY_PAIR_NAME}.pem"
    echo "  Removed local ${KEY_PAIR_NAME}.pem file."
fi
echo "  Key pair deleted."
echo ""

# -------------------------------------------------------
# [3/8] Delete security group
# -------------------------------------------------------
echo "[3/8] Deleting security group (${SECURITY_GROUP_NAME})..."
SG_ID=$(aws ec2 describe-security-groups \
    --region "${REGION}" \
    --filters "Name=group-name,Values=${SECURITY_GROUP_NAME}" \
    --query "SecurityGroups[0].GroupId" \
    --output text 2>/dev/null || true)

if [[ -n "${SG_ID}" && "${SG_ID}" != "None" ]]; then
    aws ec2 delete-security-group --region "${REGION}" --group-id "${SG_ID}" 2>/dev/null || true
    echo "  Security group ${SG_ID} deleted."
else
    echo "  Security group not found. Skipping."
fi
echo ""

# -------------------------------------------------------
# [4/8] Remove role from instance profile
# -------------------------------------------------------
echo "[4/8] Removing role (${ROLE_NAME}) from instance profile (${INSTANCE_PROFILE_NAME})..."
aws iam remove-role-from-instance-profile \
    --instance-profile-name "${INSTANCE_PROFILE_NAME}" \
    --role-name "${ROLE_NAME}" 2>/dev/null || true
echo "  Done."
echo ""

# -------------------------------------------------------
# [5/8] Delete instance profile
# -------------------------------------------------------
echo "[5/8] Deleting instance profile (${INSTANCE_PROFILE_NAME})..."
aws iam delete-instance-profile \
    --instance-profile-name "${INSTANCE_PROFILE_NAME}" 2>/dev/null || true
echo "  Done."
echo ""

# -------------------------------------------------------
# [6/8] Detach policy from role
# -------------------------------------------------------
echo "[6/8] Detaching policy (${POLICY_NAME}) from role (${ROLE_NAME})..."
aws iam detach-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-arn "${POLICY_ARN}" 2>/dev/null || true
echo "  Done."
echo ""

# -------------------------------------------------------
# [7/8] Delete IAM role and policy
# -------------------------------------------------------
echo "[7/8] Deleting IAM role (${ROLE_NAME}) and policy (${POLICY_NAME})..."
aws iam delete-role --role-name "${ROLE_NAME}" 2>/dev/null || true
echo "  Role deleted."
aws iam delete-policy --policy-arn "${POLICY_ARN}" 2>/dev/null || true
echo "  Policy deleted."
echo ""

# -------------------------------------------------------
# [8/8] Delete S3 bucket
# -------------------------------------------------------
echo "[8/8] S3 bucket: ${BUCKET_NAME}"
read -p "  Delete S3 bucket and ALL its contents? (yes/no): " CONFIRM_S3
if [[ "${CONFIRM_S3}" == "yes" ]]; then
    echo "  Emptying bucket..."
    aws s3 rm "s3://${BUCKET_NAME}" --recursive --region "${REGION}" 2>/dev/null || true
    echo "  Deleting bucket..."
    aws s3api delete-bucket --bucket "${BUCKET_NAME}" --region "${REGION}" 2>/dev/null || true
    echo "  Bucket deleted."
else
    echo "  Skipped S3 bucket deletion."
fi
echo ""

# -------------------------------------------------------
# Cleanup local config
# -------------------------------------------------------
if [[ -f "ec2_config.txt" ]]; then
    rm -f ec2_config.txt
    echo "Removed ec2_config.txt"
fi

echo ""
echo "============================================="
echo "  Infrastructure Teardown Complete"
echo "============================================="
