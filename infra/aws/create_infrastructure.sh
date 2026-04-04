#!/usr/bin/env bash
# ---------------------------------------------------------------------------
#  JioHotstar Media Stream Analytics - AWS Infrastructure Setup
#  Creates S3 bucket, IAM role/policy, security group, key pair, and EC2
#  instance for the Spark-based lakehouse pipeline.
# ---------------------------------------------------------------------------
set -euo pipefail

REGION="ap-south-1"
ACCOUNT_ID="868896905478"
BUCKET_NAME="jiohotstar-lakehouse-${ACCOUNT_ID}"
ROLE_NAME="jiohotstar-ec2-role"
POLICY_NAME="jiohotstar-s3-access"
INSTANCE_PROFILE_NAME="jiohotstar-ec2-profile"
SG_NAME="jiohotstar-sg"
KEY_NAME="jiohotstar-key"
AMI_ID="ami-0f58b397bc5c1f2e8"          # Ubuntu 22.04 LTS in ap-south-1
INSTANCE_TYPE="m7i-flex.large"
VOLUME_SIZE=30
VOLUME_TYPE="gp3"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEY_FILE="${SCRIPT_DIR}/${KEY_NAME}.pem"
CONFIG_FILE="${SCRIPT_DIR}/ec2_config.txt"

TAGS="Key=Name,Value=jiohotstar-spark Key=Project,Value=JioHotstar-DE-ML-AI Key=StopAfterHours,Value=4"

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
info() { echo -e "\n\033[1;34m$1\033[0m"; }

# ---------------------------------------------------------------------------
# [1/10] Detect caller IP for security-group ingress
# ---------------------------------------------------------------------------
info "[1/10] Detecting your public IP address..."
MY_IP=$(curl -s https://checkip.amazonaws.com | tr -d '[:space:]')
echo "  Public IP: ${MY_IP}"

# ---------------------------------------------------------------------------
# [2/10] Create S3 bucket
# ---------------------------------------------------------------------------
info "[2/10] Creating S3 bucket: ${BUCKET_NAME}..."
if aws s3api head-bucket --bucket "${BUCKET_NAME}" --region "${REGION}" 2>/dev/null; then
    echo "  Bucket already exists - skipping."
else
    aws s3api create-bucket \
        --bucket "${BUCKET_NAME}" \
        --region "${REGION}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
    echo "  Bucket created."
fi

# ---------------------------------------------------------------------------
# [3/10] Create S3 prefix structure
# ---------------------------------------------------------------------------
info "[3/10] Creating S3 prefix structure..."
for PREFIX in bronze/ silver/ gold/ checkpoints/bronze_viewing_events/; do
    aws s3api put-object \
        --bucket "${BUCKET_NAME}" \
        --key "${PREFIX}" \
        --region "${REGION}" > /dev/null
    echo "  s3://${BUCKET_NAME}/${PREFIX}"
done

# ---------------------------------------------------------------------------
# [4/10] Create IAM role for EC2
# ---------------------------------------------------------------------------
info "[4/10] Creating IAM role: ${ROLE_NAME}..."
TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "ec2.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}'

if aws iam get-role --role-name "${ROLE_NAME}" 2>/dev/null; then
    echo "  Role already exists - skipping."
else
    aws iam create-role \
        --role-name "${ROLE_NAME}" \
        --assume-role-policy-document "${TRUST_POLICY}" \
        --tags Key=Project,Value=JioHotstar-DE-ML-AI
    echo "  Role created."
fi

# ---------------------------------------------------------------------------
# [5/10] Attach S3 access policy to role
# ---------------------------------------------------------------------------
info "[5/10] Attaching S3 access policy: ${POLICY_NAME}..."
S3_POLICY=$(cat <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::${BUCKET_NAME}/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${BUCKET_NAME}"
    }
  ]
}
POLICY
)

POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
if aws iam get-policy --policy-arn "${POLICY_ARN}" 2>/dev/null; then
    echo "  Policy already exists - skipping creation."
else
    aws iam create-policy \
        --policy-name "${POLICY_NAME}" \
        --policy-document "${S3_POLICY}"
    echo "  Policy created."
fi

aws iam attach-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-arn "${POLICY_ARN}" 2>/dev/null || true
echo "  Policy attached to role."

# ---------------------------------------------------------------------------
# [6/10] Create instance profile and add role
# ---------------------------------------------------------------------------
info "[6/10] Creating instance profile: ${INSTANCE_PROFILE_NAME}..."
if aws iam get-instance-profile --instance-profile-name "${INSTANCE_PROFILE_NAME}" 2>/dev/null; then
    echo "  Instance profile already exists - skipping."
else
    aws iam create-instance-profile \
        --instance-profile-name "${INSTANCE_PROFILE_NAME}"
    aws iam add-role-to-instance-profile \
        --instance-profile-name "${INSTANCE_PROFILE_NAME}" \
        --role-name "${ROLE_NAME}"
    echo "  Instance profile created and role attached."
    echo "  Waiting 10s for IAM propagation..."
    sleep 10
fi

# ---------------------------------------------------------------------------
# [7/10] Create security group
# ---------------------------------------------------------------------------
info "[7/10] Creating security group: ${SG_NAME}..."
VPC_ID=$(aws ec2 describe-vpcs \
    --region "${REGION}" \
    --filters Name=is-default,Values=true \
    --query 'Vpcs[0].VpcId' --output text)

SG_ID=$(aws ec2 describe-security-groups \
    --region "${REGION}" \
    --filters Name=group-name,Values="${SG_NAME}" Name=vpc-id,Values="${VPC_ID}" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "None")

if [[ "${SG_ID}" != "None" && -n "${SG_ID}" ]]; then
    echo "  Security group already exists: ${SG_ID} - skipping."
else
    SG_ID=$(aws ec2 create-security-group \
        --group-name "${SG_NAME}" \
        --description "JioHotstar Spark cluster access" \
        --vpc-id "${VPC_ID}" \
        --region "${REGION}" \
        --query 'GroupId' --output text)
    echo "  Security group created: ${SG_ID}"

    for PORT in 22 8080 8081; do
        aws ec2 authorize-security-group-ingress \
            --group-id "${SG_ID}" \
            --protocol tcp \
            --port "${PORT}" \
            --cidr "${MY_IP}/32" \
            --region "${REGION}"
        echo "  Opened port ${PORT} for ${MY_IP}/32"
    done

    aws ec2 create-tags \
        --resources "${SG_ID}" \
        --tags Key=Name,Value="${SG_NAME}" Key=Project,Value=JioHotstar-DE-ML-AI \
        --region "${REGION}"
fi

# ---------------------------------------------------------------------------
# [8/10] Create key pair
# ---------------------------------------------------------------------------
info "[8/10] Creating key pair: ${KEY_NAME}..."
if aws ec2 describe-key-pairs --key-names "${KEY_NAME}" --region "${REGION}" 2>/dev/null; then
    echo "  Key pair already exists - skipping."
    if [[ ! -f "${KEY_FILE}" ]]; then
        echo "  WARNING: ${KEY_FILE} not found locally. You may need to recreate the key pair."
    fi
else
    aws ec2 create-key-pair \
        --key-name "${KEY_NAME}" \
        --region "${REGION}" \
        --query 'KeyMaterial' --output text > "${KEY_FILE}"
    chmod 400 "${KEY_FILE}"
    echo "  Key pair created and saved to ${KEY_FILE}"
fi

# ---------------------------------------------------------------------------
# [9/10] Launch EC2 instance
# ---------------------------------------------------------------------------
info "[9/10] Launching EC2 instance (${INSTANCE_TYPE}, ${AMI_ID})..."

EXISTING_INSTANCE=$(aws ec2 describe-instances \
    --region "${REGION}" \
    --filters \
        "Name=tag:Name,Values=jiohotstar-spark" \
        "Name=instance-state-name,Values=running,stopped,pending" \
    --query 'Reservations[0].Instances[0].InstanceId' --output text 2>/dev/null || echo "None")

if [[ "${EXISTING_INSTANCE}" != "None" && -n "${EXISTING_INSTANCE}" ]]; then
    INSTANCE_ID="${EXISTING_INSTANCE}"
    echo "  Instance already exists: ${INSTANCE_ID} - skipping launch."
else
    INSTANCE_ID=$(MSYS_NO_PATHCONV=1 aws ec2 run-instances \
        --image-id "${AMI_ID}" \
        --instance-type "${INSTANCE_TYPE}" \
        --key-name "${KEY_NAME}" \
        --security-group-ids "${SG_ID}" \
        --iam-instance-profile Name="${INSTANCE_PROFILE_NAME}" \
        --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=${VOLUME_SIZE},VolumeType=${VOLUME_TYPE}}" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=jiohotstar-spark},{Key=Project,Value=JioHotstar-DE-ML-AI},{Key=StopAfterHours,Value=4}]" \
        --region "${REGION}" \
        --query 'Instances[0].InstanceId' --output text)
    echo "  Instance launched: ${INSTANCE_ID}"
fi

echo "  Waiting for instance to enter running state..."
aws ec2 wait instance-running --instance-ids "${INSTANCE_ID}" --region "${REGION}"

PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids "${INSTANCE_ID}" \
    --region "${REGION}" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "  Instance running. Public IP: ${PUBLIC_IP}"

# ---------------------------------------------------------------------------
# [10/10] Save configuration and print summary
# ---------------------------------------------------------------------------
info "[10/10] Saving configuration to ${CONFIG_FILE}..."
cat > "${CONFIG_FILE}" <<EOF
INSTANCE_ID=${INSTANCE_ID}
PUBLIC_IP=${PUBLIC_IP}
BUCKET_NAME=${BUCKET_NAME}
REGION=${REGION}
KEY_FILE=${KEY_FILE}
SG_ID=${SG_ID}
EOF
echo "  Config saved."

echo ""
echo "============================================================"
echo "  Infrastructure ready!"
echo "============================================================"
echo ""
echo "  SSH command:"
echo "    ssh -i ${KEY_FILE} ubuntu@${PUBLIC_IP}"
echo ""
echo "  S3 bucket:"
echo "    s3://${BUCKET_NAME}"
echo ""
echo "  COST REMINDER:"
echo "    t3.medium in ap-south-1 ~ \$0.0456/hr"
echo "    30 GB gp3 EBS ~ \$0.096/day"
echo "    Tag StopAfterHours=4 is set - configure a Lambda or"
echo "    EventBridge rule to auto-stop after 4 hours to save costs."
echo ""
echo "============================================================"
