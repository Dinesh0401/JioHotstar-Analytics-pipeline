# Phase 2: AWS Cloud Bronze Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest all Phase 1 data sources (MySQL, PostgreSQL, Kafka, CSV/JSON/Excel) into a Bronze Delta Lake layer on AWS S3 using PySpark.

**Architecture:** Single EC2 t3.medium runs Docker containers (MySQL, PostgreSQL, Kafka) and PySpark. Batch ingestion reads from JDBC/files and writes Delta tables to S3. Kafka streaming uses Spark Structured Streaming with micro-batch trigger. Environment variables switch between local (`lakehouse/`) and cloud (`s3://`) paths.

**Tech Stack:** PySpark 3.5.1, Delta Lake 3.1.0, Java 11, Hadoop-AWS 3.3.2, AWS CLI, Docker Compose, Ubuntu 22.04

**AWS Account:** 868896905478, Region: ap-south-1, IAM User: jio-hotstar

---

## File Structure

### New files to create:

| File | Responsibility |
|------|---------------|
| `.env.local` | Local environment: ports 3307/5433, lakehouse path = `lakehouse` |
| `.env.ec2` | EC2 environment: ports 3306/5432, lakehouse path = `s3://jiohotstar-lakehouse-868896905478` |
| `infra/aws/create_infrastructure.sh` | Creates S3, IAM, EC2, security group, key pair |
| `infra/aws/destroy_infrastructure.sh` | Tears down all AWS resources |
| `infra/ec2/provision_ec2.sh` | Installs Docker, Java, PySpark on EC2 |
| `spark/__init__.py` | Package init |
| `spark/conf/__init__.py` | Package init |
| `spark/conf/spark_settings.py` | SparkSession factory with env-aware config |
| `spark/bronze/__init__.py` | Package init |
| `spark/bronze/batch/__init__.py` | Package init |
| `spark/bronze/batch/ingest_mysql.py` | MySQL users + subscriptions → Bronze Delta |
| `spark/bronze/batch/ingest_postgres.py` | PostgreSQL content + ratings → Bronze Delta |
| `spark/bronze/batch/ingest_files.py` | CSV + JSON + Excel → Bronze Delta |
| `spark/bronze/streaming/__init__.py` | Package init |
| `spark/bronze/streaming/ingest_kafka_streaming.py` | Kafka → Structured Streaming → Bronze Delta |
| `spark/jobs/__init__.py` | Package init |
| `spark/jobs/run_bronze.py` | Batch orchestrator |
| `spark/jobs/validate_bronze.py` | 12 validation checks |

### Existing files to modify:

| File | Changes |
|------|---------|
| `config/settings.py` | Add env-var reads for ports, LAKEHOUSE_PATH, CHECKPOINT_PATH |
| `docker/docker-compose.yml` | Parameterize MySQL/PostgreSQL ports |
| `.gitignore` | Add lakehouse/, checkpoints/, .env, *.pem, infra/aws/ec2_config.txt |
| `requirements.txt` | Add pyspark, delta-spark, boto3 |

---

### Task 1: Environment Configuration

**Files:**
- Modify: `config/settings.py`
- Modify: `docker/docker-compose.yml`
- Modify: `.gitignore`
- Modify: `requirements.txt`
- Create: `.env.local`
- Create: `.env.ec2`

- [ ] **Step 1: Update `config/settings.py` to read from environment variables**

```python
"""Shared configuration for all data generation and Spark scripts."""
import os

# Project root (two levels up from config/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── MySQL ──
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3307"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "streaming_pass")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "streaming_users")

# ── PostgreSQL ──
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "streaming_pass")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "streaming_content")

# ── Kafka ──
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "media.viewing.live"

# ── Paths (absolute, derived from PROJECT_ROOT) ──
DATA_SOURCES_DIR = os.path.join(PROJECT_ROOT, os.getenv("DATA_SOURCES_DIR", "data_sources"))
MASTER_IDS_PATH = os.path.join(PROJECT_ROOT, "data_generation", "master_ids.json")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")

# ── Lakehouse ──
LAKEHOUSE_PATH = os.getenv("LAKEHOUSE_PATH", os.path.join(PROJECT_ROOT, "lakehouse"))
CHECKPOINT_PATH = os.path.join(LAKEHOUSE_PATH, "checkpoints") if not LAKEHOUSE_PATH.startswith("s3://") else f"{LAKEHOUSE_PATH}/checkpoints"

# ── Entity Counts ──
NUM_USERS = 5000
NUM_CONTENT = 2000
NUM_CAMPAIGNS = 50
NUM_VIEWING_EVENTS = 50000
NUM_REVIEWS = 500
NUM_RATINGS_PER_CONTENT_AVG = 8  # ~16,000 total ratings

# ── Deterministic Seed ──
SEED = 42

# ── SQLAlchemy Connection URLs ──
MYSQL_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
POSTGRES_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
```

- [ ] **Step 2: Create `.env.local`**

```
MYSQL_HOST=localhost
MYSQL_PORT=3307
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
KAFKA_BOOTSTRAP=localhost:9092
LAKEHOUSE_PATH=lakehouse
DATA_SOURCES_DIR=data_sources
```

- [ ] **Step 3: Create `.env.ec2`**

```
MYSQL_HOST=localhost
MYSQL_PORT=3306
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
KAFKA_BOOTSTRAP=localhost:9092
LAKEHOUSE_PATH=s3://jiohotstar-lakehouse-868896905478
DATA_SOURCES_DIR=data_sources
```

- [ ] **Step 4: Update `docker/docker-compose.yml` to parameterize ports**

Replace the hardcoded MySQL port line:
```yaml
    ports:
      - "${MYSQL_PORT:-3307}:3306"
```

Replace the hardcoded PostgreSQL port line:
```yaml
    ports:
      - "${POSTGRES_PORT:-5433}:5432"
```

- [ ] **Step 5: Update `.gitignore`**

Append these lines:

```
# Lakehouse (local Delta tables)
lakehouse/

# Spark streaming checkpoints
checkpoints/

# Environment files (may contain paths)
.env

# AWS key files
*.pem
infra/aws/ec2_config.txt
```

- [ ] **Step 6: Update `requirements.txt`**

Append:

```
pyspark==3.5.1
delta-spark==3.1.0
boto3>=1.34.0
```

- [ ] **Step 7: Verify Phase 1 still works with env-var config**

Run: `python validate_phase1.py`
Expected: 14/14 checks passed (existing functionality unchanged)

- [ ] **Step 8: Commit**

```bash
git add config/settings.py docker/docker-compose.yml .gitignore requirements.txt .env.local .env.ec2
git commit -m "feat: environment-variable config for local/EC2 portability"
```

---

### Task 2: AWS Infrastructure Creation Script

**Files:**
- Create: `infra/aws/create_infrastructure.sh`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p infra/aws infra/ec2
```

- [ ] **Step 2: Write `infra/aws/create_infrastructure.sh`**

```bash
#!/bin/bash
set -euo pipefail

# ── Configuration ──
REGION="ap-south-1"
ACCOUNT_ID="868896905478"
BUCKET_NAME="jiohotstar-lakehouse-${ACCOUNT_ID}"
ROLE_NAME="jiohotstar-ec2-role"
POLICY_NAME="jiohotstar-s3-access"
INSTANCE_PROFILE_NAME="jiohotstar-ec2-profile"
SG_NAME="jiohotstar-sg"
KEY_NAME="jiohotstar-key"
INSTANCE_TYPE="t3.medium"
# Ubuntu 22.04 LTS AMI for ap-south-1 (x86_64)
AMI_ID="ami-0f58b397bc5c1f2e8"
VOLUME_SIZE=30
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/ec2_config.txt"

echo "============================================"
echo "  JioHotstar AWS Infrastructure Setup"
echo "  Region: ${REGION}"
echo "  Account: ${ACCOUNT_ID}"
echo "============================================"

# ── Step 1: Create S3 Bucket ──
echo ""
echo "[1/10] Creating S3 bucket: ${BUCKET_NAME}"
if aws s3api head-bucket --bucket "${BUCKET_NAME}" 2>/dev/null; then
    echo "  Bucket already exists, skipping."
else
    aws s3api create-bucket \
        --bucket "${BUCKET_NAME}" \
        --region "${REGION}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
    echo "  Bucket created."
fi

# Create prefix structure
for prefix in bronze/ silver/ gold/ checkpoints/bronze_viewing_events/; do
    aws s3api put-object --bucket "${BUCKET_NAME}" --key "${prefix}" --region "${REGION}" > /dev/null
done
echo "  Prefix structure created (bronze/, silver/, gold/, checkpoints/)."

# ── Step 2: Create IAM Policy ──
echo ""
echo "[2/10] Creating IAM policy: ${POLICY_NAME}"
POLICY_DOC=$(cat <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET_NAME}",
        "arn:aws:s3:::${BUCKET_NAME}/*"
      ]
    }
  ]
}
POLICY
)

POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
if aws iam get-policy --policy-arn "${POLICY_ARN}" 2>/dev/null; then
    echo "  Policy already exists, skipping."
else
    aws iam create-policy \
        --policy-name "${POLICY_NAME}" \
        --policy-document "${POLICY_DOC}" > /dev/null
    echo "  Policy created."
fi

# ── Step 3: Create IAM Role ──
echo ""
echo "[3/10] Creating IAM role: ${ROLE_NAME}"
TRUST_POLICY=$(cat <<TRUST
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "ec2.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
TRUST
)

if aws iam get-role --role-name "${ROLE_NAME}" 2>/dev/null; then
    echo "  Role already exists, skipping."
else
    aws iam create-role \
        --role-name "${ROLE_NAME}" \
        --assume-role-policy-document "${TRUST_POLICY}" > /dev/null
    echo "  Role created."
fi

# ── Step 4: Attach Policy to Role ──
echo ""
echo "[4/10] Attaching policy to role"
aws iam attach-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-arn "${POLICY_ARN}" 2>/dev/null || true
echo "  Policy attached."

# ── Step 5: Create Instance Profile ──
echo ""
echo "[5/10] Creating instance profile: ${INSTANCE_PROFILE_NAME}"
if aws iam get-instance-profile --instance-profile-name "${INSTANCE_PROFILE_NAME}" 2>/dev/null; then
    echo "  Instance profile already exists, skipping."
else
    aws iam create-instance-profile \
        --instance-profile-name "${INSTANCE_PROFILE_NAME}" > /dev/null
    aws iam add-role-to-instance-profile \
        --instance-profile-name "${INSTANCE_PROFILE_NAME}" \
        --role-name "${ROLE_NAME}"
    echo "  Instance profile created and role added."
    echo "  Waiting 10s for IAM propagation..."
    sleep 10
fi

# ── Step 6: Create Security Group ──
echo ""
echo "[6/10] Creating security group: ${SG_NAME}"
MY_IP=$(curl -s https://checkip.amazonaws.com)/32

# Get default VPC
VPC_ID=$(aws ec2 describe-vpcs \
    --filters "Name=isDefault,Values=true" \
    --query "Vpcs[0].VpcId" \
    --output text \
    --region "${REGION}")

SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${SG_NAME}" "Name=vpc-id,Values=${VPC_ID}" \
    --query "SecurityGroups[0].GroupId" \
    --output text \
    --region "${REGION}" 2>/dev/null || echo "None")

if [ "${SG_ID}" != "None" ] && [ -n "${SG_ID}" ]; then
    echo "  Security group already exists: ${SG_ID}"
else
    SG_ID=$(aws ec2 create-security-group \
        --group-name "${SG_NAME}" \
        --description "JioHotstar Spark pipeline access" \
        --vpc-id "${VPC_ID}" \
        --region "${REGION}" \
        --query "GroupId" \
        --output text)

    # SSH
    aws ec2 authorize-security-group-ingress \
        --group-id "${SG_ID}" \
        --protocol tcp --port 22 \
        --cidr "${MY_IP}" \
        --region "${REGION}" > /dev/null

    # Adminer (web UI)
    aws ec2 authorize-security-group-ingress \
        --group-id "${SG_ID}" \
        --protocol tcp --port 8080 \
        --cidr "${MY_IP}" \
        --region "${REGION}" > /dev/null

    # Kafka UI
    aws ec2 authorize-security-group-ingress \
        --group-id "${SG_ID}" \
        --protocol tcp --port 8081 \
        --cidr "${MY_IP}" \
        --region "${REGION}" > /dev/null

    echo "  Security group created: ${SG_ID}"
    echo "  Allowed SSH + 8080 + 8081 from ${MY_IP}"
fi

# ── Step 7: Create Key Pair ──
echo ""
echo "[7/10] Creating key pair: ${KEY_NAME}"
KEY_FILE="${SCRIPT_DIR}/${KEY_NAME}.pem"
if aws ec2 describe-key-pairs --key-names "${KEY_NAME}" --region "${REGION}" 2>/dev/null; then
    echo "  Key pair already exists, skipping."
else
    aws ec2 create-key-pair \
        --key-name "${KEY_NAME}" \
        --query "KeyMaterial" \
        --output text \
        --region "${REGION}" > "${KEY_FILE}"
    chmod 400 "${KEY_FILE}"
    echo "  Key pair created: ${KEY_FILE}"
fi

# ── Step 8: Launch EC2 Instance ──
echo ""
echo "[8/10] Launching EC2 instance"

# Check if instance already running
EXISTING_ID=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=jiohotstar-spark" "Name=instance-state-name,Values=running,stopped" \
    --query "Reservations[0].Instances[0].InstanceId" \
    --output text \
    --region "${REGION}" 2>/dev/null || echo "None")

if [ "${EXISTING_ID}" != "None" ] && [ -n "${EXISTING_ID}" ]; then
    INSTANCE_ID="${EXISTING_ID}"
    echo "  Instance already exists: ${INSTANCE_ID}"
    # Start if stopped
    STATE=$(aws ec2 describe-instances \
        --instance-ids "${INSTANCE_ID}" \
        --query "Reservations[0].Instances[0].State.Name" \
        --output text \
        --region "${REGION}")
    if [ "${STATE}" = "stopped" ]; then
        aws ec2 start-instances --instance-ids "${INSTANCE_ID}" --region "${REGION}" > /dev/null
        echo "  Starting stopped instance..."
    fi
else
    INSTANCE_ID=$(aws ec2 run-instances \
        --image-id "${AMI_ID}" \
        --instance-type "${INSTANCE_TYPE}" \
        --key-name "${KEY_NAME}" \
        --security-group-ids "${SG_ID}" \
        --iam-instance-profile Name="${INSTANCE_PROFILE_NAME}" \
        --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":${VOLUME_SIZE},\"VolumeType\":\"gp3\"}}]" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=jiohotstar-spark},{Key=Project,Value=JioHotstar-DE-ML-AI},{Key=StopAfterHours,Value=4}]" \
        --region "${REGION}" \
        --query "Instances[0].InstanceId" \
        --output text)
    echo "  Instance launched: ${INSTANCE_ID}"
fi

# ── Step 9: Wait for Running ──
echo ""
echo "[9/10] Waiting for instance to be running..."
aws ec2 wait instance-running --instance-ids "${INSTANCE_ID}" --region "${REGION}"
echo "  Instance is running."

# ── Step 10: Get Public IP and Save Config ──
echo ""
echo "[10/10] Retrieving instance details"
PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids "${INSTANCE_ID}" \
    --query "Reservations[0].Instances[0].PublicIpAddress" \
    --output text \
    --region "${REGION}")

cat > "${CONFIG_FILE}" <<EOF
INSTANCE_ID=${INSTANCE_ID}
PUBLIC_IP=${PUBLIC_IP}
BUCKET_NAME=${BUCKET_NAME}
REGION=${REGION}
KEY_FILE=${KEY_FILE}
SG_ID=${SG_ID}
EOF

echo ""
echo "============================================"
echo "  Infrastructure Created Successfully!"
echo "============================================"
echo ""
echo "  Instance ID:  ${INSTANCE_ID}"
echo "  Public IP:    ${PUBLIC_IP}"
echo "  S3 Bucket:    s3://${BUCKET_NAME}"
echo "  Key File:     ${KEY_FILE}"
echo ""
echo "  SSH Command:"
echo "    ssh -i ${KEY_FILE} ubuntu@${PUBLIC_IP}"
echo ""
echo "  Config saved to: ${CONFIG_FILE}"
echo ""
echo "  Estimated cost: ~\$0.04/hour (t3.medium)"
echo "  REMEMBER: Stop instance when not in use!"
echo "    aws ec2 stop-instances --instance-ids ${INSTANCE_ID} --region ${REGION}"
```

- [ ] **Step 3: Make executable and commit**

```bash
chmod +x infra/aws/create_infrastructure.sh
git add infra/aws/create_infrastructure.sh
git commit -m "feat: AWS infrastructure creation script (S3, IAM, EC2)"
```

---

### Task 3: AWS Infrastructure Destruction Script

**Files:**
- Create: `infra/aws/destroy_infrastructure.sh`

- [ ] **Step 1: Write `infra/aws/destroy_infrastructure.sh`**

```bash
#!/bin/bash
set -euo pipefail

REGION="ap-south-1"
ACCOUNT_ID="868896905478"
BUCKET_NAME="jiohotstar-lakehouse-${ACCOUNT_ID}"
ROLE_NAME="jiohotstar-ec2-role"
POLICY_NAME="jiohotstar-s3-access"
INSTANCE_PROFILE_NAME="jiohotstar-ec2-profile"
SG_NAME="jiohotstar-sg"
KEY_NAME="jiohotstar-key"
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================"
echo "  JioHotstar AWS Infrastructure Teardown"
echo "============================================"
echo ""
echo "  This will destroy ALL project resources."
read -p "  Continue? (yes/no): " CONFIRM
if [ "${CONFIRM}" != "yes" ]; then
    echo "  Aborted."
    exit 0
fi

# ── Step 1: Terminate EC2 Instance ──
echo ""
echo "[1/8] Terminating EC2 instance..."
INSTANCE_ID=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=jiohotstar-spark" "Name=instance-state-name,Values=running,stopped,pending" \
    --query "Reservations[0].Instances[0].InstanceId" \
    --output text \
    --region "${REGION}" 2>/dev/null || echo "None")

if [ "${INSTANCE_ID}" != "None" ] && [ -n "${INSTANCE_ID}" ]; then
    aws ec2 terminate-instances --instance-ids "${INSTANCE_ID}" --region "${REGION}" > /dev/null
    echo "  Terminating ${INSTANCE_ID}... waiting..."
    aws ec2 wait instance-terminated --instance-ids "${INSTANCE_ID}" --region "${REGION}"
    echo "  Instance terminated."
else
    echo "  No instance found, skipping."
fi

# ── Step 2: Delete Key Pair ──
echo ""
echo "[2/8] Deleting key pair: ${KEY_NAME}"
aws ec2 delete-key-pair --key-name "${KEY_NAME}" --region "${REGION}" 2>/dev/null || true
rm -f "${SCRIPT_DIR}/${KEY_NAME}.pem"
echo "  Key pair deleted."

# ── Step 3: Delete Security Group ──
echo ""
echo "[3/8] Deleting security group: ${SG_NAME}"
SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${SG_NAME}" \
    --query "SecurityGroups[0].GroupId" \
    --output text \
    --region "${REGION}" 2>/dev/null || echo "None")

if [ "${SG_ID}" != "None" ] && [ -n "${SG_ID}" ]; then
    aws ec2 delete-security-group --group-id "${SG_ID}" --region "${REGION}" 2>/dev/null || true
    echo "  Security group deleted."
else
    echo "  Security group not found, skipping."
fi

# ── Step 4: Remove Role from Instance Profile ──
echo ""
echo "[4/8] Cleaning up instance profile"
aws iam remove-role-from-instance-profile \
    --instance-profile-name "${INSTANCE_PROFILE_NAME}" \
    --role-name "${ROLE_NAME}" 2>/dev/null || true

# ── Step 5: Delete Instance Profile ──
echo ""
echo "[5/8] Deleting instance profile: ${INSTANCE_PROFILE_NAME}"
aws iam delete-instance-profile \
    --instance-profile-name "${INSTANCE_PROFILE_NAME}" 2>/dev/null || true
echo "  Instance profile deleted."

# ── Step 6: Detach Policy from Role ──
echo ""
echo "[6/8] Detaching policy from role"
aws iam detach-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-arn "${POLICY_ARN}" 2>/dev/null || true

# ── Step 7: Delete IAM Role and Policy ──
echo ""
echo "[7/8] Deleting IAM role and policy"
aws iam delete-role --role-name "${ROLE_NAME}" 2>/dev/null || true
aws iam delete-policy --policy-arn "${POLICY_ARN}" 2>/dev/null || true
echo "  IAM role and policy deleted."

# ── Step 8: S3 Bucket ──
echo ""
echo "[8/8] S3 bucket: ${BUCKET_NAME}"
read -p "  Delete S3 bucket and ALL data? (yes/no): " DELETE_BUCKET
if [ "${DELETE_BUCKET}" = "yes" ]; then
    aws s3 rb "s3://${BUCKET_NAME}" --force --region "${REGION}" 2>/dev/null || true
    echo "  S3 bucket deleted."
else
    echo "  S3 bucket preserved."
fi

# Clean up config file
rm -f "${SCRIPT_DIR}/ec2_config.txt"

echo ""
echo "============================================"
echo "  Infrastructure Teardown Complete"
echo "============================================"
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x infra/aws/destroy_infrastructure.sh
git add infra/aws/destroy_infrastructure.sh
git commit -m "feat: AWS infrastructure destruction script"
```

---

### Task 4: EC2 Provisioning Script

**Files:**
- Create: `infra/ec2/provision_ec2.sh`

- [ ] **Step 1: Write `infra/ec2/provision_ec2.sh`**

```bash
#!/bin/bash
set -euo pipefail

echo "============================================"
echo "  JioHotstar EC2 Provisioning"
echo "============================================"

REPO_URL="https://github.com/Dinesh0401/JioHotstar-Analytics-pipeline.git"
PROJECT_DIR="$HOME/media_stream_analytics"

# ── Step 1: System Updates ──
echo ""
echo "[1/8] Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y -qq git curl unzip wget software-properties-common

# ── Step 2: Docker + Docker Compose ──
echo ""
echo "[2/8] Installing Docker..."
if command -v docker &>/dev/null; then
    echo "  Docker already installed, skipping."
else
    curl -fsSL https://get.docker.com | sudo sh
    sudo usermod -aG docker ubuntu
    echo "  Docker installed."
fi

# Verify Docker Compose plugin
if docker compose version &>/dev/null; then
    echo "  Docker Compose plugin available."
else
    echo "  ERROR: Docker Compose plugin not found."
    exit 1
fi

# ── Step 3: Java 11 ──
echo ""
echo "[3/8] Installing Java 11..."
if java -version 2>&1 | grep -q "11\."; then
    echo "  Java 11 already installed, skipping."
else
    sudo apt-get install -y -qq openjdk-11-jdk-headless
    echo "  Java 11 installed."
fi
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# ── Step 4: Python Packages ──
echo ""
echo "[4/8] Installing Python packages..."
sudo apt-get install -y -qq python3-pip python3-venv
pip3 install --quiet \
    pyspark==3.5.1 \
    delta-spark==3.1.0 \
    boto3 \
    pymysql \
    psycopg2-binary \
    kafka-python \
    numpy==1.26.4 \
    faker==28.0.0 \
    openpyxl==3.1.2 \
    Pillow==10.3.0 \
    sqlalchemy==2.0.30
echo "  Python packages installed."

# ── Step 5: AWS CLI ──
echo ""
echo "[5/8] Verifying AWS CLI..."
if command -v aws &>/dev/null; then
    echo "  AWS CLI available: $(aws --version)"
else
    curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
    unzip -q /tmp/awscliv2.zip -d /tmp
    sudo /tmp/aws/install
    rm -rf /tmp/awscliv2.zip /tmp/aws
    echo "  AWS CLI installed."
fi

# Verify IAM role access
echo "  Testing IAM role..."
aws sts get-caller-identity || { echo "ERROR: IAM role not attached to EC2"; exit 1; }

# ── Step 6: Clone Repo ──
echo ""
echo "[6/8] Cloning repository..."
if [ -d "${PROJECT_DIR}" ]; then
    cd "${PROJECT_DIR}"
    git pull
    echo "  Repository updated."
else
    git clone "${REPO_URL}" "${PROJECT_DIR}"
    cd "${PROJECT_DIR}"
    echo "  Repository cloned."
fi

# Set EC2 environment
cp .env.ec2 .env
echo "  Environment set to EC2 (.env.ec2 -> .env)"

# Start containers
cd "${PROJECT_DIR}/docker"
docker compose up -d
cd "${PROJECT_DIR}"
echo "  Docker containers started."

# ── Step 7: Verify Services ──
echo ""
echo "[7/8] Verifying services..."
echo "  Waiting for MySQL..."
for i in $(seq 1 30); do
    if python3 -c "import pymysql; pymysql.connect(host='localhost', port=3306, user='root', password='streaming_pass', database='streaming_users').close()" 2>/dev/null; then
        echo "  MySQL ready."
        break
    fi
    sleep 2
done

echo "  Waiting for PostgreSQL..."
for i in $(seq 1 30); do
    if python3 -c "import psycopg2; psycopg2.connect(host='localhost', port=5432, user='postgres', password='streaming_pass', dbname='streaming_content').close()" 2>/dev/null; then
        echo "  PostgreSQL ready."
        break
    fi
    sleep 2
done

echo "  Waiting for Kafka..."
for i in $(seq 1 30); do
    if docker exec streaming_kafka kafka-topics --list --bootstrap-server localhost:9092 &>/dev/null; then
        echo "  Kafka ready."
        break
    fi
    sleep 2
done

# Run Phase 1 data generation
echo "  Running Phase 1 data generation..."
python3 run_phase1.py
echo "  Running Phase 1 validation..."
python3 validate_phase1.py

# ── Step 8: Test S3 Connectivity ──
echo ""
echo "[8/8] Testing Spark → S3 connectivity..."
python3 -c "
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName('s3-test') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.InstanceProfileCredentialsProvider')

spark = configure_spark_with_delta_pip(builder).getOrCreate()
df = spark.createDataFrame([('test', 1)], ['name', 'value'])
df.write.format('delta').mode('overwrite').save('s3a://jiohotstar-lakehouse-868896905478/bronze/_connectivity_test/')
print('S3 write successful!')
read_df = spark.read.format('delta').load('s3a://jiohotstar-lakehouse-868896905478/bronze/_connectivity_test/')
print(f'S3 read successful: {read_df.count()} rows')
spark.stop()
"

echo ""
echo "============================================"
echo "  EC2 Provisioning Complete!"
echo "============================================"
echo ""
echo "  Next steps:"
echo "    python3 -m spark.jobs.run_bronze"
echo "    python3 -m spark.jobs.validate_bronze"
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x infra/ec2/provision_ec2.sh
git add infra/ec2/provision_ec2.sh
git commit -m "feat: EC2 provisioning script (Docker, Java, PySpark, S3 test)"
```

---

### Task 5: Spark Session Factory

**Files:**
- Create: `spark/__init__.py`
- Create: `spark/conf/__init__.py`
- Create: `spark/conf/spark_settings.py`

- [ ] **Step 1: Create package init files**

Create empty `spark/__init__.py`, `spark/conf/__init__.py`, `spark/bronze/__init__.py`, `spark/bronze/batch/__init__.py`, `spark/bronze/streaming/__init__.py`, `spark/jobs/__init__.py`.

- [ ] **Step 2: Write `spark/conf/spark_settings.py`**

```python
"""Spark session factory with environment-aware configuration."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from config.settings import (
    CHECKPOINT_PATH,
    KAFKA_BOOTSTRAP,
    LAKEHOUSE_PATH,
    MYSQL_DATABASE,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_USER,
    POSTGRES_DATABASE,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

# Maven coordinates for required JARs
SPARK_PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.1.0",
    "org.apache.hadoop:hadoop-aws:3.3.2",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "com.mysql:mysql-connector-j:8.3.0",
    "org.postgresql:postgresql:42.7.3",
])


def get_spark_session(app_name="JioHotstar-Bronze"):
    """Create a configured SparkSession for Bronze ingestion.

    Automatically detects local vs S3 mode from LAKEHOUSE_PATH env var.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", SPARK_PACKAGES) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # S3 configuration (only when writing to S3)
    if LAKEHOUSE_PATH.startswith("s3://"):
        s3a_path = LAKEHOUSE_PATH.replace("s3://", "s3a://")
        builder = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_lakehouse_path():
    """Return the lakehouse base path (local or S3)."""
    path = LAKEHOUSE_PATH
    if path.startswith("s3://"):
        return path.replace("s3://", "s3a://")
    return os.path.abspath(path)


def get_bronze_path(table_name):
    """Return the full path for a Bronze table."""
    base = get_lakehouse_path()
    return f"{base}/bronze/{table_name}"


def get_checkpoint_path(name):
    """Return the checkpoint path for a streaming job."""
    base = get_lakehouse_path()
    return f"{base}/checkpoints/{name}"


def get_mysql_jdbc_url():
    """Return JDBC URL for MySQL."""
    return f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"


def get_mysql_jdbc_props():
    """Return JDBC connection properties for MySQL."""
    return {"user": MYSQL_USER, "password": MYSQL_PASSWORD, "driver": "com.mysql.cj.jdbc.Driver"}


def get_postgres_jdbc_url():
    """Return JDBC URL for PostgreSQL."""
    return f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"


def get_postgres_jdbc_props():
    """Return JDBC connection properties for PostgreSQL."""
    return {"user": POSTGRES_USER, "password": POSTGRES_PASSWORD, "driver": "org.postgresql.Driver"}
```

- [ ] **Step 3: Commit**

```bash
git add spark/
git commit -m "feat: Spark session factory with env-aware S3/local config"
```

---

### Task 6: MySQL Batch Ingestion

**Files:**
- Create: `spark/bronze/batch/ingest_mysql.py`

- [ ] **Step 1: Write `spark/bronze/batch/ingest_mysql.py`**

```python
"""Bronze ingestion: MySQL users and subscriptions via JDBC."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import current_timestamp, lit

from spark.conf.spark_settings import (
    get_bronze_path,
    get_mysql_jdbc_props,
    get_mysql_jdbc_url,
    get_spark_session,
)


def ingest_users(spark):
    """Read users table from MySQL and write to Bronze Delta."""
    print("Ingesting MySQL users...")
    df = spark.read.jdbc(
        url=get_mysql_jdbc_url(),
        table="users",
        properties=get_mysql_jdbc_props(),
    )
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source", lit("mysql"))

    output_path = get_bronze_path("users")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/users: {count} rows written to {output_path}")
    return count


def ingest_subscriptions(spark):
    """Read subscriptions table from MySQL and write to Bronze Delta."""
    print("Ingesting MySQL subscriptions...")
    df = spark.read.jdbc(
        url=get_mysql_jdbc_url(),
        table="subscriptions",
        properties=get_mysql_jdbc_props(),
    )
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source", lit("mysql"))

    output_path = get_bronze_path("subscriptions")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/subscriptions: {count} rows written to {output_path}")
    return count


def main():
    """Run MySQL Bronze ingestion."""
    spark = get_spark_session("Bronze-MySQL")
    try:
        ingest_users(spark)
        ingest_subscriptions(spark)
        print("MySQL Bronze ingestion complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add spark/bronze/batch/ingest_mysql.py
git commit -m "feat: MySQL Bronze ingestion (users + subscriptions via JDBC)"
```

---

### Task 7: PostgreSQL Batch Ingestion

**Files:**
- Create: `spark/bronze/batch/ingest_postgres.py`

- [ ] **Step 1: Write `spark/bronze/batch/ingest_postgres.py`**

```python
"""Bronze ingestion: PostgreSQL content_catalogue and ratings via JDBC."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import current_timestamp, lit

from spark.conf.spark_settings import (
    get_bronze_path,
    get_postgres_jdbc_props,
    get_postgres_jdbc_url,
    get_spark_session,
)


def ingest_content(spark):
    """Read content_catalogue table from PostgreSQL and write to Bronze Delta."""
    print("Ingesting PostgreSQL content_catalogue...")
    df = spark.read.jdbc(
        url=get_postgres_jdbc_url(),
        table="content_catalogue",
        properties=get_postgres_jdbc_props(),
    )
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source", lit("postgres"))

    output_path = get_bronze_path("content")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/content: {count} rows written to {output_path}")
    return count


def ingest_ratings(spark):
    """Read ratings table from PostgreSQL and write to Bronze Delta."""
    print("Ingesting PostgreSQL ratings...")
    df = spark.read.jdbc(
        url=get_postgres_jdbc_url(),
        table="ratings",
        properties=get_postgres_jdbc_props(),
    )
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source", lit("postgres"))

    output_path = get_bronze_path("ratings")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/ratings: {count} rows written to {output_path}")
    return count


def main():
    """Run PostgreSQL Bronze ingestion."""
    spark = get_spark_session("Bronze-PostgreSQL")
    try:
        ingest_content(spark)
        ingest_ratings(spark)
        print("PostgreSQL Bronze ingestion complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add spark/bronze/batch/ingest_postgres.py
git commit -m "feat: PostgreSQL Bronze ingestion (content + ratings via JDBC)"
```

---

### Task 8: File Sources Batch Ingestion

**Files:**
- Create: `spark/bronze/batch/ingest_files.py`

- [ ] **Step 1: Write `spark/bronze/batch/ingest_files.py`**

```python
"""Bronze ingestion: CSV, JSON, and Excel file sources."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

import pandas as pd
from pyspark.sql.functions import current_timestamp, lit

from config.settings import DATA_SOURCES_DIR
from spark.conf.spark_settings import get_bronze_path, get_spark_session


def ingest_csv(spark):
    """Read content_metadata.csv and write to Bronze Delta."""
    print("Ingesting CSV content_metadata...")
    csv_path = os.path.join(DATA_SOURCES_DIR, "csv", "content_metadata.csv")

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source", lit("csv"))

    output_path = get_bronze_path("content_metadata")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/content_metadata: {count} rows written to {output_path}")
    return count


def ingest_json(spark):
    """Read viewing_events_batch.json and write to Bronze Delta."""
    print("Ingesting JSON viewing_events_batch...")
    json_path = os.path.join(DATA_SOURCES_DIR, "json", "viewing_events_batch.json")

    df = spark.read.option("multiLine", "true").json(json_path)
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source", lit("json"))

    output_path = get_bronze_path("viewing_events_batch")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/viewing_events_batch: {count} rows written to {output_path}")
    return count


def ingest_excel(spark):
    """Read ad_campaigns.xlsx via pandas and write to Bronze Delta."""
    print("Ingesting Excel ad_campaigns...")
    excel_path = os.path.join(DATA_SOURCES_DIR, "excel", "ad_campaigns.xlsx")

    pdf = pd.read_excel(excel_path)
    df = spark.createDataFrame(pdf)
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source", lit("excel"))

    output_path = get_bronze_path("campaigns")
    df.write.format("delta").mode("overwrite").save(output_path)
    count = df.count()
    print(f"  bronze/campaigns: {count} rows written to {output_path}")
    return count


def main():
    """Run file sources Bronze ingestion."""
    spark = get_spark_session("Bronze-Files")
    try:
        ingest_csv(spark)
        ingest_json(spark)
        ingest_excel(spark)
        print("File sources Bronze ingestion complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add spark/bronze/batch/ingest_files.py
git commit -m "feat: file sources Bronze ingestion (CSV + JSON + Excel)"
```

---

### Task 9: Kafka Streaming Ingestion

**Files:**
- Create: `spark/bronze/streaming/ingest_kafka_streaming.py`

- [ ] **Step 1: Write `spark/bronze/streaming/ingest_kafka_streaming.py`**

```python
"""Bronze ingestion: Kafka streaming events via Spark Structured Streaming."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lit,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from config.settings import KAFKA_BOOTSTRAP, KAFKA_TOPIC
from spark.conf.spark_settings import (
    get_bronze_path,
    get_checkpoint_path,
    get_spark_session,
)

# Schema matching the Kafka producer event format
VIEWING_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("content_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device_category", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("watch_duration_ms", LongType(), True),
    StructField("seek_position_ms", LongType(), True),
    StructField("event_ts", StringType(), True),
    StructField("session_start_ts", StringType(), True),
    StructField("referrer", StringType(), True),
])


def main():
    """Start Kafka → Bronze Delta streaming pipeline."""
    spark = get_spark_session("Bronze-Kafka-Streaming")

    print(f"Starting Kafka streaming from topic: {KAFKA_TOPIC}")
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP}")

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON value and extract fields
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value", "partition", "offset") \
        .select(
            from_json(col("json_value"), VIEWING_EVENT_SCHEMA).alias("data"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
        ) \
        .select("data.*", "_kafka_partition", "_kafka_offset") \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source", lit("kafka"))

    # Write to Bronze Delta with micro-batch trigger
    output_path = get_bronze_path("viewing_events")
    checkpoint_path = get_checkpoint_path("bronze_viewing_events")

    print(f"Writing to: {output_path}")
    print(f"Checkpoint: {checkpoint_path}")

    query = parsed_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="30 seconds") \
        .start(output_path)

    print("Streaming started. Press Ctrl+C to stop.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming...")
        query.stop()
        print("Streaming stopped.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add spark/bronze/streaming/ingest_kafka_streaming.py
git commit -m "feat: Kafka Bronze streaming ingestion (micro-batch → Delta)"
```

---

### Task 10: Bronze Batch Orchestrator

**Files:**
- Create: `spark/jobs/run_bronze.py`

- [ ] **Step 1: Write `spark/jobs/run_bronze.py`**

```python
"""Bronze batch orchestrator: runs all batch ingestion pipelines."""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.conf.spark_settings import get_bronze_path, get_spark_session


def main():
    """Run all batch Bronze ingestion pipelines."""
    print("=" * 60)
    print("  BRONZE BATCH INGESTION")
    print("=" * 60)

    start_time = time.time()
    spark = get_spark_session("Bronze-Orchestrator")

    try:
        # Step 1: MySQL ingestion
        print("\n[1/3] MySQL ingestion (users + subscriptions)")
        print("-" * 40)
        from spark.bronze.batch.ingest_mysql import ingest_subscriptions, ingest_users
        user_count = ingest_users(spark)
        sub_count = ingest_subscriptions(spark)

        # Step 2: PostgreSQL ingestion
        print("\n[2/3] PostgreSQL ingestion (content + ratings)")
        print("-" * 40)
        from spark.bronze.batch.ingest_postgres import ingest_content, ingest_ratings
        content_count = ingest_content(spark)
        rating_count = ingest_ratings(spark)

        # Step 3: File sources ingestion
        print("\n[3/3] File sources ingestion (CSV + JSON + Excel)")
        print("-" * 40)
        from spark.bronze.batch.ingest_files import ingest_csv, ingest_excel, ingest_json
        csv_count = ingest_csv(spark)
        json_count = ingest_json(spark)
        excel_count = ingest_excel(spark)

        elapsed = time.time() - start_time

        # Summary
        print("\n" + "=" * 60)
        print("  BRONZE BATCH INGESTION COMPLETE")
        print("=" * 60)
        print(f"\n  Time: {elapsed:.1f}s")
        print(f"\n  Bronze tables written:")
        print(f"    users:                {user_count:>6} rows")
        print(f"    subscriptions:        {sub_count:>6} rows")
        print(f"    content:              {content_count:>6} rows")
        print(f"    ratings:              {rating_count:>6} rows")
        print(f"    content_metadata:     {csv_count:>6} rows")
        print(f"    viewing_events_batch: {json_count:>6} rows")
        print(f"    campaigns:            {excel_count:>6} rows")
        print(f"\n  Lakehouse path: {get_bronze_path('')}")
        print(f"\n  Next: python -m spark.jobs.validate_bronze")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add spark/jobs/run_bronze.py
git commit -m "feat: Bronze batch orchestrator"
```

---

### Task 11: Bronze Validation Script

**Files:**
- Create: `spark/jobs/validate_bronze.py`

- [ ] **Step 1: Write `spark/jobs/validate_bronze.py`**

```python
"""Validation script for Bronze Delta Lake tables."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.conf.spark_settings import get_bronze_path, get_lakehouse_path, get_spark_session


class BronzeValidation:
    """Collect and report Bronze validation checks."""

    def __init__(self):
        self.results = []

    def check(self, name, condition, detail=""):
        status = "PASS" if condition else "FAIL"
        self.results.append((name, status, detail))
        marker = "+" if condition else "X"
        print(f"  [{marker}] {name}: {detail}")

    def summary(self):
        passed = sum(1 for _, s, _ in self.results if s == "PASS")
        total = len(self.results)
        print(f"\n{'=' * 50}")
        print(f"  BRONZE VALIDATION: {passed}/{total} checks passed")
        print(f"{'=' * 50}")
        if passed < total:
            print("\n  Failed checks:")
            for name, status, detail in self.results:
                if status == "FAIL":
                    print(f"    - {name}: {detail}")
        return passed == total


def main():
    """Run all Bronze validation checks."""
    spark = get_spark_session("Bronze-Validation")
    v = BronzeValidation()

    # Table definitions: (name, min_rows, max_rows, source_tag)
    batch_tables = [
        ("users", 5000, 5000, "mysql"),
        ("subscriptions", 6000, 8000, "mysql"),
        ("content", 2000, 2000, "postgres"),
        ("ratings", 15000, 18000, "postgres"),
        ("content_metadata", 2000, 2000, "csv"),
        ("viewing_events_batch", 50000, 50000, "json"),
        ("campaigns", 50, 50, "excel"),
    ]

    print("\n--- Batch Bronze Tables ---")
    for table_name, min_rows, max_rows, source_tag in batch_tables:
        path = get_bronze_path(table_name)
        try:
            df = spark.read.format("delta").load(path)
            count = df.count()
            v.check(
                f"bronze/{table_name} row count",
                min_rows <= count <= max_rows,
                f"{count} rows (expected {min_rows}-{max_rows})",
            )
        except Exception as e:
            v.check(f"bronze/{table_name} row count", False, f"Error: {e}")

    print("\n--- Streaming Bronze Table ---")
    viewing_events_path = get_bronze_path("viewing_events")
    try:
        df = spark.read.format("delta").load(viewing_events_path)
        v.check("bronze/viewing_events exists", True, f"{df.count()} rows")
    except Exception:
        v.check("bronze/viewing_events exists", False, "Table not found (run Kafka streaming first)")

    print("\n--- Audit Columns ---")
    for table_name, _, _, source_tag in batch_tables:
        path = get_bronze_path(table_name)
        try:
            df = spark.read.format("delta").load(path)
            has_ingested = "_ingested_at" in df.columns
            has_source = "_source" in df.columns
            v.check(
                f"bronze/{table_name} has _ingested_at",
                has_ingested,
                "present" if has_ingested else "MISSING",
            )
            if has_source:
                actual_source = df.select("_source").first()[0]
                v.check(
                    f"bronze/{table_name} _source tag",
                    actual_source == source_tag,
                    f"'{actual_source}' (expected '{source_tag}')",
                )
        except Exception:
            pass  # Already reported above

    print("\n--- Delta Log ---")
    lakehouse = get_lakehouse_path()
    if lakehouse.startswith("s3a://"):
        # S3 validation
        import boto3
        s3 = boto3.client("s3")
        bucket = lakehouse.replace("s3a://", "").split("/")[0]
        for table_name, _, _, _ in batch_tables:
            prefix = f"bronze/{table_name}/_delta_log/"
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            has_log = resp.get("KeyCount", 0) > 0
            v.check(f"bronze/{table_name} _delta_log", has_log, "exists" if has_log else "MISSING")
    else:
        # Local validation
        for table_name, _, _, _ in batch_tables:
            log_path = os.path.join(lakehouse, "bronze", table_name, "_delta_log")
            has_log = os.path.isdir(log_path)
            v.check(f"bronze/{table_name} _delta_log", has_log, "exists" if has_log else "MISSING")

    all_passed = v.summary()
    spark.stop()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add spark/jobs/validate_bronze.py
git commit -m "feat: Bronze validation script (12 checks)"
```

---

### Task 12: Local Testing

**Files:** None new — uses existing files

- [ ] **Step 1: Create local lakehouse directory**

```bash
mkdir -p lakehouse/bronze lakehouse/silver lakehouse/gold lakehouse/checkpoints/bronze_viewing_events
```

- [ ] **Step 2: Ensure Phase 1 containers are running**

```bash
docker ps
```

Expected: streaming_mysql, streaming_postgres, streaming_kafka, streaming_zookeeper all running.

If not running: `python run_phase1.py`

- [ ] **Step 3: Run Bronze batch ingestion locally**

```bash
python -m spark.jobs.run_bronze
```

Expected output:
```
BRONZE BATCH INGESTION
[1/3] MySQL ingestion (users + subscriptions)
  bronze/users: 5000 rows
  bronze/subscriptions: 6975 rows
[2/3] PostgreSQL ingestion (content + ratings)
  bronze/content: 2000 rows
  bronze/ratings: 16800 rows
[3/3] File sources ingestion (CSV + JSON + Excel)
  bronze/content_metadata: 2000 rows
  bronze/viewing_events_batch: 50000 rows
  bronze/campaigns: 50 rows
BRONZE BATCH INGESTION COMPLETE
```

- [ ] **Step 4: Validate Bronze tables locally**

```bash
python -m spark.jobs.validate_bronze
```

Expected: All batch checks pass. Streaming check may show "not found" (expected — Kafka streaming not run yet).

- [ ] **Step 5: Test Kafka streaming locally**

Terminal 1 — start Kafka producer:
```bash
python -m data_generation.kafka_producer --mode batch
```

Terminal 2 — start streaming ingestion (run for ~30 seconds then Ctrl+C):
```bash
python -m spark.bronze.streaming.ingest_kafka_streaming
```

- [ ] **Step 6: Re-validate with streaming table**

```bash
python -m spark.jobs.validate_bronze
```

Expected: All checks pass including bronze/viewing_events.

- [ ] **Step 7: Commit any fixes**

```bash
git add -A
git commit -m "test: local Bronze pipeline verified"
```

---

### Task 13: Push to GitHub and EC2 Deployment

**Files:** None new — deployment workflow

- [ ] **Step 1: Push all Phase 2 code to GitHub**

```bash
git push origin main
```

- [ ] **Step 2: Create AWS infrastructure from laptop**

```bash
bash infra/aws/create_infrastructure.sh
```

Expected: S3 bucket, IAM role, EC2 instance created. SSH command printed.

- [ ] **Step 3: SSH into EC2**

```bash
ssh -i infra/aws/jiohotstar-key.pem ubuntu@<PUBLIC_IP>
```

(Get PUBLIC_IP from `infra/aws/ec2_config.txt`)

- [ ] **Step 4: Run provisioning script on EC2**

```bash
bash media_stream_analytics/infra/ec2/provision_ec2.sh
```

Expected: Docker, Java, PySpark installed. Phase 1 data generated. S3 connectivity tested.

- [ ] **Step 5: Run Bronze batch ingestion on EC2**

```bash
cd ~/media_stream_analytics
python3 -m spark.jobs.run_bronze
```

Expected: All 7 batch tables written to S3.

- [ ] **Step 6: Validate Bronze on EC2**

```bash
python3 -m spark.jobs.validate_bronze
```

Expected: All batch checks pass.

- [ ] **Step 7: Run Kafka streaming on EC2**

Terminal 1:
```bash
python3 -m data_generation.kafka_producer --mode batch
```

Terminal 2:
```bash
python3 -m spark.bronze.streaming.ingest_kafka_streaming
```

Wait ~60 seconds, then Ctrl+C.

- [ ] **Step 8: Verify S3 Bronze tables**

```bash
aws s3 ls s3://jiohotstar-lakehouse-868896905478/bronze/ --recursive | head -30
```

Expected: Delta files and `_delta_log/` directories for all 8 tables.

- [ ] **Step 9: Final validation on EC2**

```bash
python3 -m spark.jobs.validate_bronze
```

Expected: ALL checks pass including streaming table.

- [ ] **Step 10: STOP EC2 to save credits**

```bash
aws ec2 stop-instances --instance-ids <INSTANCE_ID> --region ap-south-1
```

---

## Summary

| Task | Description | Commit |
|------|-------------|--------|
| 1 | Environment configuration (.env, settings.py, docker-compose) | `feat: environment-variable config` |
| 2 | AWS create_infrastructure.sh | `feat: AWS infrastructure creation script` |
| 3 | AWS destroy_infrastructure.sh | `feat: AWS infrastructure destruction script` |
| 4 | EC2 provision_ec2.sh | `feat: EC2 provisioning script` |
| 5 | Spark session factory | `feat: Spark session factory` |
| 6 | MySQL batch ingestion | `feat: MySQL Bronze ingestion` |
| 7 | PostgreSQL batch ingestion | `feat: PostgreSQL Bronze ingestion` |
| 8 | File sources batch ingestion | `feat: file sources Bronze ingestion` |
| 9 | Kafka streaming ingestion | `feat: Kafka Bronze streaming ingestion` |
| 10 | Bronze batch orchestrator | `feat: Bronze batch orchestrator` |
| 11 | Bronze validation script | `feat: Bronze validation script` |
| 12 | Local testing | `test: local Bronze pipeline verified` |
| 13 | GitHub push + EC2 deployment | (deployment, no commit) |
