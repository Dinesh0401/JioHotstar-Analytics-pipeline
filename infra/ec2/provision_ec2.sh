#!/usr/bin/env bash
# ==============================================================================
# EC2 Provisioning Script — Media Stream Analytics
# Transforms bare Ubuntu 22.04 into a working Spark + Docker environment.
# Run ON the EC2 instance after SSH-ing in.
# ==============================================================================
set -euo pipefail

REPO_URL="https://github.com/Dinesh0401/JioHotstar-Analytics-pipeline.git"
PROJECT_DIR="$HOME/media_stream_analytics"

banner() {
  echo ""
  echo "============================================================"
  echo "  $1"
  echo "============================================================"
  echo ""
}

# --------------------------------------------------------------------------
# [1/8] System updates and base packages
# --------------------------------------------------------------------------
banner "[1/8] System updates and base packages"

REQUIRED_PKGS=(git curl unzip wget software-properties-common)
MISSING_PKGS=()
for pkg in "${REQUIRED_PKGS[@]}"; do
  if ! dpkg -s "$pkg" &>/dev/null; then
    MISSING_PKGS+=("$pkg")
  fi
done

if [ ${#MISSING_PKGS[@]} -gt 0 ]; then
  echo "Installing missing packages: ${MISSING_PKGS[*]}"
  sudo apt-get update -qq
  sudo apt-get install -y -qq "${MISSING_PKGS[@]}"
else
  echo "All base packages already installed. Skipping."
fi

# --------------------------------------------------------------------------
# [2/8] Docker + Docker Compose
# --------------------------------------------------------------------------
banner "[2/8] Docker + Docker Compose"

if command -v docker &>/dev/null; then
  echo "Docker already installed: $(docker --version)"
else
  echo "Installing Docker via get.docker.com ..."
  curl -fsSL https://get.docker.com | sudo sh
  echo "Docker installed: $(docker --version)"
fi

# Add ubuntu user to docker group (idempotent)
if id -nG ubuntu 2>/dev/null | grep -qw docker; then
  echo "User 'ubuntu' already in docker group."
else
  sudo usermod -aG docker ubuntu
  echo "Added 'ubuntu' to docker group. Group change takes effect on next login."
fi

# Verify Docker Compose plugin
if docker compose version &>/dev/null; then
  echo "Docker Compose plugin available: $(docker compose version)"
else
  echo "ERROR: Docker Compose plugin not found. Please verify Docker installation."
  exit 1
fi

# --------------------------------------------------------------------------
# [3/8] Java 11
# --------------------------------------------------------------------------
banner "[3/8] Java 11 (OpenJDK headless)"

if java -version 2>&1 | grep -q "11\."; then
  echo "Java 11 already installed."
else
  echo "Installing OpenJDK 11 headless ..."
  sudo apt-get install -y -qq openjdk-11-jdk-headless
  echo "Java installed: $(java -version 2>&1 | head -1)"
fi

export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Persist JAVA_HOME if not already in profile
if ! grep -q 'JAVA_HOME.*java-11' "$HOME/.bashrc" 2>/dev/null; then
  cat >> "$HOME/.bashrc" <<'JAVA_EOF'

# Java 11 (provisioned by provision_ec2.sh)
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
export PATH="$JAVA_HOME/bin:$PATH"
JAVA_EOF
  echo "JAVA_HOME written to ~/.bashrc"
else
  echo "JAVA_HOME already in ~/.bashrc"
fi

# --------------------------------------------------------------------------
# [4/8] Python packages via pip3
# --------------------------------------------------------------------------
banner "[4/8] Python packages via pip3"

# Ensure pip is available
if ! command -v pip3 &>/dev/null; then
  echo "Installing python3-pip ..."
  sudo apt-get install -y -qq python3-pip
fi

PIP_PACKAGES=(
  "pyspark==3.5.1"
  "delta-spark==3.1.0"
  "boto3"
  "pymysql"
  "psycopg2-binary"
  "kafka-python"
  "numpy==1.26.4"
  "faker==28.0.0"
  "openpyxl==3.1.2"
  "Pillow==10.3.0"
  "sqlalchemy==2.0.30"
)

# Check if all packages are already installed by testing the first and last
if python3 -c "import pyspark, delta, boto3, pymysql, psycopg2, kafka, numpy, faker, openpyxl, PIL, sqlalchemy" 2>/dev/null; then
  echo "All Python packages already installed. Skipping."
else
  echo "Installing Python packages ..."
  pip3 install --quiet --break-system-packages "${PIP_PACKAGES[@]}" 2>/dev/null \
    || pip3 install --quiet "${PIP_PACKAGES[@]}"
  echo "Python packages installed."
fi

# --------------------------------------------------------------------------
# [5/8] AWS CLI
# --------------------------------------------------------------------------
banner "[5/8] AWS CLI"

if command -v aws &>/dev/null; then
  echo "AWS CLI already installed: $(aws --version)"
else
  echo "Installing AWS CLI v2 ..."
  TMPDIR=$(mktemp -d)
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "$TMPDIR/awscliv2.zip"
  unzip -q "$TMPDIR/awscliv2.zip" -d "$TMPDIR"
  sudo "$TMPDIR/aws/install"
  rm -rf "$TMPDIR"
  echo "AWS CLI installed: $(aws --version)"
fi

# --------------------------------------------------------------------------
# [6/8] Clone repo and start Docker services
# --------------------------------------------------------------------------
banner "[6/8] Clone repo and start Docker services"

if [ -d "$PROJECT_DIR/.git" ]; then
  echo "Repository already cloned. Pulling latest ..."
  git -C "$PROJECT_DIR" pull --ff-only || echo "Pull failed (maybe local changes); continuing."
else
  echo "Cloning repository ..."
  git clone "$REPO_URL" "$PROJECT_DIR"
fi

cd "$PROJECT_DIR"

# Copy EC2-specific env if available
if [ -f ".env.ec2" ]; then
  cp .env.ec2 .env
  echo "Copied .env.ec2 -> .env"
elif [ -f ".env" ]; then
  echo ".env already exists; .env.ec2 not found. Using existing .env"
else
  echo "WARNING: Neither .env.ec2 nor .env found. Services may use defaults."
fi

echo "Starting Docker Compose services ..."
docker compose -f docker/docker-compose.yml up -d

# --------------------------------------------------------------------------
# [7/8] Verify services and run Phase 1
# --------------------------------------------------------------------------
banner "[7/8] Verify services and run Phase 1"

# --- MySQL readiness ---
echo "Waiting for MySQL (port 3306) ..."
MAX_RETRIES=30
RETRY=0
until docker exec streaming_mysql mysqladmin ping -h localhost -u root -pstreaming_pass --silent 2>/dev/null; do
  RETRY=$((RETRY + 1))
  if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
    echo "ERROR: MySQL did not become ready in time."
    exit 1
  fi
  echo "  MySQL not ready yet (attempt $RETRY/$MAX_RETRIES) ..."
  sleep 3
done
echo "MySQL is ready."

# --- PostgreSQL readiness ---
echo "Waiting for PostgreSQL (port 5432) ..."
RETRY=0
until docker exec streaming_postgres pg_isready -U postgres --silent 2>/dev/null; do
  RETRY=$((RETRY + 1))
  if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
    echo "ERROR: PostgreSQL did not become ready in time."
    exit 1
  fi
  echo "  PostgreSQL not ready yet (attempt $RETRY/$MAX_RETRIES) ..."
  sleep 3
done
echo "PostgreSQL is ready."

# --- Kafka readiness ---
echo "Waiting for Kafka (port 9092) ..."
RETRY=0
until docker exec streaming_kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; do
  RETRY=$((RETRY + 1))
  if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
    echo "ERROR: Kafka did not become ready in time."
    exit 1
  fi
  echo "  Kafka not ready yet (attempt $RETRY/$MAX_RETRIES) ..."
  sleep 3
done
echo "Kafka is ready."

echo ""
echo "All services are up. Running Phase 1 data generation ..."
echo ""

python3 run_phase1.py
echo "Phase 1 data generation complete."

echo ""
echo "Running Phase 1 validation ..."
python3 validate_phase1.py
echo "Phase 1 validation complete."

# --------------------------------------------------------------------------
# [8/8] Test S3 connectivity via Delta Lake
# --------------------------------------------------------------------------
banner "[8/8] Test S3 connectivity (Delta + S3A)"

S3_BUCKET="s3a://jiohotstar-lakehouse-868896905478/bronze/_connectivity_test/"

python3 - <<'PYEOF'
import sys
try:
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    extra_pkgs = [
        "org.apache.hadoop:hadoop-aws:3.3.2",
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026",
    ]

    builder = (
        SparkSession.builder
        .appName("S3ConnectivityTest")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    )

    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_pkgs).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    s3_path = "s3a://jiohotstar-lakehouse-868896905478/bronze/_connectivity_test/"

    # Write a small test table
    test_df = spark.createDataFrame(
        [("connectivity_check", 1)],
        ["test_key", "test_value"]
    )
    test_df.write.format("delta").mode("overwrite").save(s3_path)
    print("  [OK] Delta write to S3 succeeded.")

    # Read it back
    read_df = spark.read.format("delta").load(s3_path)
    row_count = read_df.count()
    print(f"  [OK] Delta read from S3 succeeded — {row_count} row(s) returned.")

    spark.stop()
    print("  S3 + Delta connectivity test PASSED.")

except Exception as e:
    print(f"  [FAIL] S3 connectivity test failed: {e}", file=sys.stderr)
    print("  Ensure AWS credentials are configured (IAM role or ~/.aws/credentials).", file=sys.stderr)
    sys.exit(1)
PYEOF

# ==========================================================================
# Done
# ==========================================================================
banner "EC2 Provisioning Complete!"

cat <<'NEXT'
Next steps:
  1. Verify AWS credentials are configured (IAM instance role recommended).
  2. Review .env for correct DB ports and connection strings.
  3. Access the UIs:
       - Adminer  (DB admin) : http://<EC2_PUBLIC_IP>:8080
       - Kafka UI            : http://<EC2_PUBLIC_IP>:8081
  4. Phase 1 data is generated. Proceed to Phase 2 (Bronze ingestion).
NEXT
