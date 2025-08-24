#!/bin/bash
# File: setup_gcp_environment.sh
# Complete setup script for GCP environment

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration - UPDATE THESE VALUES
PROJECT_ID="your-project-id"
REGION="us-central1"
ZONE="us-central1-b"
CLUSTER_NAME="spark-pipeline-cluster"
BUCKET_NAME="your-unique-bucket-name"
DATASET_ID="spark_pipeline_demo"
SERVICE_ACCOUNT_NAME="dataproc-pipeline-sa"

echo -e "${GREEN}🚀 Setting up GCP environment for Spark Pipeline${NC}"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
echo -e "${YELLOW}📋 Checking prerequisites...${NC}"

if ! command_exists gcloud; then
    echo -e "${RED}❌ gcloud CLI not found. Please install Google Cloud SDK.${NC}"
    echo "Visit: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

if ! command_exists bq; then
    echo -e "${RED}❌ bq CLI not found. Please install BigQuery CLI.${NC}"
    exit 1
fi

if ! command_exists gsutil; then
    echo -e "${RED}❌ gsutil not found. Please install Google Cloud SDK.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Prerequisites check passed${NC}"

# Validate configuration
if [ "$PROJECT_ID" = "your-project-id" ] || [ "$BUCKET_NAME" = "your-unique-bucket-name" ]; then
    echo -e "${RED}❌ Please update configuration variables in this script:${NC}"
    echo "  PROJECT_ID='your-actual-project-id'"
    echo "  BUCKET_NAME='your-actual-unique-bucket-name'"
    exit 1
fi

# Set project
echo -e "${YELLOW}🔧 Setting up project configuration...${NC}"
gcloud config set project $PROJECT_ID
gcloud config set compute/region $REGION
gcloud config set compute/zone $ZONE

# Enable APIs
echo -e "${YELLOW}🔌 Enabling required APIs...${NC}"
gcloud services enable dataproc.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable compute.googleapis.com

# Create service account
echo -e "${YELLOW}👤 Creating service account...${NC}"
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
    --display-name="Dataproc Pipeline Service Account" \
    --description="Service account for Spark pipeline on Dataproc" || \
    echo "Service account already exists"

# Grant necessary permissions
echo -e "${YELLOW}🔐 Granting permissions to service account...${NC}"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/dataproc.worker"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/storage.objectAdmin"

# Create GCS bucket
echo -e "${YELLOW}🗂️  Creating GCS bucket...${NC}"
gsutil mb -p $PROJECT_ID -c STANDARD -l $REGION gs://$BUCKET_NAME || \
    echo "Bucket already exists or creation failed"

# Create BigQuery dataset
echo -e "${YELLOW}📊 Creating BigQuery dataset...${NC}"
bq mk --location=US $PROJECT_ID:$DATASET_ID || \
    echo "Dataset already exists or creation failed"

# Create Dataproc cluster
echo -e "${YELLOW}🖥️  Creating Dataproc cluster...${NC}"
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --zone=$ZONE \
    --master-machine-type=n1-standard-4 \
    --master-boot-disk-size=100GB \
    --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=100GB \
    --num-workers=2 \
    --max-workers=8 \
    --enable-autoscaling \
    --image-version=2.0-debian10 \
    --optional-components=JUPYTER,ZEPPELIN \
    --service-account=$SERVICE_ACCOUNT_EMAIL \
    --properties='spark:spark.driver.memory=4g,spark:spark.executor.memory=4g,spark:spark.sql.adaptive.enabled=true' \
    --project=$PROJECT_ID || \
    echo "Cluster creation failed or cluster already exists"

# Upload pipeline code
echo -e "${YELLOW}📦 Uploading pipeline code to GCS...${NC}"
if [ -f "bigquery_dataproc_pipeline.py" ]; then
    gsutil cp bigquery_dataproc_pipeline.py gs://$BUCKET_NAME/code/
    gsutil cp requirements.txt gs://$BUCKET_NAME/code/
    echo -e "${GREEN}✅ Code uploaded successfully${NC}"
else
    echo -e "${YELLOW}⚠️  Pipeline code not found in current directory${NC}"
    echo "Make sure bigquery_dataproc_pipeline.py is in the current directory"
fi

# Summary
echo -e "${GREEN}🎉 GCP environment setup completed!${NC}"
echo
echo "Configuration Summary:"
echo "  Project ID: $PROJECT_ID"
echo "  Region: $REGION"
echo "  Cluster Name: $CLUSTER_NAME"
echo "  Bucket Name: $BUCKET_NAME"
echo "  Dataset ID: $DATASET_ID"
echo
echo "Next steps:"
echo "1. Update the pipeline code with your PROJECT_ID and BUCKET_NAME"
echo "2. Run: ./submit_job.sh to execute the pipeline"
echo "3. Access Jupyter at: http://$(gcloud compute instances describe ${CLUSTER_NAME}-m --zone=$ZONE --format='value(networkInterfaces[0].accessConfigs[0].natIP)'):8123"
echo "4. Access Spark UI at: http://$(gcloud compute instances describe ${CLUSTER_NAME}-m --zone=$ZONE --format='value(networkInterfaces[0].accessConfigs[0].natIP)'):4040"

# Create job submission script
cat > submit_job.sh << EOF
#!/bin/bash
# Auto-generated job submission script

PROJECT_ID="$PROJECT_ID"
REGION="$REGION"
CLUSTER_NAME="$CLUSTER_NAME"
BUCKET_NAME="$BUCKET_NAME"

echo "🚀 Submitting Spark job to Dataproc..."

# Upload latest code
gsutil cp bigquery_dataproc_pipeline.py gs://\$BUCKET_NAME/code/

# Submit job
JOB_ID=\$(gcloud dataproc jobs submit pyspark \\
    gs://\$BUCKET_NAME/code/bigquery_dataproc_pipeline.py \\
    --cluster=\$CLUSTER_NAME \\
    --region=\$REGION \\
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \\
    --properties='spark.driver.memory=4g,spark.executor.memory=4g,spark.sql.adaptive.enabled=true' \\
    --project=\$PROJECT_ID \\
    --format="value(reference.jobId)")

echo "📋 Job submitted with ID: \$JOB_ID"
echo "🔗 View job at: https://console.cloud.google.com/dataproc/jobs/\$JOB_ID?project=\$PROJECT_ID&region=\$REGION"

# Wait for completion
echo "⏳ Waiting for job completion..."
gcloud dataproc jobs wait \$JOB_ID --region=\$REGION

echo "✅ Job completed!"
EOF

chmod +x submit_job.sh

# Create cleanup script
cat > cleanup.sh << EOF
#!/bin/bash
# Auto-generated cleanup script

PROJECT_ID="$PROJECT_ID"
REGION="$REGION"
CLUSTER_NAME="$CLUSTER_NAME"
BUCKET_NAME="$BUCKET_NAME"
DATASET_ID="$DATASET_ID"

echo "🧹 Cleaning up resources..."

# Delete cluster
echo "🗑️  Deleting Dataproc cluster..."
gcloud dataproc clusters delete \$CLUSTER_NAME --region=\$REGION --quiet || echo "Cluster not found"

# Delete BigQuery dataset
echo "🗑️  Deleting BigQuery dataset..."
bq rm -r -f \$PROJECT_ID:\$DATASET_ID || echo "Dataset not found"

# Delete bucket contents (keep bucket)
echo "🗑️  Cleaning GCS bucket..."
gsutil -m rm -r gs://\$BUCKET_NAME/code/ || echo "Code directory not found"
gsutil -m rm -r gs://\$BUCKET_NAME/temp_data/ || echo "Temp directory not found"

echo "✅ Cleanup completed!"
echo "Note: GCS bucket \$BUCKET_NAME was preserved. Delete manually if needed:"
echo "  gsutil rm -r gs://\$BUCKET_NAME"
EOF

chmod +x cleanup.sh

echo -e "${GREEN}📝 Created additional scripts:${NC}"
echo "  submit_job.sh - Submit pipeline job to Dataproc"
echo "  cleanup.sh - Clean up all created resources"