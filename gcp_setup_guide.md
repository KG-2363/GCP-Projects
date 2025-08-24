# Google Cloud Dataproc and BigQuery Setup Guide

## Prerequisites

1. **Google Cloud Account** with billing enabled
2. **gcloud CLI** installed and configured
3. **Required APIs** enabled in your project

## Step 1: Enable Required APIs

```bash
# Enable required Google Cloud APIs
gcloud services enable dataproc.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable compute.googleapis.com
```

## Step 2: Set Up Environment Variables

```bash
# Set your project configuration
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export ZONE="us-central1-b"
export CLUSTER_NAME="spark-pipeline-cluster"
export BUCKET_NAME="your-unique-bucket-name"
export DATASET_ID="spark_pipeline_demo"

# Set default project
gcloud config set project $PROJECT_ID
gcloud config set compute/region $REGION
gcloud config set compute/zone $ZONE
```

## Step 3: Create Google Cloud Storage Bucket

```bash
# Create GCS bucket for temporary files and data
gsutil mb -p $PROJECT_ID -c STANDARD -l $REGION gs://$BUCKET_NAME

# Verify bucket creation
gsutil ls gs://$BUCKET_NAME
```

## Step 4: Create BigQuery Dataset

```bash
# Create BigQuery dataset
bq mk --location=US $PROJECT_ID:$DATASET_ID

# Verify dataset creation
bq ls $PROJECT_ID:$DATASET_ID
```

## Step 5: Create Dataproc Cluster

### Option A: Basic Cluster (for testing)

```bash
# Create a basic Dataproc cluster
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --zone=$ZONE \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=50GB \
    --worker-machine-type=n1-standard-2 \
    --worker-boot-disk-size=50GB \
    --num-workers=2 \
    --image-version=2.0-debian10 \
    --project=$PROJECT_ID
```

### Option B: Production Cluster (with autoscaling)

```bash
# Create production cluster with autoscaling
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --zone=$ZONE \
    --master-machine-type=n1-standard-4 \
    --master-boot-disk-size=100GB \
    --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=100GB \
    --num-workers=2 \
    --max-workers=10 \
    --enable-autoscaling \
    --image-version=2.0-debian10 \
    --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
    --metadata=enable-cloud-sql-hive-metastore=false \
    --optional-components=JUPYTER,ZEPPELIN \
    --enable-ip-alias \
    --project=$PROJECT_ID
```

### Option C: Memory-Optimized Cluster (for large data)

```bash
# Create memory-optimized cluster for handling OOM issues
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --zone=$ZONE \
    --master-machine-type=n1-highmem-4 \
    --master-boot-disk-size=100GB \
    --worker-machine-type=n1-highmem-4 \
    --worker-boot-disk-size=100GB \
    --num-workers=3 \
    --max-workers=8 \
    --enable-autoscaling \
    --image-version=2.0-debian10 \
    --properties='spark:spark.driver.memory=6g,spark:spark.executor.memory=6g,spark:spark.driver.maxResultSize=2g' \
    --project=$PROJECT_ID
```

## Step 6: Upload Pipeline Code

```bash
# Upload your Python script to GCS
gsutil cp bigquery_dataproc_pipeline.py gs://$BUCKET_NAME/code/
gsutil cp requirements.txt gs://$BUCKET_NAME/code/

# Create additional directories
gsutil -m cp -r solutions/ gs://$BUCKET_NAME/code/
```

## Step 7: Submit Spark Job to Dataproc

### Option A: Submit PySpark Job

```bash
# Submit the pipeline job
gcloud dataproc jobs submit pyspark \
    gs://$BUCKET_NAME/code/bigquery_dataproc_pipeline.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files=gs://$BUCKET_NAME/code/requirements.txt \
    --properties='spark.driver.memory=4g,spark.executor.memory=4g' \
    --project=$PROJECT_ID
```

### Option B: Submit with Custom Configuration

```bash
# Submit with specific Spark configurations
gcloud dataproc jobs submit pyspark \
    gs://$BUCKET_NAME/code/bigquery_dataproc_pipeline.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --properties='spark.driver.memory=6g,spark.executor.memory=6g,spark.executor.cores=4,spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true' \
    --project=$PROJECT_ID
```

## Step 8: Monitor Job Execution

### Using gcloud CLI

```bash
# List running jobs
gcloud dataproc jobs list --region=$REGION --cluster=$CLUSTER_NAME

# Get job details (replace JOB_ID with actual job ID)
gcloud dataproc jobs describe JOB_ID --region=$REGION

# View job logs
gcloud dataproc jobs wait JOB_ID --region=$REGION
```

### Using Web Console

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Navigate to **Dataproc > Jobs**
3. Select your job to view details and logs
4. Access **Spark UI** through the job details page

## Step 9: Verify Results in BigQuery

```bash
# Query the results using bq command line
bq query --use_legacy_sql=false "
SELECT 
  partition_key, 
  COUNT(*) as record_count 
FROM \`$PROJECT_ID.$DATASET_ID.target_orders_v3\` 
GROUP BY partition_key 
ORDER BY record_count DESC
"

# Query daily aggregated data
bq query --use_legacy_sql=false "
SELECT 
  date,
  COUNT(DISTINCT customer_id) as unique_customers,
  SUM(total_amount) as daily_revenue
FROM \`$PROJECT_ID.$DATASET_ID.target_orders_v3\`
GROUP BY date
ORDER BY date
"
```

## Alternative: Using Jupyter/Zeppelin Notebooks

### Access Jupyter Notebook

```bash
# Get cluster details to find Jupyter URL
gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION

# Create SSH tunnel to access Jupyter (if not using public IP)
gcloud compute ssh $CLUSTER_NAME-m \
    --zone=$ZONE \
    --ssh-flag="-D 1080" \
    --ssh-flag="-N" \
    --ssh-flag="-n"

# Access Jupyter at: http://CLUSTER_MASTER_IP:8123
```

### Access Zeppelin Notebook

```bash
# Access Zeppelin at: http://CLUSTER_MASTER_IP:8080
# You can run the pipeline interactively in notebooks
```

## Step 10: Production Deployment Scripts

### Create deployment script: `deploy_pipeline.sh`

```bash
#!/bin/bash

# Configuration
PROJECT_ID="your-project-id"
REGION="us-central1"
CLUSTER_NAME="spark-pipeline-cluster"
BUCKET_NAME="your-unique-bucket-name"

set -e

echo "🚀 Deploying Spark Pipeline to Dataproc..."

# Upload code to GCS
echo "📦 Uploading code to GCS..."
gsutil -m cp -r . gs://$BUCKET_NAME/code/

# Submit job with error handling
echo "▶️  Submitting Spark job..."
JOB_ID=$(gcloud dataproc jobs submit pyspark \
    gs://$BUCKET_NAME/code/bigquery_dataproc_pipeline.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --properties='spark.driver.memory=4g,spark.executor.memory=4g,spark.sql.adaptive.enabled=true' \
    --project=$PROJECT_ID \
    --format="value(reference.jobId)")

echo "📋 Job submitted with ID: $JOB_ID"

# Wait for job completion
echo "⏳ Waiting for job completion..."
gcloud dataproc jobs wait $JOB_ID --region=$REGION

# Check job status
STATUS=$(gcloud dataproc jobs describe $JOB_ID --region=$REGION --format="value(status.state)")

if [ "$STATUS" = "DONE" ]; then
    echo "✅ Job completed successfully!"
    
    # Verify results in BigQuery
    echo "🔍 Verifying results in BigQuery..."
    bq query --use_legacy_sql=false "
    SELECT COUNT(*) as total_records 
    FROM \`$PROJECT_ID.spark_pipeline_demo.target_orders_v3\`
    "
else
    echo "❌ Job failed with status: $STATUS"
    echo "📋 Check logs at: https://console.cloud.google.com/dataproc/jobs/$JOB_ID?project=$PROJECT_ID&region=$REGION"
    exit 1
fi
```

### Create cluster management script: `manage_cluster.sh`

```bash
#!/bin/bash

PROJECT_ID="your-project-id"
REGION="us-central1"
CLUSTER_NAME="spark-pipeline-cluster"

case "$1" in
    create)
        echo "🔄 Creating Dataproc cluster..."
        gcloud dataproc clusters create $CLUSTER_NAME \
            --region=$REGION \
            --master-machine-type=n1-standard-4 \
            --master-boot-disk-size=100GB \
            --worker-machine-type=n1-standard-4 \
            --worker-boot-disk-size=100GB \
            --num-workers=2 \
            --max-workers=8 \
            --enable-autoscaling \
            --image-version=2.0-debian10 \
            --optional-components=JUPYTER,ZEPPELIN \
            --project=$PROJECT_ID
        ;;
    delete)
        echo "🗑️  Deleting Dataproc cluster..."
        gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet
        ;;
    status)
        echo "📋 Cluster status:"
        gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION --format="value(status.state)"
        ;;
    scale-up)
        echo "⬆️  Scaling cluster up..."
        gcloud dataproc clusters update $CLUSTER_NAME \
            --region=$REGION \
            --num-workers=4 \
            --max-workers=12
        ;;
    scale-down)
        echo "⬇️  Scaling cluster down..."
        gcloud dataproc clusters update $CLUSTER_NAME \
            --region=$REGION \
            --num-workers=2 \
            --max-workers=6
        ;;
    *)
        echo "Usage: $0 {create|delete|status|scale-up|scale-down}"
        exit 1
        ;;
esac
```

## Step 11: Monitoring and Troubleshooting

### Monitor Job Performance

```bash
# View Spark UI through port forwarding
gcloud compute ssh $CLUSTER_NAME-m \
    --zone=$ZONE \
    --ssh-flag="-L 4040:localhost:4040"

# Then access http://localhost:4040 in your browser
```

### Common Issues and Solutions

#### 1. Out of Memory Errors
```bash
# Increase driver and executor memory
gcloud dataproc jobs submit pyspark \
    gs://$BUCKET_NAME/code/bigquery_dataproc_pipeline.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --properties='spark.driver.memory=8g,spark.executor.memory=8g,spark.driver.maxResultSize=4g'
```

#### 2. BigQuery Quota Exceeded
```bash
# Use staging bucket for large writes
gcloud dataproc jobs submit pyspark \
    gs://$BUCKET_NAME/code/bigquery_dataproc_pipeline.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --properties='spark.sql.execution.bigquery.temporaryGcsBucket=gs://$BUCKET_NAME/temp'
```

#### 3. Slow Performance Due to Skewness
```bash
# Enable adaptive query execution
gcloud dataproc jobs submit pyspark \
    gs://$BUCKET_NAME/code/bigquery_dataproc_pipeline.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --properties='spark.sql.adaptive.enabled=true,spark.sql.adaptive.skewJoin.enabled=true'
```

### Debugging Commands

```bash
# Get cluster logs
gcloud dataproc clusters export $CLUSTER_NAME \
    --region=$REGION \
    --destination=cluster-config.yaml

# Get job logs
gcloud dataproc jobs describe JOB_ID \
    --region=$REGION \
    --format="value(driverOutputResourceUri)"

# SSH into cluster for debugging
gcloud compute ssh $CLUSTER_NAME-m --zone=$ZONE

# Check Spark history server
# Access: http://CLUSTER_MASTER_IP:18080
```

## Step 12: Cost Optimization

### 1. Use Preemptible Instances

```bash
# Create cluster with preemptible workers
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --master-machine-type=n1-standard-2 \
    --worker-machine-type=n1-standard-2 \
    --preemptible \
    --num-preemptible-workers=4 \
    --num-workers=1 \
    --max-workers=8
```

### 2. Auto-delete Cluster

```bash
# Create cluster that auto-deletes after job completion
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --max-idle=10m \
    --enable-autoscaling \
    --max-workers=6
```

### 3. Use Spot VMs (Beta)

```bash
# Use Spot VMs for significant cost savings
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --spot \
    --num-workers=2 \
    --max-workers=8
```

## Step 13: CI/CD Integration

### GitHub Actions Workflow: `.github/workflows/dataproc-pipeline.yml`

```yaml
name: Deploy Spark Pipeline to Dataproc

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - id: 'auth'
      uses: 'google-github-actions/auth@v0'
      with:
        credentials_json: '${{ secrets.GCP_SA_KEY }}'
    
    - name: 'Set up Cloud SDK'
      uses: 'google-github-actions/setup-gcloud@v0'
    
    - name: 'Upload code to GCS'
      run: |
        gsutil -m cp -r . gs://${{ secrets.BUCKET_NAME }}/code/
    
    - name: 'Submit Spark job'
      run: |
        gcloud dataproc jobs submit pyspark \
          gs://${{ secrets.BUCKET_NAME }}/code/bigquery_dataproc_pipeline.py \
          --cluster=${{ secrets.CLUSTER_NAME }} \
          --region=${{ secrets.REGION }} \
          --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
```

## Step 14: Data Quality Checks

### BigQuery Data Quality Queries

```sql
-- Check for data completeness
SELECT 
  date,
  COUNT(*) as total_records,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(DISTINCT partition_key) as unique_partitions,
  SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amounts
FROM `your-project-id.spark_pipeline_demo.target_orders_v3`
GROUP BY date
ORDER BY date;

-- Check for data skewness
SELECT 
  partition_key,
  COUNT(*) as record_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM `your-project-id.spark_pipeline_demo.target_orders_v3`
GROUP BY partition_key
ORDER BY record_count DESC;

-- Check for data anomalies
SELECT 
  'Negative amounts' as check_type,
  COUNT(*) as anomaly_count
FROM `your-project-id.spark_pipeline_demo.target_orders_v3`
WHERE total_amount < 0
UNION ALL
SELECT 
  'Future dates' as check_type,
  COUNT(*) as anomaly_count
FROM `your-project-id.spark_pipeline_demo.target_orders_v3`
WHERE date > CURRENT_DATE();
```

## Step 15: Clean Up Resources

### Complete cleanup script: `cleanup.sh`

```bash
#!/bin/bash

PROJECT_ID="your-project-id"
REGION="us-central1"
CLUSTER_NAME="spark-pipeline-cluster"
BUCKET_NAME="your-unique-bucket-name"
DATASET_ID="spark_pipeline_demo"

echo "🧹 Cleaning up resources..."

# Delete Dataproc cluster
echo "🗑️  Deleting Dataproc cluster..."
gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet || echo "Cluster not found"

# Delete BigQuery dataset and tables
echo "🗑️  Deleting BigQuery dataset..."
bq rm -r -f $PROJECT_ID:$DATASET_ID

# Delete GCS bucket
echo "🗑️  Deleting GCS bucket..."
gsutil -m rm -r gs://$BUCKET_NAME || echo "Bucket not found"

echo "✅ Cleanup completed!"
```

## Quick Reference Commands

| **Action** | **Command** |
|------------|-------------|
| Create cluster | `gcloud dataproc clusters create $CLUSTER_NAME --region=$REGION` |
| Submit job | `gcloud dataproc jobs submit pyspark script.py --cluster=$CLUSTER_NAME --region=$REGION` |
| List jobs | `gcloud dataproc jobs list --region=$REGION` |
| Delete cluster | `gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION` |
| Query BigQuery | `bq query --use_legacy_sql=false "SELECT * FROM dataset.table LIMIT 10"` |
| Upload to GCS | `gsutil cp file.py gs://bucket/path/` |
| SSH to cluster | `gcloud compute ssh $CLUSTER_NAME-m --zone=$ZONE` |

## Best Practices Summary

1. **Use appropriate machine types** - Balance cost and performance
2. **Enable autoscaling** - Handle variable workloads efficiently  
3. **Use preemptible instances** - Reduce costs for fault-tolerant jobs
4. **Partition BigQuery tables** - Improve query performance and reduce costs
5. **Monitor resource usage** - Optimize cluster sizing based on actual usage
6. **Use staging buckets** - Handle large BigQuery operations efficiently
7. **Enable adaptive query execution** - Let Spark optimize automatically
8. **Set up proper logging** - Use Cloud Logging for better debugging
9. **Implement data quality checks** - Validate results in BigQuery
10. **Clean up resources** - Avoid unnecessary costs