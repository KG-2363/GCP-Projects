# PySpark Pipeline with BigQuery and Google Cloud Dataproc

This project demonstrates a complete PySpark data pipeline that runs on Google Cloud Dataproc and writes to BigQuery. It includes intentional failures (OOM, data skewness) and their step-by-step solutions for learning purposes.

## 🎯 What You'll Learn

- **Memory Management**: Handle OutOfMemoryError in Spark applications
- **Data Skewness**: Identify and solve data skewness issues using salting techniques
- **BigQuery Integration**: Read from and write to BigQuery using Spark
- **Google Cloud Dataproc**: Deploy and manage Spark clusters in the cloud
- **Production Best Practices**: Monitoring, error handling, and cost optimization

## 📋 Prerequisites

### Required Tools
- **Google Cloud Account** with billing enabled
- **gcloud CLI** installed and authenticated
- **BigQuery CLI (bq)** installed  
- **gsutil** for Cloud Storage operations

### Required Permissions
Your account needs the following IAM roles:
- `Dataproc Admin` or `Dataproc Editor`
- `BigQuery Admin` or `BigQuery Data Editor`
- `Storage Admin` or `Storage Object Admin`
- `Compute Admin` (for creating VM instances)

## 🚀 Quick Start

### Step 1: Clone and Configure

```bash
# Clone the repository
git clone <repository-url>
cd pyspark-bigquery-dataproc

# Copy and edit configuration
cp gcp_config.sh my_config.sh
nano my_config.sh

# Update these required values:
export PROJECT_ID="your-actual-project-id"
export BUCKET_NAME="your-unique-bucket-name-20241201"
```

### Step 2: Set Up Environment

```bash
# Load configuration
source my_config.sh

# Validate configuration
validate_config

# Set up GCP environment (creates cluster, bucket, dataset)
chmod +x setup_gcp_environment.sh
./setup_gcp_environment.sh
```

### Step 3: Run the Pipeline

```bash
# Submit the pipeline job to Dataproc
./submit_job.sh

# Monitor the job in Cloud Console or view logs
gcloud dataproc jobs list --region=$REGION
```

### Step 4: View Results

```bash
# Query results in BigQuery
bq query --use_legacy_sql=false "
SELECT partition_key, COUNT(*) as record_count 
FROM \`$PROJECT_ID.$DATASET_ID.target_orders_v3\` 
GROUP BY partition_key 
ORDER BY record_count DESC
"
```

### Step 5: Clean Up

```bash
# Clean up all resources
./cleanup.sh
```

## 📁 Project Structure

```
pyspark-bigquery-dataproc/
├── bigquery_dataproc_pipeline.py    # Main pipeline code
├── gcp_config.sh                   # Configuration template
├── setup_gcp_environment.sh        # Environment setup script
├── requirements.txt                # Python dependencies
├── README.md                       # This file
├── solutions/
│   ├── memory_optimization.md      # Memory issue solutions
│   ├── skewness_solutions.md      # Data skewness solutions
│   └── best_practices.md          # General best practices
└── deployment/
    ├── submit_job.sh              # Job submission script
    ├── cleanup.sh                 # Resource cleanup script
    └── monitor.sh                 # Monitoring script
```

## 🔧 Configuration Options

### Development Configuration
```bash
source my_config.sh
create_dev_config  # Small, cost-effective setup
show_config        # View current settings
```

### Production Configuration
```bash
source my_config.sh
create_prod_config  # Robust, scalable setup
estimate_costs     # View estimated costs
```

### Memory-Optimized Configuration
```bash
source my_config.sh
create_memory_optimized_config  # For memory-intensive workloads
```

## 🧪 Pipeline Phases

The pipeline demonstrates three phases of optimization:

### Phase 1: Problematic Pipeline (Will Fail)
- **Issues**: OutOfMemoryError, expensive operations
- **Cause**: `collect_list()`, excessive caching
- **Expected Result**: ❌ Job fails with OOM

### Phase 2: Memory-Fixed Pipeline (Slow Performance)
- **Issues**: Data skewness causing uneven task distribution
- **Cause**: 80% of data has the same partition key
- **Expected Result**: ✅ Completes but very slow

### Phase 3: Optimized Pipeline (Fast Performance)
- **Solutions**: Salting technique, two-phase aggregation
- **Optimizations**: Proper partitioning, adaptive query execution
- **Expected Result**: ✅ Fast, efficient processing

## 📊 Monitoring and Debugging

### Access Spark UI
```bash
# Create tunnel to Spark UI
gcloud compute ssh ${CLUSTER_NAME}-m \
    --zone=$ZONE \
    --ssh-flag="-L 4040:localhost:4040"
# Then open http://localhost:4040
```

### View Job Logs
```bash
# List all jobs
gcloud dataproc jobs list --region=$REGION

# Get job details
gcloud dataproc jobs describe JOB_ID --region=$REGION

# View driver logs
gcloud dataproc jobs describe JOB_ID \
    --region=$REGION \
    --format="value(driverOutputResourceUri)"
```

### BigQuery Monitoring
```bash
# Check job history
bq ls -j --max_results=10

# View table details
bq show $PROJECT_ID:$DATASET_ID.target_orders_v3

# Check data quality
bq query --use_legacy_sql=false "
SELECT 
  COUNT(*) as total_records,
  COUNT(DISTINCT partition_key) as unique_keys,
  AVG(total_amount) as avg_amount
FROM \`$PROJECT_ID.$DATASET_ID.target_orders_v3\`
"
```

## 🎓 Learning Scenarios

### Scenario 1: Out of Memory Error
**Trigger**: Run Phase 1 pipeline
**Error**: `java.lang.OutOfMemoryError: Java heap space`
**Solution**: Remove `collect_list()`, increase driver memory

### Scenario 2: Data Skewness
**Trigger**: Run Phase 2 pipeline  
**Symptom**: Some tasks take 10x longer than others
**Solution**: Salting technique with two-phase aggregation

### Scenario 3: BigQuery Write Optimization
**Challenge**: Efficiently write large datasets to BigQuery
**Solution**: Partitioning, clustering, proper data types

## 💰 Cost Optimization

### Use Preemptible Instances
```bash
# Edit configuration
export USE_PREEMPTIBLE="true"
# Saves ~70% on worker costs
```

### Enable Auto-deletion
```bash
# Auto-delete cluster after 30 minutes of inactivity
export IDLE_DELETE_TTL="30m"
```

### Right-size Your Cluster
```bash
# For development
create_dev_config

# Monitor usage and adjust
gcloud compute instances list --filter="name:$CLUSTER_NAME"
```

## 🔍 Troubleshooting

### Common Issues and Solutions

#### Issue: "Permission denied" errors
```bash
# Grant necessary permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="user:your-email@example.com" \
    --role="roles/dataproc.editor"
```

#### Issue: BigQuery quota exceeded
```bash
# Use staging bucket for large operations
export SPARK_PROPERTIES="$SPARK_PROPERTIES,spark:temporaryGcsBucket=gs://$BUCKET_NAME"
```

#### Issue: Cluster creation fails
```bash
# Check quotas
gcloud compute project-info describe --project=$PROJECT_ID

# Try different region
export REGION="us-west1"
```

#### Issue: Out of memory during job execution
```bash
# Increase memory allocation
export DRIVER_MEMORY="8g"
export EXECUTOR_MEMORY="8g"
# Then recreate cluster
```

## 🔗 Useful Links

- [Google Cloud Dataproc Documentation](https://cloud.google.com/dataproc/docs)
- [BigQuery Spark Connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [Google Cloud Pricing Calculator](https://cloud.google.com/products/calculator)

## 📈 Advanced Topics

### CI/CD Integration
```yaml
# .github/workflows/dataproc-deploy.yml
name: Deploy to Dataproc
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: google-github-actions/auth@v0
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}
      - name: Deploy pipeline
        run: |
          source gcp_config.sh
          ./submit_job.sh
```

### Custom Docker Images
```bash
# Build custom Dataproc image with additional libraries
gcloud builds submit --tag gcr.io/$PROJECT_ID/custom-dataproc .
```

### Streaming Pipeline
```python
# Modify pipeline for real-time processing
df = spark.readStream \
    .format("bigquery") \
    .option("table", f"{project_id}.{dataset_id}.streaming_table") \
    .load()
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with different configurations
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Apache Spark community for the excellent framework
- Google Cloud team for Dataproc and BigQuery integration
- Contributors and testers who helped improve this learning resource

---

## Quick Reference Commands

| **Action** | **Command** |
|------------|-------------|
| Load config | `source my_config.sh` |
| Show config | `show_config` |
| Validate setup | `validate_config` |
| Create cluster | `./setup_gcp_environment.sh` |
| Submit job | `./submit_job.sh` |
| List jobs | `gcloud dataproc jobs list --region=$REGION` |
| View results | `bq query "SELECT * FROM dataset.table LIMIT 10"` |
| Clean up | `./cleanup.sh` |

**Happy Learning! 🎉**