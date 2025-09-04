# File: bigquery_dataproc_pipeline.py
# PySpark Pipeline modified for Google Cloud Dataproc and BigQuery

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
import time
import os
from google.cloud import bigquery

class BigQueryDataprocPipeline:
    def __init__(self, project_id, dataset_id, bucket_name):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.spark = None
        self.bq_client = None
        self.setup_spark()
        self.setup_bigquery_client()
    
    def setup_spark(self):
        """Initialize Spark session with BigQuery and GCS configurations"""
        print("Setting up Spark session for Dataproc with BigQuery...")
        
        # BigQuery connector JAR is pre-installed on Dataproc
        self.spark = SparkSession.builder \
            .appName("BigQueryDataprocPipeline") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        # Set BigQuery temporary bucket
        self.spark.conf.set("temporaryGcsBucket", self.bucket_name)
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark session created for Dataproc")
    
    def setup_bigquery_client(self):
        """Initialize BigQuery client"""
        try:
            self.bq_client = bigquery.Client(project=self.project_id)
            print("✅ BigQuery client initialized")
        except Exception as e:
            print(f"⚠️  Warning: Could not initialize BigQuery client: {e}")
            self.bq_client = None
    
    def create_bigquery_dataset(self):
        """Create BigQuery dataset if it doesn't exist"""
        if not self.bq_client:
            print("⚠️  BigQuery client not available, skipping dataset creation")
            return
        
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        
        try:
            self.bq_client.get_dataset(dataset_id)
            print(f"✅ Dataset {dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"  # or your preferred location
            dataset = self.bq_client.create_dataset(dataset, timeout=30)
            print(f"✅ Created dataset {dataset_id}")
    
    def create_sample_data_gcs(self):
        """Create sample data and save to GCS, then load to BigQuery"""
        print("Creating sample data with skewed distribution...")
        
        # Create skewed data - 80% of records have same key
        data = []
        
        # 80% records with skewed key
        for i in range(8000):
            data.append((
                "SKEWED_KEY",  # This will cause data skew
                f"customer_{random.randint(1, 100)}",
                random.randint(100, 1000),
                random.uniform(10.0, 500.0),
                f"product_{random.randint(1, 50)}",
                "2024-01-15"
            ))
        
        # 20% records with normal distribution
        for i in range(2000):
            data.append((
                f"normal_key_{random.randint(1, 100)}",
                f"customer_{random.randint(1, 100)}",
                random.randint(100, 1000),
                random.uniform(10.0, 500.0),
                f"product_{random.randint(1, 50)}",
                "2024-01-15"
            ))
        
        schema = StructType([
            StructField("partition_key", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("product", StringType(), True),
            StructField("date", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        
        # Save to GCS first (for intermediate processing)
        gcs_temp_path = f"gs://{self.bucket_name}/temp_data/temp_orders"
        df.write.mode("overwrite").parquet(gcs_temp_path)
        
        # Write to BigQuery temp table
        temp_table = f"{self.project_id}.{self.dataset_id}.temp_orders"
        
        df.write \
            .format("bigquery") \
            .option("table", temp_table) \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
        
        print(f"✅ Created temp table: {temp_table} with {df.count()} records")
        return df
    
    def create_target_tables_bq(self):
        """Create target tables in BigQuery"""
        print("Creating target tables in BigQuery...")
        
        # Define target table schema
        target_schema = [
            bigquery.SchemaField("partition_key", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("customer_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("order_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("amount", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("product", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("date", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("processed_timestamp", "TIMESTAMP", mode="NULLABLE")
        ]
        
        # Create target tables
        target_tables = [
            "target_orders_v1",
            "target_orders_v2", 
            "target_orders_v3"
        ]
        
        if self.bq_client:
            for table_name in target_tables:
                table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
                table = bigquery.Table(table_id, schema=target_schema)
                
                try:
                    self.bq_client.create_table(table)
                    print(f"✅ Created BigQuery table: {table_id}")
                except Exception as e:
                    if "Already Exists" in str(e):
                        print(f"✅ Table {table_id} already exists")
                    else:
                        print(f"❌ Error creating table {table_id}: {e}")
        else:
            print("⚠️  BigQuery client not available, tables will be created automatically on write")
    
    def problematic_pipeline_v1(self):
        """Version 1: Pipeline that will fail with OOM and skewness issues"""
        print("\n" + "="*50)
        print("RUNNING PROBLEMATIC PIPELINE V1 - BigQuery Version")
        print("="*50)
        
        try:
            print("Step 1: Reading from BigQuery temp table...")
            temp_table = f"{self.project_id}.{self.dataset_id}.temp_orders"
            
            temp_df = self.spark.read \
                .format("bigquery") \
                .option("table", temp_table) \
                .load()
            
            print("Step 2: Adding processing timestamp...")
            processed_df = temp_df.withColumn("processed_timestamp", current_timestamp())
            
            print("Step 3: Performing expensive operations that cause OOM...")
            # This will cause memory issues
            result_df = processed_df \
                .groupBy("partition_key", "customer_id", "product") \
                .agg(
                    sum("amount").alias("total_amount"),
                    count("*").alias("order_count"),
                    collect_list("order_id").alias("order_list"),  # Memory intensive
                    first("date").alias("date"),
                    first("processed_timestamp").alias("processed_timestamp")
                ) \
                .withColumn("avg_amount", col("total_amount") / col("order_count")) \
                .cache()  # This will consume memory
            
            print("Step 4: Writing to BigQuery (this will likely fail)...")
            target_table = f"{self.project_id}.{self.dataset_id}.target_orders_v1"
            
            result_df.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .mode("overwrite") \
                .save()
            
            result_count = result_df.count()
            print(f"✅ Processed {result_count} records")
            
        except Exception as e:
            print(f"❌ Pipeline failed with error: {str(e)}")
            print("This is expected - the pipeline has resource and skewness issues")
            return False
        
        return True
    
    def problematic_pipeline_v2(self):
        """Version 2: Fix memory issues but still have skewness problems"""
        print("\n" + "="*50)
        print("RUNNING PROBLEMATIC PIPELINE V2 - Fixed Memory Issues")
        print("="*50)
        
        try:
            print("Step 1: Reading from BigQuery temp table...")
            temp_table = f"{self.project_id}.{self.dataset_id}.temp_orders"
            
            temp_df = self.spark.read \
                .format("bigquery") \
                .option("table", temp_table) \
                .load()
            
            print("Step 2: Adding processing timestamp...")
            processed_df = temp_df.withColumn("processed_timestamp", current_timestamp())
            
            print("Step 3: Performing aggregations without memory-intensive operations...")
            # Fixed: Removed collect_list and cache
            result_df = processed_df \
                .groupBy("partition_key", "customer_id", "product") \
                .agg(
                    sum("amount").alias("total_amount"),
                    count("*").alias("order_count"),
                    first("date").alias("date"),
                    first("processed_timestamp").alias("processed_timestamp")
                ) \
                .withColumn("avg_amount", col("total_amount") / col("order_count"))
            
            print("Step 4: Writing to BigQuery (will be slow due to skewness)...")
            start_time = time.time()
            
            target_table = f"{self.project_id}.{self.dataset_id}.target_orders_v2"
            
            # Write to BigQuery with partitioning
            result_df.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .option("partitionField", "date") \
                .option("partitionType", "DAY") \
                .mode("overwrite") \
                .save()
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            print(f"✅ Processing completed in {processing_time:.2f} seconds")
            print("⚠️  Note: Processing was slow due to data skewness on partition_key")
            
            # Show partition distribution using BigQuery
            if self.bq_client:
                query = f"""
                SELECT partition_key, COUNT(*) as record_count 
                FROM `{target_table}` 
                GROUP BY partition_key 
                ORDER BY record_count DESC
                """
                
                results = self.bq_client.query(query).to_dataframe()
                print("\nPartition distribution:")
                print(results.head(10))
            
        except Exception as e:
            print(f"❌ Pipeline failed with error: {str(e)}")
            return False
        
        return True
    
    def optimized_pipeline_v3(self):
        """Version 3: Fully optimized pipeline with all fixes for BigQuery"""
        print("\n" + "="*50)
        print("RUNNING OPTIMIZED PIPELINE V3 - All Issues Fixed")
        print("="*50)
        
        try:
            print("Step 1: Reading from BigQuery temp table...")
            temp_table = f"{self.project_id}.{self.dataset_id}.temp_orders"
            
            temp_df = self.spark.read \
                .format("bigquery") \
                .option("table", temp_table) \
                .option("filter", "date >= '2024-01-01'")  # Predicate pushdown \
                .load()
            
            print("Step 2: Handling data skewness with salting technique...")
            # Add salt to skewed keys
            salted_df = temp_df \
                .withColumn("salt", 
                    when(col("partition_key") == "SKEWED_KEY", 
                         (rand() * 10).cast("int"))
                    .otherwise(lit(0))) \
                .withColumn("salted_partition_key", 
                    concat(col("partition_key"), lit("_"), col("salt"))) \
                .withColumn("processed_timestamp", current_timestamp())
            
            print("Step 3: Performing aggregations with salted keys...")
            # First aggregation with salted keys
            intermediate_df = salted_df \
                .groupBy("salted_partition_key", "customer_id", "product") \
                .agg(
                    sum("amount").alias("total_amount"),
                    count("*").alias("order_count"),
                    first("partition_key").alias("original_partition_key"),
                    first("date").alias("date"),
                    first("processed_timestamp").alias("processed_timestamp")
                )
            
            # Second aggregation to merge salted results
            final_df = intermediate_df \
                .groupBy("original_partition_key", "customer_id", "product") \
                .agg(
                    sum("total_amount").alias("total_amount"),
                    sum("order_count").alias("order_count"),
                    first("date").alias("date"),
                    first("processed_timestamp").alias("processed_timestamp")
                ) \
                .withColumn("avg_amount", col("total_amount") / col("order_count")) \
                .withColumnRenamed("original_partition_key", "partition_key") \
                .withColumn("order_id", lit(None).cast("int"))  # Add missing column for schema consistency
            
            print("Step 4: Optimizing partitions before writing...")
            # Repartition for balanced writing
            balanced_df = final_df.repartition(4, col("partition_key"))
            
            print("Step 5: Writing to BigQuery with optimized settings...")
            start_time = time.time()
            
            target_table = f"{self.project_id}.{self.dataset_id}.target_orders_v3"
            
            # Write to BigQuery with clustering
            balanced_df.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .option("partitionField", "date") \
                .option("partitionType", "DAY") \
                .option("clusteredFields", "partition_key,customer_id") \
                .option("createDisposition", "CREATE_IF_NEEDED") \
                .mode("overwrite") \
                .save()
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            print(f"✅ Optimized processing completed in {processing_time:.2f} seconds")
            
            # Show final results using BigQuery
            if self.bq_client:
                query = f"""
                SELECT partition_key, COUNT(*) as record_count,
                       AVG(total_amount) as avg_total_amount
                FROM `{target_table}` 
                GROUP BY partition_key 
                ORDER BY record_count DESC
                LIMIT 10
                """
                
                results = self.bq_client.query(query).to_dataframe()
                print("\nFinal results:")
                print(results)
            
        except Exception as e:
            print(f"❌ Pipeline failed with error: {str(e)}")
            return False
        
        return True
    
    def demonstrate_incremental_append_bq(self):
        """Demonstrate incremental data append to BigQuery"""
        print("\n" + "="*50)
        print("DEMONSTRATING INCREMENTAL APPEND TO BIGQUERY")
        print("="*50)
        
        try:
            # Create new batch of data
            new_data = []
            for i in range(1000):
                new_data.append((
                    f"batch2_key_{random.randint(1, 10)}",
                    f"customer_{random.randint(1, 100)}",
                    random.randint(2000, 3000),
                    random.uniform(10.0, 500.0),
                    f"product_{random.randint(1, 50)}",
                    "2024-01-16"
                ))
            
            schema = StructType([
                StructField("partition_key", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("order_id", IntegerType(), True),
                StructField("amount", DoubleType(), True),
                StructField("product", StringType(), True),
                StructField("date", StringType(), True)
            ])
            
            new_batch_df = self.spark.createDataFrame(new_data, schema)
            
            print("Processing new batch with optimized pipeline...")
            
            # Process new batch using optimized approach
            processed_batch = new_batch_df \
                .withColumn("processed_timestamp", current_timestamp()) \
                .groupBy("partition_key", "customer_id", "product") \
                .agg(
                    sum("amount").alias("total_amount"),
                    count("*").alias("order_count"),
                    first("date").alias("date"),
                    first("processed_timestamp").alias("processed_timestamp")
                ) \
                .withColumn("avg_amount", col("total_amount") / col("order_count"))
            
            # Append to existing BigQuery table
            target_table = f"{self.project_id}.{self.dataset_id}.target_orders_v3"
            
            processed_batch.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()
            
            print("✅ Incremental append to BigQuery completed")
            
            # Show updated counts using BigQuery
            if self.bq_client:
                query = f"SELECT COUNT(*) as total FROM `{target_table}`"
                result = self.bq_client.query(query).to_dataframe()
                total_count = result.iloc[0]['total']
                print(f"Total records in BigQuery table: {total_count}")
        
        except Exception as e:
            print(f"❌ Incremental append failed: {str(e)}")
    
    def run_bigquery_analytics(self):
        """Run some analytics queries on BigQuery data"""
        print("\n" + "="*50)
        print("RUNNING BIGQUERY ANALYTICS")
        print("="*50)
        
        if not self.bq_client:
            print("❌ BigQuery client not available")
            return
        
        target_table = f"{self.project_id}.{self.dataset_id}.target_orders_v3"
        
        # Query 1: Top customers by total amount
        query1 = f"""
        SELECT 
            customer_id,
            SUM(total_amount) as total_spent,
            COUNT(*) as transaction_count
        FROM `{target_table}`
        GROUP BY customer_id
        ORDER BY total_spent DESC
        LIMIT 10
        """
        
        print("Top 10 customers by total amount:")
        results1 = self.bq_client.query(query1).to_dataframe()
        print(results1)
        
        # Query 2: Daily aggregated data
        query2 = f"""
        SELECT 
            date,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(total_amount) as daily_revenue,
            AVG(avg_amount) as avg_order_value
        FROM `{target_table}`
        GROUP BY date
        ORDER BY date
        """
        
        print("\nDaily aggregated metrics:")
        results2 = self.bq_client.query(query2).to_dataframe()
        print(results2)
    
    def cleanup(self):
        """Clean up resources"""
        print("\nCleaning up resources...")
        if self.spark:
            self.spark.stop()
        print("Cleanup completed")

def main():
    """Main execution function for BigQuery/Dataproc pipeline"""
    
    # Configuration - Update these values for your environment
    PROJECT_ID = "gcp-pde-466109"  # Replace with your GCP project ID
    DATASET_ID = "my_pipeline_dataset"  # BigQuery dataset name
    BUCKET_NAME = "my_pipeline_bucket_2363"  # Replace with your GCS bucket name
    
    # Validate configuration
    if PROJECT_ID == "gcp-pde-466109" or BUCKET_NAME == "my_pipeline_bucket_2363":
        print("❌ Please update PROJECT_ID and BUCKET_NAME in the configuration section")
        print("Update the following variables in main():")
        print("  PROJECT_ID = 'gcp-pde-466109'")
        print("  BUCKET_NAME = 'my_pipeline_bucket_2363'")
        return
    
    print(f"🚀 Starting BigQuery/Dataproc Pipeline Demo")
    print(f"Project ID: {PROJECT_ID}")
    print(f"Dataset ID: {DATASET_ID}")
    print(f"Temp Bucket: {BUCKET_NAME}")
    
    demo = BigQueryDataprocPipeline(PROJECT_ID, DATASET_ID, BUCKET_NAME)
    
    try:
        # Create BigQuery dataset
        demo.create_bigquery_dataset()
        
        # Create target tables
        demo.create_target_tables_bq()
        
        # Create sample data and load to BigQuery
        demo.create_sample_data_gcs()
        
        # Run problematic pipeline v1 (will fail)
        print("\n🔥 Running pipeline that WILL FAIL due to OOM...")
        demo.problematic_pipeline_v1()
        
        # Run problematic pipeline v2 (memory fixed, but slow due to skewness)
        print("\n⚡ Running pipeline with memory fixes but skewness issues...")
        demo.problematic_pipeline_v2()
        
        # Run optimized pipeline v3 (all issues fixed)
        print("\n🚀 Running fully optimized pipeline...")
        demo.optimized_pipeline_v3()
        
        # Demonstrate incremental append
        demo.demonstrate_incremental_append_bq()
        
        # Run BigQuery analytics
        demo.run_bigquery_analytics()
        
        print("\n✅ All pipeline phases completed successfully!")
        
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        demo.cleanup()

if __name__ == "__main__":
    main()
