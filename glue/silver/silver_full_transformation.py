import sys
import time 
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job 
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StringType

# Read Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Job 
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
bucket = "retail-data-platform-huzzy"

datasets = [
    "customers",
    "geolocation",
    "order_items",
    "orders",
    "payments",
    "products",
    "reviews",
    "sellers",
    "categories"
    ]
    
# --------------------------------------------------------
# Required Columns 
# --------------------------------------------------------

REQUIRED_COLUMNS = {
    "customers": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
        ],
    
    "orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp"
        ],
        
    "order_items": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value"
        ],
    
    "payments": [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
        ],
    
    "reviews": [
        "review_id",
        "order_id",
        "review_score"
        ],
        
    "products": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_lenght_cm",
        "product_height_cm",
        "product_width_cm"
        ],
    
    "sellers": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
        ],
        
    "categories": [
        "product_category_name",
        "product_category_name_english"
        ],
        
    "geolocation": [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state"
        ]
}

# --------------------------------------------------------
# Metrics
# --------------------------------------------------------
    
job_metrics = []
    
# --------------------------------------------------------
# Logger
# --------------------------------------------------------

def log(message):
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    

# --------------------------------------------------------
# Schema Validation 
# --------------------------------------------------------

def validate_schema(dataset, df):
    
    expected_columns = REQUIRED_COLUMNS.get(dataset, [])
    
    missing_columns = [
        column
        for column in expected_columns
        if column not in df.columns
        ]
        
    if len(missing_columns) > 0:
        log(f"Schema validation failed for {dataset}")
            
        for column in missing_columns:
            log(f"Missing column: {column}")
                
        return False
        
    return True

# --------------------------------------------------------
# Generic Cleaning Function 
# --------------------------------------------------------
    
def generic_clean(df):
    # Remove duplicate rows
    df = df.dropDuplicates()
    
    # Remove rows where every column is null
    df = df.na.drop(how="all")
    
    # Trim every string column
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(field.name, trim(col(field.name)))
    return df


# --------------------------------------------------------
# Dataset Metrics  
# --------------------------------------------------------
    
def record_metrics(
    dataset,
    rows_before,
    rows_after,
    status,
    duration
):
    job_metrics.append({
        
        "dataset": dataset,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "rows_removed": rows_before - rows_after,
        "status": status,
        "duration": round(duration, 2)
    })
        

# --------------------------------------------------------
# Dataset Specific Rules 
# --------------------------------------------------------
    
def apply_business_rules(dataset, df):
        
    if dataset == "customers":
            
        df = df.dropDuplicates(["customer_id"])
        df = df.dropna(subset=["customer_id", "customer_unique_id"])
            
    elif dataset == "orders":
            
        df = df.dropDuplicates(["order_id"])
        df = df.dropna(subset=["order_id","customer_id"])
        df = df.filter(col("order_status").isNotNull())
            
    elif dataset == "order_items":
            
        df = df.dropDuplicates(["order_id", "order_item_id"])
        df = df.dropna(subset=["order_id", "product_id", "seller_id"])
        df = df.filter(col("price") > 0)
        df = df.filter(col("freight_value") >= 0)
    
    elif dataset == "payments":
        df = df.dropDuplicates(["order_id", "payment_sequential"])
        df = df.filter(col("payment_value") > 0)
        df = df.filter(col("payment_installments") > 0)
        
            
    elif dataset == "reviews":
            
        df = df.filter(
            (col("review_score") >= 1) &
            (col("review_score") <= 5)
            )
            
        df = df.dropDuplicates(["review_id"])
            
    elif dataset == "products":
        
        df = df.dropDuplicates(["product_id"])    
        df = df.filter(col("product_weight_g") > 0)
        df = df.filter(col("product_lenght_cm") > 0)
        df = df.filter(col("product_height_cm") > 0)
        df = df.filter(col("product_width_cm") > 0)
            
    elif dataset == "sellers":
            
        df = df.dropDuplicates(["seller_id"])
        df = df.dropna(subset=["seller_id"])
            
    elif dataset == "categories":
        df = df.dropDuplicates()
        df = df.dropna()
            
    elif dataset == "geolocation":
            
        df = df.dropDuplicates([
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng"
                ])
        df = df.dropna(subset=["geolocation_zip_code_prefix"])
    return df
        
# --------------------------------------------------------
# Main Processing Loop 
# --------------------------------------------------------
for dataset in datasets:
        
    start_time = time.time()
    
    log("=" * 70)
    log(f"Processing dataset: {dataset}")
        
    try:
        input_path = f"s3://{bucket}/bronze/{dataset}/"
        output_path = f"s3://{bucket}/silver/{dataset}/"
        
        df = spark.read.parquet(input_path)
        
        # Validate schema
        if not validate_schema(dataset, df):
            record_metrics(
                dataset,
                0,
                0,
                "SCHEMA FAILED",
                time.time() - start_time
                )
                
            continue
            
            rows_before = df.count()
            
            # Generic cleaning
            df = generic_clean(df)
            
            # Dataset Business Rules 
            df = apply_business_rules(dataset, df)
            
            rows_after = df.count()
             
            (df.write.mode("overwrite").parquet(output_path))
            
            duration = time.time() - start_time
            
            record_metrics(
                dataset,
                rows_before,
                rows_after,
                "SUCCESS",
                duration
                )
            
            log(f"Rows Before  : {rows_before}")
            log(f"Rows After   : {rows_after}")
            log(f"Rows Removed : {rows_before - rows_after}")
            log(f"Duration     : {round(duration, 2)} sec")
            log(f"{dataset} completed successfully.")
        
    except Exception as e:
            
        duration = time.time() - start_time
            
        record_metrics(
        dataset,
        rows_before,
        rows_after,
        "FAILED",
        duration 
            )
             
        log(f"ERROR processing {dataset}")
        log(str(e))
        print(f"{dataset} written successfully.")
             
    except Exception as e:
            
        print(f"ERROR processing {dataset}")
            
        print(str(e))

# --------------------------------------------------------
# Job Summary
# --------------------------------------------------------

log("=" * 70)
log("Silver Transformation Summary")
log("=" * 70)

for metric in job_metrics:
    log(f"Dataset           : {metric['dataset']}")
    log(f"Status            : {metric['status']}")
    log(f"Rows Read         : {metric['rows_before']}")
    log(f"Rows Written      : {metric['rows_after']}")
    log(f"Rows Removed      : {metric['rows_removed']}")
    log(f"Duration          : {metric['duration']} sec")
    log("-" * 70)

successful_jobs = len(
    [m for m in job_metrics if m["status"] == "SUCCESS"]
    )

failed_jobs = len(
    [m for m in job_metrics if m["status"] != "SUCCESS"]
    )

total_rows_read = sum(
    m["rows_before"] for m in job_metrics
    )
    
total_rows_written = sum(
    m["rows_after"] for m in job_metrics
    )
    
total_rows_removed = sum(
    m["rows_removed"] for m in job_metrics
    )
    
log("=" * 70)
log("JOB SUMMARY")
log("=" * 70)
log(f"Datasets Processed  : {len(datasets)}")
log(f"Successful Jobs     : {successful_jobs}")
log(f"Failed Jobs         : {failed_jobs}")
log(f"Rows Read           : {total_rows_read}")
log(f"Rows Written        : {total_rows_written}")
log(f"Rows Removed        : {total_rows_removed}")
log("=" * 70)
log("Silver Transformation Completed Successfully")
log("=" * 70)

job.commit()