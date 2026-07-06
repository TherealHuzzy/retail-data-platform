import sys
from awsglue.context import GlueContext
from awsglue.job import Job 
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Read Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Job 
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Bucket name 
bucket = "retail-data-platform-huzzy"

# List of datasets
datasets = [
    ("customers", "olist_customers_dataset.csv"),
    ("geolocation", "olist_geolocation_dataset.csv"),
    ("order_items", "olist_order_items_dataset.csv"),
    ("orders", "olist_orders_dataset.csv"),
    ("payments", "olist_order_payments_dataset.csv"),
    ("products", "olist_products_dataset.csv"),
    ("reviews", "olist_order_reviews_dataset.csv"),
    ("sellers", "olist_sellers_dataset.csv"),
    ("categories", "product_category_name_translation.csv")
    ]

# Loop through every dataset
for folder, filename in datasets:
    input_path = f"s3://{bucket}/raw/{folder}/{filename}"
    output_path = f"s3://{bucket}/bronze/{folder}/"
    
    print(f"Processing {filename}")
    
    df = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    )
    
    print(f"Rows: {df.count()}")
    
    (
        df.write.mode("overwrite").parquet(output_path)
    )

job.commit()
    