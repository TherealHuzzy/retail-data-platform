import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# ----------------------------------------------------------
# Initialize Glue
# ----------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args["JOB_NAME"], args)

# ----------------------------------------------------------
# Configuration
# ----------------------------------------------------------

BUCKET = "retail-data-platform-huzzy"

INPUT_PATH = f"s3://{BUCKET}/silver/customers/"

OUTPUT_PATH = f"s3://{BUCKET}/gold/dim_customers/"

# ----------------------------------------------------------
# Read Silver Customers
# ----------------------------------------------------------

df = spark.read.parquet(INPUT_PATH)

# ----------------------------------------------------------
# Select Dimension Columns
# ----------------------------------------------------------

dim_customers = (
    df.select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_city"),
        col("customer_state")
    )
    .dropDuplicates(["customer_id"])
)

# ----------------------------------------------------------
# Write Gold Dimension
# ----------------------------------------------------------

(
    dim_customers.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print("Customer Dimension created successfully.")

job.commit()