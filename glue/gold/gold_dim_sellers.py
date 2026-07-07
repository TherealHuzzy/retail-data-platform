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

INPUT_PATH = f"s3://{BUCKET}/silver/sellers/"

OUTPUT_PATH = f"s3://{BUCKET}/gold/dim_sellers/"

# ----------------------------------------------------------
# Read Sellers
# ----------------------------------------------------------

df = spark.read.parquet(INPUT_PATH)

# ----------------------------------------------------------
# Build Seller Dimension
# ----------------------------------------------------------

dim_sellers = (

    df.select(

        col("seller_id"),

        col("seller_city"),

        col("seller_state")

    )

    .dropDuplicates(["seller_id"])

)

# ----------------------------------------------------------
# Write Gold Dimension
# ----------------------------------------------------------

dim_sellers.write.mode("overwrite").parquet(OUTPUT_PATH)

print("Seller Dimension created successfully.")

job.commit()