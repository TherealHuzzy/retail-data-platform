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

PRODUCTS_PATH = f"s3://{BUCKET}/silver/products/"

CATEGORIES_PATH = f"s3://{BUCKET}/silver/categories/"

OUTPUT_PATH = f"s3://{BUCKET}/gold/dim_products/"

# ----------------------------------------------------------
# Read Silver Tables
# ----------------------------------------------------------

products = spark.read.parquet(PRODUCTS_PATH)

categories = spark.read.parquet(CATEGORIES_PATH)

# ----------------------------------------------------------
# Join Products to Category Translation
# ----------------------------------------------------------

dim_products = (

    products.alias("p")

    .join(

        categories.alias("c"),

        col("p.product_category_name") == col("c.product_category_name"),

        "left"

    )

    .select(

        col("p.product_id"),

        col("p.product_category_name"),

        col("c.product_category_name_english"),

        col("p.product_weight_g"),

        col("p.product_length_cm"),

        col("p.product_height_cm"),

        col("p.product_width_cm")

    )

    .dropDuplicates(["product_id"])

)

# ----------------------------------------------------------
# Write Gold Dimension
# ----------------------------------------------------------

dim_products.write.mode("overwrite").parquet(OUTPUT_PATH)

print("Product Dimension created successfully.")

job.commit()