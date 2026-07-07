import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    to_date,
    dayofmonth,
    month,
    year,
    quarter,
    weekofyear,
    date_format
)

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

INPUT_PATH = f"s3://{BUCKET}/silver/orders/"

OUTPUT_PATH = f"s3://{BUCKET}/gold/dim_date/"

# ----------------------------------------------------------
# Read Orders
# ----------------------------------------------------------

orders = spark.read.parquet(INPUT_PATH)

# ----------------------------------------------------------
# Build Date Dimension
# ----------------------------------------------------------

dim_date = (

    orders

    .select(
        to_date(col("order_purchase_timestamp")).alias("date")
    )

    .dropDuplicates()

    .withColumn("day", dayofmonth(col("date")))

    .withColumn("month", month(col("date")))

    .withColumn("month_name", date_format(col("date"), "MMMM"))

    .withColumn("quarter", quarter(col("date")))

    .withColumn("year", year(col("date")))

    .withColumn("week", weekofyear(col("date")))

    .withColumn("weekday", date_format(col("date"), "EEEE"))

)

# ----------------------------------------------------------
# Write Gold Dimension
# ----------------------------------------------------------

dim_date.write.mode("overwrite").parquet(OUTPUT_PATH)

print("Date Dimension created successfully.")

job.commit()