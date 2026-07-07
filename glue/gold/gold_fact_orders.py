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

ORDERS_PATH = f"s3://{BUCKET}/silver/orders/"

ORDER_ITEMS_PATH = f"s3://{BUCKET}/silver/order_items/"

PAYMENTS_PATH = f"s3://{BUCKET}/silver/payments/"

OUTPUT_PATH = f"s3://{BUCKET}/gold/fact_orders/"

# ----------------------------------------------------------
# Read Silver Tables
# ----------------------------------------------------------

orders = spark.read.parquet(ORDERS_PATH)

order_items = spark.read.parquet(ORDER_ITEMS_PATH)

payments = spark.read.parquet(PAYMENTS_PATH)

# ----------------------------------------------------------
# Join Orders + Order Items
# ----------------------------------------------------------

fact_orders = (

    orders.alias("o")

    .join(

        order_items.alias("oi"),

        col("o.order_id") == col("oi.order_id"),

        "left"

    )

)

# ----------------------------------------------------------
# Join Payments
# ----------------------------------------------------------

fact_orders = (

    fact_orders

    .join(

        payments.alias("p"),

        col("o.order_id") == col("p.order_id"),

        "left"

    )

)

# ----------------------------------------------------------
# Select Final Columns
# ----------------------------------------------------------

fact_orders = (

    fact_orders.select(

        col("o.order_id").alias("order_id"),

        col("o.customer_id"),

        col("oi.product_id"),

        col("oi.seller_id"),

        col("o.order_purchase_timestamp"),

        col("o.order_status"),

        col("oi.price"),

        col("oi.freight_value"),

        col("p.payment_value")

    )

)

# ----------------------------------------------------------
# Write Gold Fact
# ----------------------------------------------------------

fact_orders.write.mode("overwrite").parquet(OUTPUT_PATH)

print("Fact Orders created successfully.")

job.commit()