import sys
import datetime
import boto3

from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

cloudwatch = boto3.client("cloudwatch")

BUCKET = "s3://retail-data-platform-huzzy"

DATASETS = {
    "customers": f"{BUCKET}/bronze/customers/",
    "orders": f"{BUCKET}/bronze/orders/",
    "order_items": f"{BUCKET}/bronze/order_items/",
    "payments": f"{BUCKET}/bronze/payments/",
    "reviews": f"{BUCKET}/bronze/reviews/",
    "products": f"{BUCKET}/bronze/products/",
    "sellers": f"{BUCKET}/bronze/sellers/",
    "categories": f"{BUCKET}/bronze/categories/",
    "geolocation": f"{BUCKET}/bronze/geolocation/"
}

REPORT_PATH = f"{BUCKET}/quality-reports/"

validation_results = []
critical_failures = []


def add_result(dataset, rule, failed_rows):
    status = "PASS" if failed_rows == 0 else "FAIL"

    validation_results.append({
        "dataset": dataset,
        "rule": rule,
        "status": status,
        "failed_rows": int(failed_rows),
        "validation_time": str(datetime.datetime.now())
    })

    if failed_rows > 0:
        critical_failures.append(f"{dataset} - {rule}: {failed_rows}")


def read_dataset(name):
    print(f"Reading {name}")
    return spark.read.parquet(DATASETS[name])


def check_null(df, dataset, column):
    failed = df.filter(col(column).isNull()).count()
    add_result(dataset, f"Null check: {column}", failed)


def check_negative(df, dataset, column):
    failed = df.filter(col(column) < 0).count()
    add_result(dataset, f"Negative value check: {column}", failed)


def check_duplicates(df, dataset, columns):
    failed = (
        df.groupBy(columns)
        .count()
        .filter(col("count") > 1)
        .count()
    )
    add_result(dataset, f"Duplicate check: {columns}", failed)


print("=" * 60)
print("DATA QUALITY VALIDATION STARTED")
print("=" * 60)

orders = read_dataset("orders")
customers = read_dataset("customers")
order_items = read_dataset("order_items")
payments = read_dataset("payments")
reviews = read_dataset("reviews")
products = read_dataset("products")
sellers = read_dataset("sellers")

# Orders
check_duplicates(orders, "orders", ["order_id"])
check_null(orders, "orders", "order_id")
check_null(orders, "orders", "customer_id")
check_null(orders, "orders", "order_purchase_timestamp")

invalid_status = (
    orders
    .filter(~col("order_status").isin(
        "approved",
        "created",
        "delivered",
        "invoiced",
        "processing",
        "shipped",
        "canceled",
        "unavailable"
    ))
    .count()
)

add_result("orders", "Invalid order status", invalid_status)

# Customers
check_duplicates(customers, "customers", ["customer_id"])
check_null(customers, "customers", "customer_id")
check_null(customers, "customers", "customer_unique_id")

# Order Items
check_duplicates(order_items, "order_items", ["order_id", "order_item_id"])
check_null(order_items, "order_items", "order_id")
check_null(order_items, "order_items", "product_id")
check_null(order_items, "order_items", "seller_id")
check_negative(order_items, "order_items", "price")
check_negative(order_items, "order_items", "freight_value")

# Payments
check_null(payments, "payments", "order_id")
check_null(payments, "payments", "payment_type")
check_negative(payments, "payments", "payment_value")
check_negative(payments, "payments", "payment_installments")

# Reviews
check_null(reviews, "reviews", "review_id")
check_null(reviews, "reviews", "order_id")

invalid_review_score = (
    reviews
    .filter((col("review_score") < 1) | (col("review_score") > 5))
    .count()
)

add_result("reviews", "Invalid review score", invalid_review_score)

# Products
check_duplicates(products, "products", ["product_id"])
check_null(products, "products", "product_id")
check_negative(products, "products", "product_weight_g")

# Sellers
check_duplicates(sellers, "sellers", ["seller_id"])
check_null(sellers, "sellers", "seller_id")

# Generate report
report_df = spark.createDataFrame(validation_results)

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
report_output_path = f"{REPORT_PATH}{timestamp}/"

report_df.coalesce(1).write.mode("overwrite").option("header", True).csv(report_output_path)

failed_rules = report_df.filter(col("status") == "FAIL").count()
total_rules = report_df.count()

print("=" * 60)
print("DATA QUALITY SUMMARY")
print("=" * 60)
print(f"Total Rules Checked: {total_rules}")
print(f"Failed Rules: {failed_rules}")
print(f"Report Path: {report_output_path}")
print("=" * 60)

# Send metrics to CloudWatch
cloudwatch.put_metric_data(
    Namespace="RetailDataPipeline",
    MetricData=[
        {
            "MetricName": "DataQualityFailedRules",
            "Value": failed_rules,
            "Unit": "Count"
        },
        {
            "MetricName": "DataQualityTotalRules",
            "Value": total_rules,
            "Unit": "Count"
        }
    ]
)

if failed_rules > 0:
    print("DATA QUALITY WARNING")
    print(f"Failed rules found: {failed_rules}")

    for failure in critical_failures:
        print(failure)

    print("Data quality report has been written to S3.")
    print("Pipeline will continue for demo/portfolio purposes.")

else:
    print("DATA QUALITY PASSED")

job.commit()