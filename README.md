
# 🛒 Retail Data Platform on AWS

## End-to-End Cloud Data Engineering Project
 
**Technologies:** AWS Glue • Amazon S3 • PySpark • Athena • CloudWatch • Tableau


# 📌 Executive Summary

This project demonstrates how to build a production-style retail analytics platform using AWS.
The pipeline ingests raw retail data into Amazon S3, transforms it through a Medallion Architecture,
validates data quality, orchestrates ETL jobs with AWS Glue Workflow, monitors execution using
CloudWatch, and exposes business-ready datasets through Athena for visualization in Tableau.


# 🎯 Business Problem

Retail businesses generate large volumes of transactional data from multiple operational systems.
The objective of this project is to centralize, transform, validate, monitor and visualize this data
to support reliable business intelligence and decision-making.


# 🏗️ Solution Architecture

<img width="1536" height="1024" alt="Architecture" src="https://github.com/user-attachments/assets/9017d938-3e89-4efa-a31e-8983aef15dce" />




Pipeline:

`CSV / Kaggle → Amazon S3 (Bronze) → AWS Glue ETL → Silver → Gold → Glue Crawler → Glue Data Catalog → Amazon Athena → Tableau Dashboard`

Monitoring:

`CloudWatch Metrics → CloudWatch Dashboard`


# ☁️ Technology Stack

| Layer | Technology |
|------|-------------|
| Cloud | Amazon Web Services |
| Storage | Amazon S3 |
| Processing | AWS Glue (PySpark) |
| Metadata | Glue Data Catalog |
| Query Engine | Amazon Athena |
| Orchestration | AWS Glue Workflow |
| Monitoring | Amazon CloudWatch |
| Visualization | Tableau |
| Programming | Python, SQL |


# 🗂️ Dataset

The solution uses the **Brazilian E-Commerce Public Dataset (Olist)** from Kaggle.

Included datasets:

- Customers
- Orders
- Order Items
- Products
- Sellers
- Reviews
- Payments
- Categories
- Geolocation


# 🥇 Medallion Architecture

### Bronze
- Raw source data
- Immutable storage
- Landing zone

### Silver
- Cleansed data
- Standardized schema
- Data quality improvements

### Gold
- Fact and Dimension tables
- Analytics-ready datasets
- Optimized for reporting


# ⚙️ ETL Workflow

1. Ingest raw data into Amazon S3 Bronze.
2. Transform Bronze → Silver.
3. Transform Silver → Gold.
4. Update Glue Data Catalog.
5. Run Data Quality Validation.
6. Publish CloudWatch Metrics.
7. Query Gold tables with Athena.
8. Build executive dashboards in Tableau.


# ✅ Data Quality

Implemented validation rules:

- Null value checks
- Duplicate detection
- Negative value checks
- Schema validation
- Record count validation

Quality metrics are published to **Amazon CloudWatch** for operational monitoring.


# 📊 Monitoring

CloudWatch monitors:

- Glue Job Status
- Failed Validation Rules
- Total Validation Rules
- Pipeline Runtime
- CloudWatch Logs

<img width="1111" height="550" alt="Screenshot 2026-07-10 at 6 45 10 PM" src="https://github.com/user-attachments/assets/cefb1ea1-2354-49eb-8668-f9d22362e3db" />



# 📈 Tableau Dashboard

The executive dashboard provides:

- Total Revenue
- Total Orders
- Total Customers
- Average Order Value
- Monthly Revenue Trend
- Revenue by State
- Top Product Categories
- Top Selling Cities

<img width="1367" height="722" alt="Screenshot 2026-07-10 at 6 42 20 PM" src="https://github.com/user-attachments/assets/1b8c8301-fce6-4f63-9ee6-52a6110f330c" />



# 💡 Key Achievements

- End-to-end AWS Data Pipeline
- Medallion Data Architecture
- Automated ETL
- Glue Workflow Orchestration
- Data Quality Framework
- CloudWatch Monitoring
- Interactive Tableau Dashboard


# 🚀 Future Enhancements

- Change Data Capture (CDC)
- EventBridge Scheduling
- SNS Alerts
- Terraform
- GitHub Actions CI/CD
- Apache Iceberg / Delta Lake


# 🏁 Conclusion

This project demonstrates practical cloud data engineering skills using AWS services and industry best practices.
It showcases the complete lifecycle of a modern analytics platform—from ingestion and transformation to monitoring and executive reporting.
