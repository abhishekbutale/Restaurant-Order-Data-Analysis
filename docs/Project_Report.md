# Restaurant Order Data Analysis Using Apache Hadoop and PySpark



**Project By:** Abhishek Butale and Mahek Gidwani



## 1. Introduction

This project analyzes restaurant orders using Apache Hadoop for distributed storage and Apache Spark (PySpark) for data processing.



## 2. Objectives

- Analyze restaurant orders and revenue.

- Compare restaurant and cuisine performance.

- Analyze customer payment methods.

- Validate data quality.

- Store analysis results in Hadoop HDFS.



## 3. Technologies

- Python 3.11

- Apache Hadoop 3.4.2

- Hadoop Distributed File System (HDFS)

- Apache Spark / PySpark 3.4.4

- Java 8

- CSV datasets



## 4. Dataset

- Orders.csv: 500 records

- Restaurants.csv: 20 records



## 5. Methodology

1. Load CSV datasets from HDFS.

2. Inspect schemas and sample records.

3. Join orders with restaurant information.

4. Analyze restaurant-wise orders and revenue.

5. Analyze cuisine-wise orders and revenue.

6. Analyze payment methods.

7. Validate data quality and revenue totals.

8. Save analysis results to HDFS and export local CSV files.



## 6. Results

- Total orders: 500

- Total restaurants: 20

- Total revenue: INR 299,071

- Highest-revenue restaurant: Veer Restaurant — INR 19,168

- Highest-revenue cuisine: North Indian — INR 46,706

- Most-used payment method: Debit Card — 174 orders



## 7. Data Quality

- Missing values in Orders: 0

- Duplicate order IDs: 0

- Orders without a matching restaurant: 0

- Source revenue matches analysis revenue: Yes

- All orders matched: Yes



## 8. HDFS Input Paths

- /restaurant/input/Orders.csv

- /restaurant/input/Restaurants.csv



## 9. HDFS Output Paths

- /restaurant/output/restaurant_analysis

- /restaurant/output/cuisine_analysis

- /restaurant/output/payment_analysis



Each output directory contains a CSV part file and a _SUCCESS marker.



## 10. Conclusion

The project successfully analyzes 500 restaurant orders using PySpark, validates the results, and stores three analysis outputs in Hadoop HDFS.



## 11. Future Scope

A web dashboard can present revenue summaries, restaurant rankings, cuisine comparisons, and payment trends using the exported CSV files.



