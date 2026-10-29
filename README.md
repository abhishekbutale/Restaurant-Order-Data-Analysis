# Restaurant Order Data Analysis Using Apache Hadoop and PySpark



## 1. Project Overview

This project analyzes restaurant order data using Apache Hadoop HDFS for distributed storage and Apache Spark (PySpark) for data processing and analysis.



## 2. Technologies Used

- Apache Hadoop 3.4.2

- Hadoop Distributed File System (HDFS)

- Apache Spark / PySpark 3.4.4

- Python 3.11

- Java 8

- CSV datasets



## 3. Dataset Information

- Orders.csv: 500 order records

- Restaurants.csv: 20 restaurant records



## 4. Project Workflow

1. Store input datasets in Hadoop HDFS.

2. Read CSV files using PySpark.

3. Join orders with restaurant information.

4. Analyze restaurant-wise revenue and orders.

5. Analyze cuisine-wise revenue and orders.

6. Analyze payment methods.

7. Validate data quality and revenue totals.

8. Save analysis results to HDFS.



## 5. Analysis Results

- Total orders: 500

- Total restaurants: 20

- Total revenue: ₹299,071

- Highest-revenue restaurant: Veer Restaurant

- Highest-revenue cuisine: North Indian

- Most-used payment method: Debit Card



## 6. Data Quality Validation

- Missing values: 0

- Duplicate order IDs: 0

- Orders without matching restaurants: 0

- Source revenue matches analysis revenue: Yes

- All orders matched after joining: Yes



## 7. HDFS Locations



Input:

- /restaurant/input/Orders.csv

- /restaurant/input/Restaurants.csv



Output:

- /restaurant/output/restaurant_analysis

- /restaurant/output/cuisine_analysis

- /restaurant/output/payment_analysis



Each output directory contains a CSV part file and a _SUCCESS marker.



## 8. How to Run



Start Hadoop HDFS:



```cmd

cd /d C:\\hadoop\\sbin

start-dfs.cmd

```



Run the analysis from another CMD window:



```cmd

cd /d C:\\RestaurantProject

py -3.11 restaurant_analysis.py

```



## 9. Conclusion

The project successfully processes restaurant order data using PySpark, validates the results, and stores three analysis outputs in Hadoop HDFS.



