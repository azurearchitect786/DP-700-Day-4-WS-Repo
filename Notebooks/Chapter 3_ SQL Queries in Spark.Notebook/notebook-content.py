# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Chapter 3: SQL Queries in Spark

# MARKDOWN ********************

# Change done by me in the Notebook in Dev which was earlier clean deployed to Prod.

# CELL ********************

salary_data_with_id = [(1, "John", "Field-eng", 3500, 40), \
    (2, "Robert", "Sales", 4000, 38), \
    (3, "Maria", "Finance", 3500, 28), \
    (4, "Michael", "Sales", 3000, 20), \
    (5, "Kelly", "Finance", 3500, 35), \
    (6, "Kate", "Finance", 3000, 45), \
    (7, "Martin", "Finance", 3500, 26), \
    (8, "Kiran", "Sales", 2200, 35), \
  ]
columns= ["ID", "Employee", "Department", "Salary", "Age"]
salary_data_with_id = spark.createDataFrame(data = salary_data_with_id, schema = columns)
salary_data_with_id.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

salary_data_with_id.write.format("csv").mode("overwrite").option("header", "true").save("salary_data.csv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

csv_data = spark.read.csv('/salary_data.csv', header=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

csv_data.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Perform transformations on the loaded data 
processed_data = csv_data.filter(csv_data["Salary"] > 3000) 
# Save the processed data as a table 
processed_data.createOrReplaceTempView("high_salary_employees") 
# Perform SQL queries on the saved table 
results = spark.sql("SELECT * FROM high_salary_employees ") 
results.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save the processed data as a view 
salary_data_with_id.createOrReplaceTempView("employees") 
#Apply filtering on data
filtered_data = spark.sql("SELECT Employee, Department, Salary, Age FROM employees WHERE age > 30") 
# Display the results 
filtered_data.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Perform an aggregation to calculate the average salary 
average_salary = spark.sql("SELECT AVG(Salary) AS average_salary FROM employees") 
# Display the average salary 
average_salary.show() 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sort the data based on the salary column in descending order 
sorted_data = spark.sql("SELECT * FROM employees ORDER BY Salary DESC") 
# Display the sorted data 
sorted_data.show() 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sort the data based on the salary column in descending order 
filtered_data = spark.sql("SELECT Employee, Department, Salary, Age FROM employees WHERE age > 30 AND Salary > 3000 ORDER BY Salary DESC") 
# Display the results 
filtered_data.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Group the data based on the Department column and take average salary for each department  
grouped_data = spark.sql("SELECT Department, avg(Salary) FROM employees GROUP BY Department") 
# Display the results 
grouped_data.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Perform grouping and multiple aggregations  
aggregated_data = spark.sql("SELECT Department, sum(Salary) AS total_salary, max(Salary) AS max_salary FROM employees GROUP BY Department") 

# Display the results 
aggregated_data.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum

# Define the window specification
window_spec = Window.partitionBy("Department").orderBy("Age")

# Calculate the cumulative sum using window function
df_with_cumulative_sum = salary_data_with_id.withColumn("cumulative_sum", sum(col("Salary")).over(window_spec))

# Display the result
df_with_cumulative_sum.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define a UDF to capitalize a string
capitalize_udf = udf(lambda x: x.upper(), StringType())

# Apply the UDF to a column
df_with_capitalized_names = salary_data_with_id.withColumn("capitalized_name", capitalize_udf("Employee"))

# Display the result
df_with_capitalized_names.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define a UDF to capitalize a string
capitalize_udf = udf(lambda x: x.upper(), StringType())

# Apply the UDF to a column
df_with_capitalized_names = salary_data_with_id.withColumn("capitalized_name", capitalize_udf("Employee"))

# Display the result
df_with_capitalized_names.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('long')
def pandas_plus_one(series: pd.Series) -> pd.Series:
    # Simply plus one by using pandas Series.
    return series + 1

salary_data_with_id.select(pandas_plus_one(salary_data_with_id.Salary)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

@pandas_udf("integer")
def add_one(s: pd.Series) -> pd.Series:
    return s + 1

spark.udf.register("add_one", add_one)
spark.sql("SELECT add_one(Salary) FROM employees").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
