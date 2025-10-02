# Fabric notebook source


# MARKDOWN ********************

# # Chapter 2: Advanced Operations in Spark Code

# CELL ********************

salary_data = [("John", "Field-eng", 3500), 
    ("Michael", "Field-eng", 4500), 
    ("Robert", None, 4000), 
    ("Maria", "Finance", 3500), 
    ("John", "Sales", 3000), 
    ("Kelly", "Finance", 3500), 
    ("Kate", "Finance", 3000), 
    ("Martin", None, 3500), 
    ("Kiran", "Sales", 2200), 
    ("Michael", "Field-eng", 4500) 
  ]
columns= ["Employee", "Department", "Salary"]
salary_data = spark.createDataFrame(data = salary_data, schema = columns)
salary_data.printSchema()
salary_data.show()


# CELL ********************

salary_data.groupby('Department').count().show()

# CELL ********************

salary_data.groupby('Department').avg().show()

# CELL ********************

from pyspark.sql.functions import col, round

salary_data.groupBy('Department')\
  .sum('Salary')\
  .withColumn('sum(Salary)',round(col('sum(Salary)'), 2))\
  .withColumnRenamed('sum(Salary)', 'Salary')\
  .orderBy('Department')\
  .show()


# CELL ********************

salary_data_with_id = [(1, "John", "Field-eng", 3500), \
    (2, "Robert", "Sales", 4000), \
    (3, "Maria", "Finance", 3500), \
    (4, "Michael", "Sales", 3000), \
    (5, "Kelly", "Finance", 3500), \
    (6, "Kate", "Finance", 3000), \
    (7, "Martin", "Finance", 3500), \
    (8, "Kiran", "Sales", 2200), \
  ]
columns= ["ID", "Employee", "Department", "Salary"]
salary_data_with_id = spark.createDataFrame(data = salary_data_with_id, schema = columns)
salary_data_with_id.show()


# CELL ********************

employee_data = [(1, "NY", "M"), \
    (2, "NC", "M"), \
    (3, "NY", "F"), \
    (4, "TX", "M"), \
    (5, "NY", "F"), \
    (6, "AZ", "F") \
  ]
columns= ["ID", "State", "Gender"]
employee_data = spark.createDataFrame(data = employee_data, schema = columns)
employee_data.show()


# CELL ********************

salary_data_with_id.join(employee_data,salary_data_with_id.ID ==  employee_data.ID,"inner").show()

# CELL ********************

salary_data_with_id.join(employee_data,salary_data_with_id.ID ==  employee_data.ID,"outer").show()

# CELL ********************

salary_data_with_id.join(employee_data,salary_data_with_id.ID ==  employee_data.ID,"left").show()

# CELL ********************

salary_data_with_id.join(employee_data,salary_data_with_id.ID ==  employee_data.ID,"right").show()

# CELL ********************

salary_data_with_id_2 = [(1, "John", "Field-eng", 3500), \
    (2, "Robert", "Sales", 4000), \
    (3, "Aliya", "Finance", 3500), \
    (4, "Nate", "Sales", 3000), \
  ]
columns2= ["ID", "Employee", "Department", "Salary"]

salary_data_with_id_2 = spark.createDataFrame(data = salary_data_with_id_2, schema = columns2)

salary_data_with_id_2.printSchema()
salary_data_with_id_2.show(truncate=False)



# CELL ********************

unionDF = salary_data_with_id.union(salary_data_with_id_2)
unionDF.show(truncate=False)


# MARKDOWN ********************

# Reading and Writing Data

# CELL ********************

display(salary_data_with_id)

# CELL ********************

salary_data_with_id.write.csv('salary_data.csv', mode='overwrite', header=True)

# CELL ********************



df_coming_from_csv=spark.read.csv('/salary_data.csv', header=True)

df_coming_from_csv.show()

# MARKDOWN ********************

# **Schema on read**

# CELL ********************

from pyspark.sql.types import *

filePath = '/salary_data.csv'
columns= ["ID", "State", "Gender"] 
schema = StructType([
      StructField("ID", IntegerType(),True),
  StructField("State",  StringType(),True),
  StructField("Gender",  StringType(),True)
])
 
read_data = spark.read.format("csv").option("header","true").schema(schema).load(filePath)
read_data.show()


# CELL ********************

salary_data_with_id.write.parquet('salary_data.parquet', mode='overwrite')

# CELL ********************


spark.read.parquet('/salary_data.parquet').show()


# CELL ********************

salary_data_with_id.write.orc('salary_data.orc', mode='overwrite')
spark.read.orc('/salary_data.orc').show()

# CELL ********************

salary_data_with_id.write.format("delta").save("/FileStore/tables/salary_data_with_id", mode='overwrite')
df = spark.read.load("/FileStore/tables/salary_data_with_id")
df.show()

