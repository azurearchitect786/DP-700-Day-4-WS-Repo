# Fabric notebook source


# MARKDOWN ********************

# # Chapter 1 : Spark Dataframes and Operations Code

# MARKDOWN ********************

# Create Dataframe Operations

# CELL ********************

import pandas as pd
from datetime import datetime, date
from pyspark.sql import Row

data_df = spark.createDataFrame([
    Row(col_1=100, col_2=200., col_3='string_test_1', col_4=date(2023, 1, 1), col_5=datetime(2023, 1, 1, 12, 0)),
    Row(col_1=200, col_2=300., col_3='string_test_2', col_4=date(2023, 2, 1), col_5=datetime(2023, 1, 2, 12, 0)),
    Row(col_1=400, col_2=500., col_3='string_test_3', col_4=date(2023, 3, 1), col_5=datetime(2023, 1, 3, 12, 0))
])


# CELL ********************

display(data_df)

# CELL ********************

import pandas as pd
from datetime import datetime, date
from pyspark.sql import Row

data_df = spark.createDataFrame([
    Row(col_1=100, col_2=200., col_3='string_test_1', col_4=date(2023, 1, 1), col_5=datetime(2023, 1, 1, 12, 0)),
    Row(col_1=200, col_2=300., col_3='string_test_2', col_4=date(2023, 2, 1), col_5=datetime(2023, 1, 2, 12, 0)),
    Row(col_1=400, col_2=500., col_3='string_test_3', col_4=date(2023, 3, 1), col_5=datetime(2023, 1, 3, 12, 0))
], schema=' col_1 long, col_2 double, col_3 string, col_4 date, col_5 timestamp')


# CELL ********************

display(data_df)

# CELL ********************

import pandas as pd
from datetime import datetime, date
from pyspark.sql import Row

pandas_df = pd.DataFrame({
    'col_1': [100, 200, 400],
    'col_2': [200., 300., 500.],
    'col_3': ['string_test_1', 'string_test_2', 'string_test_3'],
    'col_4': [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)],
    'col_5': [datetime(2023, 1, 1, 12, 0), datetime(2023, 1, 2, 12, 0), datetime(2023, 1, 3, 12, 0)]
})
df = spark.createDataFrame(pandas_df)


# CELL ********************

display(df)

# CELL ********************

from datetime import datetime, date
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

rdd = spark.sparkContext.parallelize([
    (100, 200., 'string_test_1', date(2023, 1, 1), datetime(2023, 1, 1, 12, 0)),
    (200, 300., 'string_test_2', date(2023, 2, 1), datetime(2023, 1, 2, 12, 0)),
    (300, 400., 'string_test_3', date(2023, 3, 1), datetime(2023, 1, 3, 12, 0))
])
data_df = spark.createDataFrame(rdd, schema=['col_1', 'col_2', 'col_3', 'col_4', 'col_5'])

# MARKDOWN ********************

# How to View the Dataframes

# CELL ********************

data_df.show()

# CELL ********************

data_df.show(2)

# CELL ********************

data_df.printSchema()

# CELL ********************

data_df.show(2, vertical=False)

# CELL ********************

data_df.columns

# CELL ********************

data_df.count()

# CELL ********************

data_df.select('col_1', 'col_2', 'col_3').describe().show()

# CELL ********************

data_df.collect()

# CELL ********************

data_df.take(2)

# CELL ********************

data_df.tail(1)

# CELL ********************

data_df.head(2)

# CELL ********************

data_df.toPandas()

# MARKDOWN ********************

# How to do Data Manipulation - Rows and Columns

# CELL ********************

from pyspark.sql import Column

data_df.select(data_df.col_3).show()


# CELL ********************

display(data_df)

# CELL ********************

from pyspark.sql import functions as F
data_df = data_df.withColumn("col_6", F.lit("NA"))
data_df.show()

data_df_new = data_df.withColumns({'col_7': data_df.col_1 + 2, 'col_8': data_df.col_1 + 3})
data_df_new.show()

# CELL ********************

data_df = data_df.selectExpr("col_1","col_2","col_3","col_4","col_6")
data_df.show()


# CELL ********************

data_df = data_df.drop("col_5")
data_df.show()


# CELL ********************

data_df.withColumn("col_2", F.col("col_2") / 100).show()

# CELL ********************

data_df = data_df.withColumnRenamed("col_3", "any_name_of_your_choice")
data_df.show()


# CELL ********************

data_df.select("col_1","col_6").distinct().show()

# CELL ********************

data_df.select("col_1","col_6").dropDuplicates(["col_6"]).show()

# CELL ********************

data_df.select(F.countDistinct("col_2").alias("Total_Unique")).show()

# CELL ********************

from pyspark.sql.functions import upper

data_df.withColumn('upper_string_col', upper(data_df.any_name_of_your_choice)).show()


# CELL ********************

data_df.filter(data_df.col_1 == 100).show()

# CELL ********************

data_df.where(data_df.col_1 == 100).show()

# CELL ********************

data_df.filter((data_df.col_1 == 100)
		& (data_df.col_6 == 'NA')).show()


# CELL ********************

data_df.filter((data_df.col_1 == 100)
		| (data_df.col_2 == 300.00)).show()


# CELL ********************

list = [100, 200]
data_df.filter(data_df.col_1.isin(list)).show()


# CELL ********************

data_df.printSchema()

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType

data_df_2 = data_df.withColumn("col_4",col("col_4").cast(StringType())) \
    .withColumn("col_1",col("col_1").cast(IntegerType()))
data_df_2.printSchema()
data_df.show()



# CELL ********************

data_df_3 = data_df_2.selectExpr("cast(col_4 as date) as col_4",
    "cast(col_1 as long) as col_1")
data_df_3.printSchema()


# CELL ********************

data_df_4 = data_df_2.selectExpr("(col_1*2) as Bonus").show()

# CELL ********************

display(data_df_3)

# CELL ********************

data_df_3.createOrReplaceTempView("CastExample")

# CELL ********************

%sql
SELECT sum(col_1) as TotalSales from CastExample group by col_4

# CELL ********************

processed_frame=spark.sql('SELECT sum(col_1) as TotalSales from CastExample group by col_4')
display(processed_frame)

# CELL ********************


data_df_4 = spark.sql("SELECT DOUBLE(col_1), DATE(col_4) from CastExample")
data_df_4.printSchema()
data_df_4.show(truncate=False)


# CELL ********************

salary_data = [("John", "Field-eng", 3500), 
    ("Michael", "Field-eng", 4500), 
    ("Robert", None, None), 
    ("Maria", "Finance", 3500), 
    ("John", "Sales", 3000), 
    ("Kelly", "Finance", 3500), 
    ("Kate", "Finance", None), 
    ("Martin", None, 3500), 
    ("Kiran", "Sales", 2200), 
    ("Michael", "Field-eng", 4500) 
  ]
columns= ["Employee", "Department", "Salary"]
salary_data = spark.createDataFrame(data = salary_data, schema = columns)
salary_data.printSchema()
salary_data.show()


# CELL ********************

salary_data.dropna().show()

# CELL ********************

salary_data.describe().show()

# CELL ********************

salary_data = salary_data.fillna({'Salary': 3525.0})
salary_data.show()
salary_data.describe().show()


# CELL ********************

new_salary_data = salary_data.dropDuplicates(['Salary']).show()

# MARKDOWN ********************

# Using Aggregrates in a Dataframe

# CELL ********************

from pyspark.sql.functions import countDistinct, avg
salary_data.select(avg('Salary')).show()


# CELL ********************

salary_data.agg({'Salary':'count'}).show()

# CELL ********************

salary_data.select(countDistinct("Salary").alias("Distinct Salary")).show()

# CELL ********************

salary_data.agg({'Salary':'max'}).show() 

# CELL ********************

salary_data.agg({'Salary':'sum'}).show()

# CELL ********************

salary_data.orderBy("Salary").show()

# CELL ********************

salary_data.orderBy(salary_data["Salary"].desc()).show()
