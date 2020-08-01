from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf,PandasUDFType

def scaled_salary(salary):
    min_sal = 1361
    max_sal = 48919896
    return (salary - min_sal)/(max_sal - min_sal)

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Read from CSV File").getOrCreate()
df = spark.read.csv("customer_data.csv", header = True, inferSchema = True)
scaling_udf = pandas_udf(scaled_salary,DoubleType())
df = df.withColumn("Scaled_salary",scaling_udf(df["Avg_salary"]))
df.show(15)
