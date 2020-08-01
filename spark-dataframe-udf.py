from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

def catagory_on_age(age):
    if age == "30-40 years" or age == "40-50 years":
        return "Mid Aged"
    elif age == "20-30 years":
        return "Young"
    else:
        return "Old"

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Read from CSV File").getOrCreate()
df = spark.read.csv("customer_data.csv", header = True, inferSchema = True)
age_udf = f.udf(catagory_on_age,StringType())
df = df.withColumn("Age_category",age_udf(df["Avg_age"]))
df.show(13)
