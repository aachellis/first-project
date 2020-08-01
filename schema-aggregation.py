from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Read from CSV File").getOrCreate()
df = spark.read.csv("customer_data.csv", header = True, inferSchema = True)
print("Aggregration of all the applied columns....")
for col in df.columns:
    if col != "Avg_Salary":
        print("Aggregration for Column: "+str(col))
        df.groupBy(col).count().orderBy("count", desc=False).show(truncate = False)
