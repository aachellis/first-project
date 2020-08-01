from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Read from CSV File").getOrCreate()
df = spark.read.csv("customer_data.csv", header = True, inferSchema = True)
print("The count of CSV entry is: "+str(df.count()))
df.printSchema()
#print("Summary of the DataFrame...")
#df.summary().show()
df.show(3)
