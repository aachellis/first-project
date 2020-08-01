from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Read from CSV File").getOrCreate()
df = spark.read.csv("customer_data.csv", header = True, inferSchema = True)
#print("Average of the Avarage Salary for a Customer Type and Max Salary....")
#df.groupBy("Customer_main_type").agg(f.avg("Avg_salary"),f.max("Avg_salary")).show()
print("Demonstration of Collect-Set and Collect-List...")
df.groupBy("Customer_subtype").agg(f.collect_set("Number_of_houses")).show()
df.groupBy("Customer_subtype").agg(f.collect_list("Number_of_houses")).show()
