from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Read from CSV File").getOrCreate()
df = spark.read.csv("customer_data.csv", header = True, inferSchema = True)
print("Making summary table with agreegrated columns.")
schema = StructType().add("Customer_Subtype","string").add("Number_of_Houses","integer").add("Avg_size_Household","integer").add("Avg_Age","string").add("Customer_main_Type","string").add("label","string")
#Making aggregrated table for each entry
customer_subtype_agg = df.groupBy("Customer_subtype").count()
number_of_houses_agg = df.groupBy("Number_of_houses").count()
avg_house_household_agg = df.groupBy("Avg_size_household").count()
avg_age_agg = df.groupBy("Avg_age").count()
customer_main_type_agg = df.groupBy("Customer_main_type").count()
label_agg = df.groupBy("label").count()
#Inserting each agreegrated results counts for summary purposes into a tuple.
values_tuple = (customer_subtype_agg.count(),number_of_houses_agg.count(),avg_house_household_agg.count(),avg_age_agg.count(),customer_main_type_agg.count(),label_agg.count())
#Uniting all into a single dataframe, which will be the result.
result_agg = spark.createDataFrame([values_tuple],schema = schema)
result_agg.show()
