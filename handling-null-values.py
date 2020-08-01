from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Hnadling Null Value in a Dataframe").getOrCreate()
schema = StructType().add("ID","integer").add("Name","string").add("City","string").add("Associated With","string")
df_na = spark.createDataFrame([(1,"John Doe","Kolkata",None),(2,"Mira Hituni","Mumbai","Alex Ray"),(3,"Alex Ray","Delhi","John Doe"),(4,"Tom Maitu","Bangalore",None)],schema=schema)
df_na.printSchema()
df_na.show()
print("After replacing null Values....")
df_na.fillna("NA").show()
