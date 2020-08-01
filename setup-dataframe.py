from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("First Dataframe Programme").getOrCreate()
schema = StructType().add("ID","integer").add("Name","string").add("City","string").add("No. of members","integer")
df = spark.createDataFrame([(1,"John Doe","Kolkata",5),(2,"Mira Hituni","Mumbai",4),(3,"Alex Ray","Delhi",7),(4,"Tom Maitu","Bangalore",5)],schema=schema)
df.printSchema()
df.show()
