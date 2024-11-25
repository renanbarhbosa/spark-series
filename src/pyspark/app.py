#import libraries and init spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(spark)

#load data
df_device = spark.read.json("C:/intellij-projects/spark-series/docs/files/devices/*.json")

#show data
#df_device.show()

#schema
df_device.printSchema()

#columns
print(df_device.columns)

#rows
print(df_device.count())

#select columns
df_device.select("manufacturer", "model", "platform").show()
df_device.selectExpr("manufacturer", "model", "platform as type").show()

#filter
df_device.filter(df_device.manufacturer == "Thomas-Chung").show()

#group by
df_device.groupby("manufacturer").count().show()

df_grouped_manufacturer = df_device.groupby("manufacturer").count()
df_grouped_manufacturer.show()
