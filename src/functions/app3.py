from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("py-functions") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

loc_users = "C:/intellij-projects/spark-series/docs/files/users/users_data_file.json"
loc_business = "C:/intellij-projects/spark-series/docs/files/business/business_data_file.json"

df_users = spark.read.json(loc_users)
df_business = spark.read.json(loc_business)

df_users.printSchema()
df_users.show()

# select
df_users.select("user_id", "name", "average_stars", "review_count", "fans", "yelping_since").show()

# lit and withColumn
df_users.withColumn("rank", when(col("useful") >= 500, lit("high")).otherwise(lit("low"))).show()

# monotonically_increasing_id
df_users.select(monotonically_increasing_id().alias("event"), "name").show(2)

# greatest
df_users.select(
    "name",
    greatest("compliment_cool", "compliment_cute", "compliment_funny", "compliment_hot").alias("highest_rate")
).show()

# expr
df_users.select(expr("CASE WHEN useful >= 500 THEN concat('high', ' ') ELSE 'low' END").alias("score")).show()

# round
df_users.select(col("average_stars"), round("average_stars", 0)).show()

# current_date and current_timestamp
df_users.select(current_date(), current_timestamp()).show()

# year
df_users.select(year(current_timestamp()).alias("year")).show()

# transform function
def verify_rank(df):
    return df.withColumn("rank", when(col("useful") >= 500, lit("high")).otherwise(lit("low")))

df_users.transform(verify_rank).show()

# avg
df_users.select(avg(col("average_stars"))).show()
