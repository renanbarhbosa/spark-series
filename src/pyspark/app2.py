#import and set configs
from packaging.version import Version as LooseVersion
from pyspark.sql import SparkSession
builder = SparkSession.builder.appName("app")
builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
builder.getOrCreate()
print(builder)

#pandas on spark
import pyspark.pandas as ps
#import pandas as ps

#read file
get_device = ps.read_json("C:/intellij-projects/spark-series/docs/files/devices/*.json")
get_subscription = ps.read_json("C:/intellij-projects/spark-series/docs/files/subscriptions/*.json")

print(get_device)
print(get_subscription)

get_device.info()
get_subscription.info()

#get plan
get_device.spark.explain(mode="formatted")
get_subscription.spark.explain(mode="formatted")
