# import session
from pyspark.sql import SparkSession
from pyspark import SparkConf

from utils.input import read_files
from utils.transforms import get_greatest_rank, get_rate
from utils.output import write_into_parquet


# main definitions & calls
def main():
    # Create Spark Session
    spark = SparkSession.builder.appName("etl-job-users").getOrCreate()

    # configs
    print(spark)
    print(SparkConf().getAll())
    spark.sparkContext.setLogLevel("ERROR")

    # extract
    users_filepath = "C:/intellij-projects/spark-series/docs/files/users/users_data_file.json"

    # Read JSON file with multiLine option
    df_users = spark.read.option("multiLine", True).json(users_filepath)

    # Show the dataframe
    df_users.show()
    df_users.printSchema()

    # transform
    df_rank = get_greatest_rank(df=df_users)
    df_rank.show()

    df_users_transformed = get_rate(df=df_users)

    # load
    output_path = "C:/intellij-projects/spark-series/docs/files/users/users_transformed"
    write_into_parquet(df=df_users_transformed, mode="overwrite", location=output_path)

    # Stop Spark Session
    spark.stop()


# entry point for pyspark etl app
if __name__ == "__main__":
    main()