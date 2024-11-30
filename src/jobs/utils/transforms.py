from pyspark.sql import DataFrame
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window


def get_greatest_rank(df: DataFrame) -> DataFrame:
    """
    Get the ranking of users based on review count

    :param df: Input DataFrame
    :return: DataFrame with user ranking
    """
    # Define window specification
    window_spec = Window.orderBy(col("review_count").desc())

    # Add rank column
    return df.withColumn("user_rank", rank().over(window_spec))


def get_rate(df: DataFrame) -> DataFrame:
    """
    Calculate additional metrics for users

    :param df: Input DataFrame
    :return: DataFrame with additional computed columns
    """
    return df.withColumn(
        "engagement_rate",
        (col("useful") / col("review_count")).cast("float")
    )