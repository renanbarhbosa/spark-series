from pyspark.sql import DataFrame


def write_into_parquet(df: DataFrame, mode: str = "overwrite", location: str = None):
    """
    Write DataFrame to Parquet format

    :param df: DataFrame to write
    :param mode: Write mode (overwrite, append, etc.)
    :param location: Output directory path
    """
    if location is None:
        raise ValueError("Output location must be specified")

    df.write.mode(mode).parquet(location)