def read_files(spark, filename, file_type="parquet"):
    """
    Read files with flexible file type support

    :param spark: SparkSession
    :param filename: Path to the file
    :param file_type: Type of file to read (parquet, json, csv, etc.)
    :return: DataFrame
    """
    if file_type == "parquet":
        return spark.read.parquet(filename)
    elif file_type == "json":
        return spark.read.option("multiLine", True).json(filename)
    elif file_type == "csv":
        return spark.read.csv(filename, header=True, inferSchema=True)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")