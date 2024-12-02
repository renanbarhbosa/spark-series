from pyspark.sql import SparkSession
from pyspark import SparkConf

from utils.input import read_files  # Certifique-se de que os módulos existem e estão no path correto
from utils.transforms import get_greatest_rank, get_rate
from utils.output import write_into_parquet


# Função principal do ETL
def main():
    # Criação da Spark Session com configurações adequadas
    spark = SparkSession.builder \
        .appName("etl-job-users") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # Configurações do Spark
    print(spark)  # Exibe informações sobre a Spark Session
    print(list(SparkConf().getAll()))  # Transforma a saída para uma lista e exibe as configurações atuais
    spark.sparkContext.setLogLevel("ERROR")  # Ajusta o nível de log para "ERROR"

    # Caminho do arquivo de entrada
    users_filepath = "C:/intellij-projects/spark-series/docs/files/users/users_data_file.json"

    # Extração: Leitura do arquivo JSON com a opção multiLine habilitada
    try:
        df_users = spark.read.option("multiLine", True).json(users_filepath)
    except Exception as e:
        print(f"Erro ao ler o arquivo JSON: {e}")
        spark.stop()
        return

    # Visualização inicial dos dados
    df_users.show(truncate=False)
    df_users.printSchema()

    # Transformações
    try:
        df_rank = get_greatest_rank(df=df_users)
        df_rank.show(truncate=False)

        df_users_transformed = get_rate(df=df_users)
        df_users_transformed.show(truncate=False)
    except Exception as e:
        print(f"Erro durante as transformações: {e}")
        spark.stop()
        return

    # Carregamento
    output_path = "C:/intellij-projects/spark-series/docs/files/users/users_transformed"
    try:
        write_into_parquet(df=df_users_transformed, mode="overwrite", location=output_path)
    except Exception as e:
        print(f"Erro ao salvar os dados no formato Parquet: {e}")
    finally:
        # Encerramento da Spark Session
        spark.stop()


# Ponto de entrada para o script ETL
if __name__ == "__main__":
    main()
