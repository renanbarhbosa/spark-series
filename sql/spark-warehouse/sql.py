from pyspark.sql import SparkSession

# Iniciar SparkSession
spark = SparkSession.builder.appName("Spark SQL Example").getOrCreate()

# Importar os dados e criar views temporárias
spark.sql("""
    CREATE TEMPORARY VIEW vw_devices
    USING org.apache.spark.sql.json
    OPTIONS (path "/intellij-projects/spark-series/docs/files/devices/*.json")
""")

spark.sql("""
    CREATE TEMPORARY VIEW vw_subscriptions
    USING org.apache.spark.sql.json
    OPTIONS (path "/intellij-projects/spark-series/docs/files/subscriptions/*.json")
""")  # Corrigido o path para apontar para subscriptions

# Listar as tabelas registradas no catálogo
print(spark.catalog.listTables())

# Selecionar os primeiros 10 registros das tabelas
spark.sql("SELECT * FROM vw_devices LIMIT 10").show()
spark.sql("SELECT * FROM vw_subscriptions LIMIT 10").show()

# Realizar o INNER JOIN entre as duas tabelas
join_datasets = spark.sql("""
    SELECT
        d.id AS device_id,
        d.manufacturer,
        d.model,
        d.platform,
        d.user_id,
        s.subscription_id,
        s.plan,
        s.status,
        s.amount
    FROM
        vw_devices AS d
    INNER JOIN
        vw_subscriptions AS s
    ON
        d.user_id = s.user_id
""")

# Exibir o resultado do join
join_datasets.show()

#info
join_datasets.show()
join_datasets.printSchema()
join_datasets.count()
