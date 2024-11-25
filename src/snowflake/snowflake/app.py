from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import os

# Configuração da conexão com Snowflake
SNOWFLAKE_CONFIG = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT", "vg65446"),
    "user": os.environ.get("SNOWFLAKE_USER", "LUANMACIEL"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD", "Q1w2e3r4!"),
    "role": os.environ.get("SNOWFLAKE_ROLE", "accountadmin"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "PYTHIANDB"),
    "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
}

# Inicia a sessão do Snowpark
session = Session.builder.configs(SNOWFLAKE_CONFIG).create()

# Lê a tabela "Subscription" usando Snowpark
df_subscription = session.table("Subscription")

# Exibe as primeiras linhas da tabela
df_subscription.show()

# Exemplo de manipulação de dados com Snowpark
# Selecionar apenas algumas colunas e aplicar filtros
df_filtered = df_subscription.select(
    col("column1"),
    col("column2")
).filter(col("status") == "active")

# Exibe os dados filtrados
df_filtered.show()

# Fecha a sessão ao finalizar
session.close()
