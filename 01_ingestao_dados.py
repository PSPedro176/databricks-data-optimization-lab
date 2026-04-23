# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão de dados brutos
# MAGIC
# MAGIC Este notebook **produz** a tabela `sales_raw`, um dataset sintético de ~50M linhas de vendas,
# MAGIC **propositalmente fragmentado** (muitos arquivos pequenos) para servir de ponto de partida
# MAGIC do laboratório de otimização (`02_otimizacao`).
# MAGIC
# MAGIC Rode uma vez antes da sessão. O notebook 2 pode ser re-executado quantas vezes quiser
# MAGIC sem precisar re-rodar este.
# MAGIC
# MAGIC ## Pré-requisitos
# MAGIC - Workspace com **Unity Catalog**
# MAGIC - Cluster **DBR 15.4 LTS+** ou SQL Warehouse serverless
# MAGIC - `USE CATALOG` + `CREATE SCHEMA` no catálogo escolhido

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros e setup

# COMMAND ----------

dbutils.widgets.text("catalog", "data_optimization_lab_catalog", "Catálogo UC")
dbutils.widgets.text("schema", "lab", "Schema")
dbutils.widgets.text("num_rows", "50000000", "Linhas a gerar")
dbutils.widgets.text("num_customers", "500000", "Clientes distintos")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
NUM_ROWS = int(dbutils.widgets.get("num_rows"))
NUM_CUSTOMERS = int(dbutils.widgets.get("num_customers"))

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Contexto: {CATALOG}.{SCHEMA} — {NUM_ROWS:,} linhas / {NUM_CUSTOMERS:,} clientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Managed vs External — rápido enquadramento
# MAGIC
# MAGIC | | Managed UC | External |
# MAGIC | --- | --- | --- |
# MAGIC | Dados | UC gerencia | Você gerencia |
# MAGIC | Predictive Optimization | Automático | Não |
# MAGIC | Undrop | Sim | Não |
# MAGIC | Lineage + ACL | Nativo | Parcial |
# MAGIC
# MAGIC `sales_raw` será criada **managed** — é o padrão recomendado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Geração do dataframe sintético
# MAGIC
# MAGIC Cenário: rede de varejo com ~500k clientes e 50M linhas de vendas em 3 anos.
# MAGIC **Skew proposital** em `customer_id` (via `rand()^2`) para expor o ganho do clustering
# MAGIC quando chegarmos ao notebook 2.

# COMMAND ----------

from pyspark.sql import functions as F

START_TS = 1672531200  # 2023-01-01 00:00:00 UTC
WINDOW_SECS = 3 * 365 * 86400

countries = ["BR", "US", "FR", "DE", "UK", "JP", "IN", "MX", "ES", "IT"]
categories = ["electronics", "apparel", "grocery", "home", "beauty", "toys", "sports", "books"]

df = (
    spark.range(0, NUM_ROWS)
    .withColumn("customer_id", (F.pow(F.rand(seed=42), F.lit(2)) * NUM_CUSTOMERS).cast("int"))
    .withColumn("event_ts", (F.lit(START_TS) + (F.rand(seed=1) * WINDOW_SECS).cast("long")).cast("timestamp"))
    .withColumn("event_date", F.to_date("event_ts"))
    .withColumn(
        "country",
        F.element_at(
            F.array(*[F.lit(c) for c in countries]),
            (F.rand(seed=2) * len(countries) + 1).cast("int"),
        ),
    )
    .withColumn(
        "category",
        F.element_at(
            F.array(*[F.lit(c) for c in categories]),
            (F.rand(seed=3) * len(categories) + 1).cast("int"),
        ),
    )
    .withColumn("quantity", (F.rand(seed=4) * 10 + 1).cast("int"))
    .withColumn("unit_price", F.round(F.rand(seed=5) * 500 + 5, 2))
    .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumnRenamed("id", "order_id")
)

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Escrita fragmentada — `sales_raw`
# MAGIC
# MAGIC Workspaces UC serverless têm `optimizeWrite` + `autoCompact` ligados por default.
# MAGIC Desligamos explicitamente via TBLPROPERTIES **só para este laboratório**, para preservar
# MAGIC a fragmentação típica de um pipeline legado que o `OPTIMIZE` vai consertar. Em produção
# MAGIC normalmente você quer deixar esses recursos ligados.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS sales_raw")
spark.sql("""
CREATE TABLE sales_raw (
  order_id BIGINT,
  customer_id INT,
  event_ts TIMESTAMP,
  event_date DATE,
  country STRING,
  category STRING,
  quantity INT,
  unit_price DOUBLE,
  amount DOUBLE
) USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'false',
  'delta.autoOptimize.autoCompact'   = 'false'
)
""")

# Escreve em 20 micro-batches para simular ingestão incremental → mais commits, mais arquivos
BATCH_COUNT = 20
batch_size = NUM_ROWS // BATCH_COUNT
for i in range(BATCH_COUNT):
    start = i * batch_size
    end = (i + 1) * batch_size if i < BATCH_COUNT - 1 else NUM_ROWS
    (
        df.filter((F.col("order_id") >= start) & (F.col("order_id") < end))
          .repartition(10, "country")
          .write.mode("append")
          .saveAsTable("sales_raw")
    )
print(f"Gravados {BATCH_COUNT} micro-batches em sales_raw.")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sales_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Próximos passos
# MAGIC
# MAGIC 1. Abra **`02_otimizacao`** para aplicar e medir `OPTIMIZE`, `CLUSTER BY`, `VACUUM`
# MAGIC    e **Predictive Optimization** sobre `sales_raw`.
# MAGIC 2. Depois rode **`03_testes_sanidade`** para validar ponta-a-ponta que o estado
# MAGIC    final das tabelas bate com o que o laboratório prometeu.
