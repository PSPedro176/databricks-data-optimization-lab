# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Otimização de layout de dados
# MAGIC
# MAGIC Mede o impacto de **OPTIMIZE**, **CLUSTER BY** (Liquid Clustering), **VACUUM** e
# MAGIC **Predictive Optimization** sobre a tabela `sales_raw` produzida pelo notebook
# MAGIC `01_ingestao_dados`.
# MAGIC
# MAGIC **Idempotente** — cada execução recria `sales_baseline`, `sales_optimized` e
# MAGIC `sales_clustered` a partir de `sales_raw`.
# MAGIC
# MAGIC ### Três tabelas, três níveis de otimização
# MAGIC
# MAGIC Cada cenário vive em uma tabela própria para que as medições de performance sejam
# MAGIC comparáveis sem depender de limpeza de cache — cada tabela tem seus próprios file IDs,
# MAGIC então o Disk Cache não contamina a medição entre elas.
# MAGIC
# MAGIC | Tabela | Origem | Otimizações aplicadas |
# MAGIC | --- | --- | --- |
# MAGIC | `sales_baseline` | `DEEP CLONE sales_raw` | nenhuma (estado "antes", intocado) |
# MAGIC | `sales_optimized` | `DEEP CLONE sales_raw` + `OPTIMIZE` | compactação |
# MAGIC | `sales_clustered` | `CTAS ... CLUSTER BY` + `OPTIMIZE` | compactação + Liquid Clustering |
# MAGIC
# MAGIC ## Agenda
# MAGIC 1. Setup + pré-check
# MAGIC 2. Conceitos-chave
# MAGIC 3. Baseline — bench sem otimização (`sales_baseline`)
# MAGIC 4. `OPTIMIZE` — compactação (`sales_optimized`)
# MAGIC 5. `CLUSTER BY` — Liquid Clustering (`sales_clustered`)
# MAGIC 6. `CLUSTER BY AUTO`
# MAGIC 7. `VACUUM` + time travel
# MAGIC 8. Consolidação dos benchmarks
# MAGIC 9. Managed tables + Predictive Optimization
# MAGIC 10. Takeaways
# MAGIC 11. Cleanup (opcional)
# MAGIC
# MAGIC > Os testes de sanidade saíram deste notebook. Rode **`03_testes_sanidade`**
# MAGIC > depois desta execução para validar o estado final ponta-a-ponta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros, setup e pré-check

# COMMAND ----------

dbutils.widgets.text("catalog", "data_optimization_lab_catalog", "Catálogo UC")
dbutils.widgets.text("schema", "lab", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Contexto: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Pré-check: sales_raw existe?
if not spark.catalog.tableExists("sales_raw"):
    raise RuntimeError(
        f"Tabela sales_raw não encontrada em {CATALOG}.{SCHEMA}. "
        "Rode o notebook 01_ingestao_dados primeiro."
    )
raw_count = spark.table("sales_raw").count()
print(f"sales_raw OK — {raw_count:,} linhas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conceitos-chave
# MAGIC
# MAGIC | Tópico | O que é | Por que importa |
# MAGIC | --- | --- | --- |
# MAGIC | **Small files** | Muitos arquivos pequenos → alto I/O e listagem custosa | `OPTIMIZE` compacta em arquivos-alvo de 1 GB |
# MAGIC | **Data skipping** | Delta mantém min/max por arquivo, pula os fora do predicado | Efetivo se dados correlatos ficam juntos (→ `CLUSTER BY`) |
# MAGIC | **OPTIMIZE** | Re-escreve arquivos pequenos em poucos grandes | Reduz latência |
# MAGIC | **CLUSTER BY (Liquid)** | Layout adaptativo; substitui `ZORDER` + `PARTITION BY` | Melhor skipping, adaptativo |
# MAGIC | **VACUUM** | Remove arquivos fora da retenção (default 7d) | Controla custo de storage |
# MAGIC | **Predictive Optimization** | Databricks dispara `OPTIMIZE`/`VACUUM`/clustering automático | Zero ops em tabelas managed |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Baseline — `DEEP CLONE` de `sales_raw` + benchmark
# MAGIC
# MAGIC `DEEP CLONE` copia os arquivos físicos — preserva a fragmentação original de `sales_raw`,
# MAGIC então a medição baseline é fiel. Idempotente: `CREATE OR REPLACE` apaga e recria.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_baseline DEEP CLONE sales_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sales_baseline;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nota sobre cache e fidelidade da medição
# MAGIC
# MAGIC Cada cenário roda em **uma tabela distinta** (`sales_baseline`, `sales_optimized`,
# MAGIC `sales_clustered`). O Disk Cache do Databricks é endereçado por file ID, então
# MAGIC tabelas diferentes nunca compartilham entradas — a medição de uma não pode se
# MAGIC beneficiar do cache aquecido pela outra. Funciona igual em Serverless e em cluster
# MAGIC clássico, sem precisar mexer em config.

# COMMAND ----------

import time

def benchmark(label, sql):
    """Roda a query uma vez e retorna o tempo decorrido."""
    t0 = time.perf_counter()
    rows = spark.sql(sql).collect()
    elapsed = time.perf_counter() - t0
    print(f"[{label}] {elapsed:6.2f}s → {len(rows)} linhas")
    return elapsed

target_sql = """
SELECT customer_id,
       SUM(amount) AS total,
       COUNT(*)    AS orders
FROM {table}
WHERE customer_id IN (1234, 5678, 9876, 54321, 100000)
  AND event_date BETWEEN '2023-06-01' AND '2023-09-30'
GROUP BY customer_id
"""

bench = {}
bench["1_baseline"] = benchmark("baseline", target_sql.format(table="sales_baseline"))

# COMMAND ----------

# MAGIC %md
# MAGIC No **Query Profile** da UI, repare em **Files read / Files pruned**. No baseline quase
# MAGIC nenhum arquivo é podado — `customer_id` está espalhado por todos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `OPTIMIZE` — compactação (`sales_optimized`)
# MAGIC
# MAGIC Em vez de mutar `sales_baseline`, criamos uma **nova** tabela `sales_optimized` como
# MAGIC `DEEP CLONE` de `sales_raw` e aplicamos `OPTIMIZE` nela. Assim `sales_baseline` fica
# MAGIC imutável como monumento do estado "antes", e a comparação `DESCRIBE DETAIL` lado a
# MAGIC lado mostra o efeito da compactação sem ambiguidade.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_optimized DEEP CLONE sales_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE sales_optimized;

# COMMAND ----------

# Comparação direta: mesma origem, só difere pelo OPTIMIZE.
from pyspark.sql import functions as F

cmp = (
    spark.sql("DESCRIBE DETAIL sales_baseline").withColumn("cenario", F.lit("baseline"))
    .unionByName(
        spark.sql("DESCRIBE DETAIL sales_optimized").withColumn("cenario", F.lit("optimized"))
    )
    .select("cenario", "numFiles", "sizeInBytes")
)
display(cmp)

# COMMAND ----------

bench["2_after_optimize"] = benchmark("after_optimize", target_sql.format(table="sales_optimized"))

# COMMAND ----------

# MAGIC %md
# MAGIC O número de arquivos cai drasticamente. Mesmo sem clustering, a query fica mais rápida
# MAGIC por reduzir open-file overhead. Mas o skipping **ainda é ruim** — o próximo passo resolve isso.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `CLUSTER BY` — Liquid Clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_clustered
# MAGIC CLUSTER BY (customer_id, event_date)
# MAGIC AS SELECT * FROM sales_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- clustering é aplicado incrementalmente pelos OPTIMIZEs
# MAGIC OPTIMIZE sales_clustered;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sales_clustered;

# COMMAND ----------

bench["3_clustered"] = benchmark("clustered", target_sql.format(table="sales_clustered"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Evolução no histórico
# MAGIC
# MAGIC `DESCRIBE HISTORY` mostra cada `OPTIMIZE` e suas métricas de clustering.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_clustered;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `CLUSTER BY AUTO` (DBR 15.4+)
# MAGIC
# MAGIC Delega a escolha das chaves ao Databricks com base no Query History. Ideal quando o
# MAGIC padrão de acesso evolui ou quando múltiplas queries diferentes batem na tabela.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Descomente para trocar para AUTO:
# MAGIC -- ALTER TABLE sales_clustered CLUSTER BY AUTO;
# MAGIC SELECT 'CLUSTER BY AUTO delega a escolha ao Databricks (requer DBR 15.4+).' AS nota;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `VACUUM` + time travel
# MAGIC
# MAGIC - Default retention: **7 dias** (preserva time travel).
# MAGIC - Sempre rode `DRY RUN` antes em produção.
# MAGIC - **Nunca** use `RETAIN 0 HOURS` em tabela lida concorrentemente.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM sales_clustered DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM sales_clustered;  -- aplica retenção default

# COMMAND ----------

# MAGIC %md
# MAGIC Cada `OPTIMIZE`, `UPDATE`, `DELETE`, `MERGE` cria novos arquivos sem remover os antigos.
# MAGIC Você paga storage por **todas** as versões dentro do retention. `VACUUM` é quem libera.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_clustered;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Consolidação dos benchmarks

# COMMAND ----------

import pandas as pd

result = (
    pd.DataFrame(
        [(k, v) for k, v in bench.items()],
        columns=["cenário", "tempo_seg"],
    )
    .assign(speedup_vs_baseline=lambda d: d["tempo_seg"].iloc[0] / d["tempo_seg"])
    .round(2)
)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Managed tables + Predictive Optimization
# MAGIC
# MAGIC Até aqui otimizamos tudo à mão — `OPTIMIZE`, `CLUSTER BY`, `VACUUM`. O último passo
# MAGIC é entregar a manutenção de volta ao Databricks: com **Predictive Optimization** em
# MAGIC uma tabela *managed*, o sistema dispara essas mesmas operações automaticamente,
# MAGIC no momento em que valem a pena.
# MAGIC
# MAGIC | Feature | Managed UC | External |
# MAGIC | --- | --- | --- |
# MAGIC | Predictive Optimization | Automático | Não |
# MAGIC | Storage lifecycle | Gerenciado | Manual |
# MAGIC | Lineage + column ACL | Nativo | Parcial |
# MAGIC | UniForm Iceberg | Sim | Sim |
# MAGIC | Undrop | Sim | Não |
# MAGIC | Liquid Clustering | Sim | Sim |

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sales_clustered ENABLE PREDICTIVE OPTIMIZATION;
# MAGIC -- Alternativa: ALTER SCHEMA lab ENABLE PREDICTIVE OPTIMIZATION;

# COMMAND ----------

# MAGIC %md
# MAGIC Com PO ativo, o Databricks dispara `OPTIMIZE`, escolha de clustering keys (em `AUTO`) e
# MAGIC `VACUUM` automaticamente — **sem job custom** de manutenção.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Takeaways
# MAGIC
# MAGIC 1. **Mede antes, otimiza depois.** `DESCRIBE DETAIL` + Query Profile já revelam o gargalo.
# MAGIC 2. **Liquid Clustering > ZORDER + PARTITION BY** na maioria dos casos — adaptativo.
# MAGIC 3. **Managed + Predictive Optimization = zero ops.**
# MAGIC 4. **Chaves de clustering**: predicados mais seletivos (aqui, `customer_id` + `event_date`). Dúvida → `AUTO`.
# MAGIC 5. **VACUUM default = 7d** é seguro. Encurtar exige justificativa.
# MAGIC 6. **Compacto ≠ clusterizado.** `OPTIMIZE` resolve small files; `CLUSTER BY` resolve skipping.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Cleanup (opcional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS sales_baseline;
# MAGIC -- DROP TABLE IF EXISTS sales_optimized;
# MAGIC -- DROP TABLE IF EXISTS sales_clustered;
# MAGIC -- Para limpar tudo inclusive a base:
# MAGIC -- DROP TABLE IF EXISTS sales_raw;
# MAGIC -- DROP SCHEMA IF EXISTS lab CASCADE;
