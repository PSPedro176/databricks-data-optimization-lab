# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Testes de sanidade do laboratório
# MAGIC
# MAGIC Este notebook **não ensina nada novo**. Ele é uma rede de segurança executada
# MAGIC *depois* de `01_ingestao_dados` e `02_otimizacao` para confirmar que o estado
# MAGIC das tabelas no metastore bate com o que a narrativa do lab prometeu.
# MAGIC
# MAGIC ### Por que um notebook separado
# MAGIC
# MAGIC - **Leitura limpa dos dois primeiros notebooks**: o `01` é didática de ingestão,
# MAGIC   o `02` é didática de otimização. Asserts no meio do texto distraem.
# MAGIC - **Checklist pronto antes de demo**: rode este notebook e, se passar tudo,
# MAGIC   o cenário está saudável para apresentar.
# MAGIC - **Regressão pós-alteração**: se você editou o `02` e quer saber se continua
# MAGIC   entregando o mesmo resultado pedagógico, este é o verificador.
# MAGIC
# MAGIC ### O que é validado
# MAGIC
# MAGIC 1. **Ingestão (`sales_raw`)** — a base está no estado "antes": managed, volumosa,
# MAGIC    fragmentada de propósito, sem clustering.
# MAGIC 2. **Otimização (`sales_baseline`, `sales_optimized`, `sales_clustered`)** — as três
# MAGIC    tabelas existem e estão no estado esperado: `sales_baseline` **ainda** fragmentada
# MAGIC    (garante comparação justa), `sales_optimized` compactada, `sales_clustered` com
# MAGIC    chaves de clustering aplicadas e `OPTIMIZE` no histórico.
# MAGIC 3. **Smoke benchmark** — a query-alvo executa progressivamente mais rápido em
# MAGIC    `sales_baseline` → `sales_optimized` → `sales_clustered`. Como cada cenário vive
# MAGIC    em uma tabela distinta, o Disk Cache (endereçado por file ID) não compartilha
# MAGIC    entradas entre medições — não precisa de `CLEAR CACHE` nem configs restritas.
# MAGIC
# MAGIC Se qualquer teste falhar, o notebook acumula todas as falhas e lança
# MAGIC `AssertionError` no final — facilita agendamento como job de verificação.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros, setup e pré-check

# COMMAND ----------

dbutils.widgets.text("catalog", "data_optimization_lab_catalog", "Catálogo UC")
dbutils.widgets.text("schema", "lab", "Schema")
dbutils.widgets.text("num_rows", "50000000", "Linhas esperadas em sales_raw")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
NUM_ROWS = int(dbutils.widgets.get("num_rows"))

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Contexto: {CATALOG}.{SCHEMA}")

# COMMAND ----------

required = ["sales_raw", "sales_baseline", "sales_optimized", "sales_clustered"]
missing = [t for t in required if not spark.catalog.tableExists(t)]
if missing:
    raise RuntimeError(
        f"Tabelas ausentes: {missing}. "
        "Rode 01_ingestao_dados e 02_otimizacao antes deste notebook."
    )
print(f"Pré-check OK — encontrei {required}.")

# COMMAND ----------

# Acumulador único: roda tudo, falha no final com a lista completa.
failures = []

def _detail(table):
    return spark.sql(f"DESCRIBE DETAIL {table}").collect()[0].asDict()

def _is_managed(table):
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    tbl_type = next((r["data_type"] for r in rows if r["col_name"] == "Type"), "")
    return "MANAGED" in tbl_type.upper(), tbl_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bloco A — Ingestão (`sales_raw`)
# MAGIC
# MAGIC O notebook `01` gerou uma tabela managed, volumosa e **propositalmente fragmentada**.
# MAGIC Esse estado "antes" é a matéria-prima do `02`. Se qualquer uma destas pré-condições
# MAGIC quebrar, o `02` perde sentido:
# MAGIC
# MAGIC | # | Checagem | Por que importa |
# MAGIC | --- | --- | --- |
# MAGIC | A1 | Volume ≥ 95% de `num_rows` | Confirma que o gerador sintético não truncou |
# MAGIC | A2 | Tipo `MANAGED` | Pré-requisito para Predictive Optimization no `02` |
# MAGIC | A3 | `numFiles ≥ 100` | Sem fragmentação o `OPTIMIZE` não teria nada a compactar |
# MAGIC | A4 | `clusteringColumns` vazio | Garante que `01` é o estado "antes", não o "depois" |

# COMMAND ----------

raw_detail = _detail("sales_raw")
raw_rows = spark.table("sales_raw").count()

# A1 — Volume esperado
if raw_rows < NUM_ROWS * 0.95:
    failures.append(f"[A1] sales_raw tem {raw_rows:,} linhas (esperado ~{NUM_ROWS:,})")

# A2 — Tabela é managed
ok, tbl_type = _is_managed("sales_raw")
if not ok:
    failures.append(f"[A2] sales_raw não é managed (Type={tbl_type})")

# A3 — Fragmentação intencional
if raw_detail["numFiles"] < 100:
    failures.append(
        f"[A3] sales_raw só tem {raw_detail['numFiles']} arquivos "
        "— esperado ≥100 para demonstrar OPTIMIZE"
    )

# A4 — Sem clustering (o clustering é introduzido no 02)
if raw_detail.get("clusteringColumns"):
    failures.append(f"[A4] sales_raw já tem clustering: {raw_detail['clusteringColumns']}")

print(
    f"sales_raw: {raw_rows:,} linhas, {raw_detail['numFiles']} arquivos, "
    f"clustering={raw_detail.get('clusteringColumns') or 'nenhum'}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bloco B — Otimização (`sales_baseline`, `sales_optimized`, `sales_clustered`)
# MAGIC
# MAGIC Depois do `02`, três tabelas derivadas devem existir, uma por nível de otimização:
# MAGIC `sales_baseline` (clone cru de `sales_raw`, **imutável**), `sales_optimized` (clone
# MAGIC + `OPTIMIZE`) e `sales_clustered` (CTAS `CLUSTER BY` + `OPTIMIZE`). Ter tabelas
# MAGIC distintas é o que mantém o benchmark honesto.
# MAGIC
# MAGIC | # | Checagem | Por que importa |
# MAGIC | --- | --- | --- |
# MAGIC | B1 | `sales_baseline.numFiles ≥ 100` | Baseline **precisa** seguir fragmentada — é o "antes" da comparação |
# MAGIC | B2 | `sales_optimized.numFiles ≤ 50` | `OPTIMIZE` partiu de ≥100 arquivos — tem que ter compactado |
# MAGIC | B3 | `customer_id` em `clusteringColumns` de `sales_clustered` | Chave de clustering foi definida |
# MAGIC | B4 | `OPTIMIZE` aparece em `DESCRIBE HISTORY sales_clustered` | Clustering foi materializado, não só declarado |
# MAGIC | B5 | As três tabelas são `MANAGED` | Necessário para o bloco de Predictive Optimization do `02` |

# COMMAND ----------

baseline = _detail("sales_baseline")
optimized = _detail("sales_optimized")
clustered = _detail("sales_clustered")

# B1 — Baseline permanece fragmentada (garante que a comparação é justa)
if baseline["numFiles"] < 100:
    failures.append(
        f"[B1] sales_baseline com apenas {baseline['numFiles']} arquivos — "
        "deveria seguir fragmentada como estado 'antes'"
    )

# B2 — Optimized pós-OPTIMIZE com poucos arquivos
if optimized["numFiles"] > 50:
    failures.append(
        f"[B2] sales_optimized ainda com {optimized['numFiles']} arquivos — OPTIMIZE não rodou"
    )

# B3 — Clustered com chaves configuradas
clustering_cols = clustered.get("clusteringColumns") or []
if "customer_id" not in clustering_cols:
    failures.append(
        f"[B3] sales_clustered sem customer_id em clusteringColumns: {clustering_cols}"
    )

# B4 — DESCRIBE HISTORY de sales_clustered inclui OPTIMIZE
hist_ops = [
    r["operation"]
    for r in spark.sql("DESCRIBE HISTORY sales_clustered").select("operation").collect()
]
if "OPTIMIZE" not in hist_ops:
    failures.append(f"[B4] sales_clustered sem OPTIMIZE no histórico: {hist_ops}")

# B5 — Todas são managed
for t in ("sales_baseline", "sales_optimized", "sales_clustered"):
    ok, tbl_type = _is_managed(t)
    if not ok:
        failures.append(f"[B5] {t} não é managed (Type={tbl_type})")

print(
    f"sales_baseline: {baseline['numFiles']} arquivos | "
    f"sales_optimized: {optimized['numFiles']} arquivos | "
    f"sales_clustered: clustering={clustering_cols}, operações={sorted(set(hist_ops))}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bloco C — Smoke benchmark (progressão baseline → optimized → clustered)
# MAGIC
# MAGIC As checagens anteriores são de **estado** — basta olhar o catálogo. Este bloco é
# MAGIC de **comportamento**: reexecuto a mesma query-alvo do `02` contra cada uma das três
# MAGIC tabelas e valido a progressão de performance.
# MAGIC
# MAGIC Cada cenário roda em sua própria tabela → o Disk Cache é endereçado por file ID,
# MAGIC então não há contaminação entre medições. O notebook é autossuficiente e roda em
# MAGIC qualquer cluster/warehouse sem depender do estado do `02`.
# MAGIC
# MAGIC | # | Checagem | Por que importa |
# MAGIC | --- | --- | --- |
# MAGIC | C1 | `optimized` mais rápido que `baseline` | Compactação sozinha já reduz open-file overhead |
# MAGIC | C2 | `clustered` ≥ 10% mais rápido que `baseline` | Clustering + OPTIMIZE entrega o maior ganho (skipping) |

# COMMAND ----------

import time

target_sql = """
SELECT customer_id,
       SUM(amount) AS total,
       COUNT(*)    AS orders
FROM {table}
WHERE customer_id IN (1234, 5678, 9876, 54321, 100000)
  AND event_date BETWEEN '2023-06-01' AND '2023-09-30'
GROUP BY customer_id
"""

def _smoke_bench(label, sql):
    t0 = time.perf_counter()
    rows = spark.sql(sql).collect()
    elapsed = time.perf_counter() - t0
    print(f"[{label}] {elapsed:6.2f}s → {len(rows)} linhas")
    return elapsed

t_baseline = _smoke_bench("baseline", target_sql.format(table="sales_baseline"))
t_optimized = _smoke_bench("optimized", target_sql.format(table="sales_optimized"))
t_clustered = _smoke_bench("clustered", target_sql.format(table="sales_clustered"))

# C1 — Optimized precisa ser mais rápido que baseline (compactação reduz overhead)
if t_optimized >= t_baseline:
    failures.append(
        f"[C1] optimized ({t_optimized:.2f}s) não ficou mais rápido "
        f"que baseline ({t_baseline:.2f}s)"
    )

# C2 — Clustered precisa ser ≥10% mais rápido que baseline
if t_clustered > t_baseline * 0.9:
    failures.append(
        f"[C2] clustered ({t_clustered:.2f}s) não ficou significativamente mais rápido "
        f"que baseline ({t_baseline:.2f}s)"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resultado consolidado

# COMMAND ----------

if failures:
    print("FALHAS:")
    for f in failures:
        print(f"  x {f}")
    raise AssertionError(f"{len(failures)} teste(s) de sanidade falhou/falharam")
else:
    print("OK — todos os testes de sanidade passaram.")