# Data Optimization Lab — Databricks

Laboratório hands-on para demonstrar o impacto de `OPTIMIZE`, `CLUSTER BY` (Liquid Clustering), `VACUUM` e **Predictive Optimization** em tabelas Delta no Unity Catalog.

## Pré-requisitos

- Workspace Databricks com **Unity Catalog**.
- Compute: **Serverless notebook compute** ou cluster clássico **DBR 15.4 LTS+**.
- Permissão `USE CATALOG` + `CREATE SCHEMA` no catálogo escolhido.

## Notebooks (executar nesta ordem)

| # | Notebook | O que faz |
| --- | --- | --- |
| 1 | `01_ingestao_dados` | Gera `sales_raw`, dataset sintético (~50M linhas) **propositalmente fragmentado** |
| 2 | `02_otimizacao` | Mede OPTIMIZE, CLUSTER BY, VACUUM e habilita Predictive Optimization |
| 3 | `03_testes_sanidade` | Rede de segurança que valida o estado das tabelas end-to-end |

### Três tabelas, três níveis de otimização

O notebook `02` materializa três tabelas derivadas de `sales_raw`, cada uma num nível distinto de otimização. Tabelas separadas garantem que o Disk Cache (endereçado por file ID) nunca contamine a medição entre cenários:

| Tabela | Origem | Otimizações |
| --- | --- | --- |
| `sales_baseline` | `DEEP CLONE sales_raw` | nenhuma (estado "antes", imutável) |
| `sales_optimized` | `DEEP CLONE sales_raw` + `OPTIMIZE` | compactação |
| `sales_clustered` | `CTAS ... CLUSTER BY (customer_id, event_date)` + `OPTIMIZE` | compactação + Liquid Clustering |

## Como rodar

1. Importe os três `.py` como notebooks no workspace (ou clone este repo via Databricks Repos).
2. Execute `01_ingestao_dados` uma vez, ajustando os widgets `catalog` / `schema` / `num_rows`.
3. Execute `02_otimizacao` — pode ser re-executado várias vezes (idempotente).
4. Execute `03_testes_sanidade` para confirmar que todas as tabelas estão no estado esperado e a progressão de performance `baseline → optimized → clustered` é visível.

Os notebooks usam widgets e não hardcodam nomes de catálogo — é só alterar os valores no topo.

## Cleanup

Célula comentada no final de `02_otimizacao` com os `DROP TABLE` necessários. Para limpar tudo:

```sql
DROP TABLE IF EXISTS sales_raw;
DROP TABLE IF EXISTS sales_baseline;
DROP TABLE IF EXISTS sales_optimized;
DROP TABLE IF EXISTS sales_clustered;
DROP SCHEMA IF EXISTS lab CASCADE;
```
