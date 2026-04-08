# Scripts

Este diretorio concentra utilitarios de automacao fora do projeto dbt em `main`.

## Gerador de suites GE a partir do manifest dbt

Arquivo:
- `generate_ge_suites_from_dbt_manifest.py`

Objetivo:
- Ler `main/target/manifest.json`
- Extrair testes por metadados estruturados (`test_metadata`, `attached_node`, `column_name`)
- Gerar suites do Great Expectations agrupadas por modelo/tabela (bom para execucao em paralelo)
- Validar o schema JSON com a propria lib do Great Expectations antes de gravar

## Requisitos

- Python 3
- Dependencias do workspace em `requirements.txt`

## Uso rapido

Execute a partir da raiz do workspace (`/workspaces/dbt`):

```bash
python scripts/generate_ge_suites_from_dbt_manifest.py
```

Esse comando usa defaults:
- Manifest: `main/target/manifest.json`
- Saida: `main/ge/suites`
- Inclui apenas testes `dbt_expectations`

## Opcoes

```bash
python scripts/generate_ge_suites_from_dbt_manifest.py \
  --manifest main/target/manifest.json \
  --output-dir main/ge/suites_all \
  --include-core-tests
```

Flags:
- `--manifest`: caminho do manifest do dbt
- `--output-dir`: pasta de saida das suites
- `--include-core-tests`: inclui mapeamento de testes core suportados (`not_null`, `unique`)

## Saida gerada

Na pasta de saida, o script cria:
- Um arquivo `index.json` com resumo, suites geradas e testes pulados
- Um arquivo `.suite.json` por `attached_node`

As suites sao serializadas no formato nativo do GX (`name`, `expectations[].type`, `meta`).

Exemplo:
- `model.main.customers.suite.json`
- `index.json`

## Observacoes

- O script nao depende de parse do nome achatado do teste.
- O campo `model` em `test_metadata.kwargs` e removido ao gerar o payload GE.
- Cada expectativa e cada suite sao validadas com `expectationConfigurationSchema` e `expectationSuiteSchema`.
- Para testes nao suportados, o motivo fica em `skipped_tests` no `index.json`.

## Gerador de artefato minimo para Airflow

Arquivo:
- `generate_airflow_artifact_from_dbt_manifest.py`

Objetivo:
- Ler `main/target/manifest.json` (schema v12)
- Extrair apenas os metadados necessarios para o Airflow montar DAGs e dependencias
- Criar DAG groups para `seeds`, `staging` e `marts` por assunto
- Classificar dependencias em `intra_dag` e `cross_dag`
- Enriquecer modelos com mapeamento opcional de suite GE via `ge/suites_all/index.json`

Uso rapido:

```bash
python scripts/generate_airflow_artifact_from_dbt_manifest.py
```

Opcoes:

```bash
python scripts/generate_airflow_artifact_from_dbt_manifest.py \
  --manifest main/target/manifest.json \
  --ge-index ge/suites_all/index.json \
  --output main/target/airflow_artifact.json \
  --allow-missing-ge
```

Saida:
- `main/target/airflow_artifact.json`

Validacoes relevantes:
- Falha se `metadata.dbt_schema_version` nao for manifest v12.
- Falha se detectar ciclo de dependencias intra-dag.
- Por padrao, falha se algum modelo nao tiver suite GE mapeada.
- Com `--allow-missing-ge`, permite gerar artefato mesmo com modelos sem suite GE.
