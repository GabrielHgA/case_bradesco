# Modernização de Catálogo de Dados

Case técnico de Engenharia de Dados sobre a modernização de um catálogo corporativo, substituindo uma solução legada pelo Microsoft Purview.

## Objetivo

Construir um processo escalável para extração, processamento e governança de metadados, garantindo melhor desempenho, qualidade e conformidade com a LGPD.

## Arquitetura

* Microsoft Purview para catálogo e governança de dados.
* Databricks e Delta Lake para processamento e armazenamento.
* APIs Atlas/Purview para extração de metadados.
* Airflow para orquestração dos pipelines.
* Python e `ThreadPoolExecutor` para processamento paralelo.
* Arquitetura Medallion: Bronze, Silver e Gold.

## Principais resultados

* Redução da extração de metadados de aproximadamente **18 para 2 horas**.
* Redução do processo de atualização de **3 dias para cerca de 1 hora**.
* Processamento incremental de ativos modificados.
* Automatização de tags e classificações relacionadas à LGPD.
* Validação de completude, linhagem e qualidade dos metadados.
* Monitoramento de SLAs e alertas de inconsistências.

## Estrutura do repositório

```text
├── bronze.py       # Extração e processamento dos metadados do Purview
└── dag_exemplo.py  # Exemplo de orquestração do pipeline
```

## Tecnologias

`Python` · `PySpark` · `Databricks` · `Microsoft Purview` · `Apache Airflow` · `Delta Lake`

> Os códigos deste repositório foram adaptados para fins de apresentação técnica e não contêm credenciais ou informações sensíveis do ambiente original.
