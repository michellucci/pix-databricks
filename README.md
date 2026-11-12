# Pix no Brasil

Projeto de engenharia de dados que usa dados abertos do Banco Central do Brasil para analisar as transações Pix por estado e mês. O painel apresenta quantidade de transações, valor enviado e ticket médio geral, de pessoas físicas (PF) e de pessoas jurídicas (PJ).

**[Acesse o painel interativo](https://michellucci.github.io/pix-databricks/)**

## Fluxo dos dados

| Etapa | O que acontece |
| --- | --- |
| Coleta | O GitHub Actions consulta a API de dados abertos do Banco Central e envia os arquivos JSON para um volume no Databricks. |
| Bronze | Um notebook PySpark lê os arquivos brutos e insere registros novos ou atualiza os que mudaram. |
| Silver | Um notebook padroniza os dados, incluindo os nomes usados nas análises. |
| Gold | Um notebook agrega os dados por estado e mês e calcula os indicadores do painel. |
| Publicação | O GitHub Pages serve o painel interativo, que lê o CSV exportado da camada Gold. |

## Estrutura do repositório

- [`.github/workflows/`](.github/workflows/) — workflows de coleta e publicação.
- [`databricks/src/`](databricks/src/) — notebooks Bronze, Silver e Gold e módulo Python reutilizável.
- [`databricks/dashboards/`](databricks/dashboards/) — definição exportada do dashboard nativo do Databricks.
- [`docs/`](docs/) — painel público e CSV com os indicadores agregados.

## Tecnologias

Databricks Free Edition, PySpark, Delta Lake, Unity Catalog, GitHub Actions e GitHub Pages.
