# State of Data Brasil — Pipeline AWS

Projeto desenvolvido para o **Tech Challenge Fase 3 (FIAP)**, com foco na construção de uma solução de **Engenharia de Dados e Analytics** em ambiente **AWS**, utilizando as pesquisas *State of Data Brasil* de **2023, 2024 e 2025**.

O objetivo é **organizar, tratar e analisar** os dados para apoiar respostas sobre o mercado brasileiro de **Dados, Analytics e Inteligência Artificial** — no contexto de uma instituição financeira que deseja expandir sua atuação nessas áreas.

---

## Objetivos da análise

As análises foram conduzidas a partir das perguntas de negócio do desafio:

1. **Como está estruturado o mercado brasileiro de Dados?**
2. **Quais perfis profissionais são mais valorizados pelo mercado?**
3. **Qual é o cenário de diversidade de gênero nas carreiras de dados?**
4. **Quais tecnologias apresentam maior adoção entre os profissionais?**
5. **Qual é o índice de adoção de Inteligência Artificial e seu impacto?**
6. **Existem diferenças relevantes entre regiões, senioridades ou modelos de trabalho?**
7. **Quais oportunidades e desafios podem ser identificados para empresas que desejam investir em Dados e Inteligência Artificial?**

Cada pergunta é respondida por um *mart* da camada Gold (ver tabela em [Camadas de dados](#camadas-de-dados)).

---

## Arquitetura

A solução segue uma arquitetura de **data lake em camadas (medallion)** na AWS Academy Lab (`us-east-1`), com orquestração via Glue Jobs e consumo analítico no Athena.

**Fluxo resumido:**

```text
Fontes (Kaggle / Data Hackers)
    → Amazon S3 (raw/)
    → AWS Glue Job sod-01-bronze → S3 bronze/
    → AWS Glue Job sod-02-silver → S3 silver/
    → AWS Glue Job sod-03-gold   → S3 gold/
    → Glue Crawler (crawler-gold-state-of-data)
    → Glue Data Catalog (database: state_of_data)
    → Amazon Athena (SQL)
    → Visualização (CSV exportado → Looker Studio / relatório executivo)
```

| Etapa | Componente | Função |
|-------|------------|--------|
| Armazenamento | Amazon S3 (`raw-bkt-806188865054`) | Data lake: `raw/`, `bronze/`, `silver/`, `gold/`, `dictionary/`, `scripts/` |
| ETL Bronze | Glue Job `sod-01-bronze` | CSV → Parquet, sanear colunas, lineage, partição por ano |
| ETL Silver | Glue Job `sod-02-silver` | Mapear campos, padronizar, unificar 2023–2025 |
| ETL Gold | Glue Job `sod-03-gold` | Agregações, explode de multi-select, 8 marts |
| Catálogo | Glue Crawler + Data Catalog | `table level 3` · DB `state_of_data` · 8 tabelas `gold_*` |
| Consulta | Amazon Athena | SQL ad hoc sobre a Gold |
| Visualização | Looker Studio (Google Data Studio) + PPT/PDF | Dashboards e storytelling executivo |
| Orquestração | AWS CLI + scripts bash | Upload, deploy e execução dos jobs |

Diagrama detalhado: https://github.com/cjuniorkt-debug/Tech-Challenge---Fase-03--Grupo09/blob/main/arquitetura/Arquitetura_AWS%20V3.drawio

---

## Tecnologias utilizadas

| Tecnologia | Uso no projeto |
|------------|----------------|
| **Amazon S3** | Data lake e armazenamento das camadas |
| **AWS Glue Jobs (PySpark)** | ETL das camadas Bronze, Silver e Gold |
| **AWS Glue Data Catalog** | Metadados das tabelas analíticas |
| **AWS Glue Crawler** | Descoberta/catalogação das pastas Gold |
| **Amazon Athena** | Consultas SQL sobre a Gold |
| **IAM (`LabRole`)** | Permissões de execução no AWS Academy Lab |
| **AWS CLI** | Deploy e operação dos jobs |
| **Apache Spark / PySpark** | Motor de processamento |
| **Parquet** | Formato das camadas Bronze, Silver e Gold |
| **Python** | Jobs, utilitários e geração de gráficos/relatório |
| **Looker Studio** | Visualização a partir de CSV exportado do Athena |
| **Draw.io** | Diagrama de arquitetura |

---

## Camadas de dados

| Camada | Path S3 | Conteúdo |
|--------|---------|----------|
| **Raw** | `s3://raw-bkt-806188865054/raw/` | CSVs originais das edições 2023, 2024 e 2025 |
| **Bronze** | `s3://.../bronze/` | Parquet particionado por ano; colunas saneadas + lineage (`_ano_pesquisa`, `_arquivo_origem`, `_data_ingestao`) |
| **Silver** | `s3://.../silver/pesquisa_unificada/` | Schema único entre os 3 anos; valores padronizados via `mapa_campos.csv` |
| **Gold** | `s3://.../gold/gold_*/` | 1 tabela base + 7 marts analíticos prontos para consumo |
| **Suporte** | `dictionary/`, `scripts/` | Dicionário de unificação e código dos jobs |

### Marts Gold × perguntas de negócio

| Tabela Gold | Pergunta |
|-------------|----------|
| `gold_respondentes` | Base em nível de respondente (consultas ad hoc) |
| `gold_panorama_mercado` | 1 — Estrutura do mercado de Dados |
| `gold_perfis_profissionais` | 2 — Perfis mais valorizados |
| `gold_diversidade` | 3 — Diversidade de gênero |
| `gold_adocao_tecnologias` | 4 — Adoção de tecnologias |
| `gold_adocao_ia` | 5 — Adoção e impacto de IA |
| `gold_perfil_regional` | 6 — Região, senioridade e modelo de trabalho |
| `gold_maturidade_e_desafios` | 7 — Oportunidades e desafios |

---

## Estrutura do repositório

tech-challenge-fase-3/
│
├── Apresentação/
└── tech_challenge_fase_3.pdf
├── Arquitetura
│   ├── 01_raw_to_silver.py
│   ├── 02_silver_to_gold.py
│
│
|── data/
│   ├── Bronze
│   ├── Silver
|   ├── Gold
|
├── arquitetura/
│   ├── arquitetura_aws.drawio
│   └── arquitetura_aws.png
│
│
├── README.md

## Fonte dos dados

| Item | Detalhe |
|------|---------|
| Pesquisa | *State of Data Brasil* (Data Hackers + Bain & Company) |
| Edições | 2023, 2024 e 2025 |
| Disponibilização | [Kaggle — Data Hackers](https://www.kaggle.com/datahackers/datasets) |
| Formato de entrada | CSV (uma edição por arquivo) |
| Escopo | Profissionais de Dados, Analytics e IA no mercado brasileiro |

---

## Autores

**FIAP — Tech Challenge Fase 3 · Grupo 142**

| Nome | RM |

Celso Koiti Tanaka Junior - RM373390

Fabrício Henrique Cardoso Alves - RM373892

Jéssica da Silva Heringer Fontenele - RM366487

Matheus Palmeira da Costa - RM371493

Stefanie Mattoso Pereira Bueno - RM371695

