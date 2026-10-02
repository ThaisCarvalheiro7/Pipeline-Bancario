# Arquitetura do Projeto — Pipeline de Dados Bancários

## 1. Visão geral

Projeto pessoal de engenharia de dados que simula o pipeline de uma instituição financeira: ingestão de transações bancárias, processamento em camadas (Bronze/Silver/Gold) e disponibilização de dados analíticos para detecção de padrões de gastos e possíveis fraudes.

**Objetivo de aprendizado:** praticar de ponta a ponta o ciclo Azure + Databricks + PySpark + VS Code + Git, com um caso de uso realista do setor bancário.

**Domínio simulado:** transações de clientes (PIX, TED, DOC, cartão de crédito/débito, boleto), contas e cadastro de clientes.

---

## 2. Arquitetura em camadas (Medallion Architecture)

```
Gerador de dados (Python/Faker)
        │
        ▼
Azure Data Lake Storage Gen2  (landing zone / raw)
        │
        ▼
┌─────────────────────── Databricks ───────────────────────┐
│                                                             │
│   BRONZE  →  SILVER  →  GOLD                                │
│   (raw)     (limpo)     (agregado / negócio)                │
│                                                             │
└──────────────────────────┬──────────────────────────────┘
                            │
                            ▼
                  Dashboard / Consumo (Power BI)
```

| Camada | Função | Formato |
|---|---|---|
| **Landing (ADLS Gen2)** | Recebe os arquivos brutos gerados/simulados, sem alteração | CSV / JSON |
| **Bronze** | Ingestão 1:1 do landing para Delta, com metadados de carga (data, arquivo de origem) | Delta Table |
| **Silver** | Limpeza, tipagem, deduplicação, aplicação de regras de negócio e qualidade | Delta Table |
| **Gold** | Agregações e métricas prontas para consumo analítico | Delta Table |

---

## 3. Fluxo de dados detalhado

1. **Geração dos dados** — script Python (`Faker` + `pandas`) roda localmente no VS Code, gerando arquivos de clientes, contas e transações com volume configurável e inconsistências propositais (nulos, duplicatas, formatos variados de data).
2. **Upload para o Data Lake** — os arquivos são enviados via SDK do Azure (`azure-storage-blob` ou `az cli`) para um container `landing/` no ADLS Gen2, particionado por data de ingestão.
3. **Ingestão Bronze** — notebook PySpark no Databricks lê os arquivos do landing, adiciona colunas técnicas (`data_ingestao`, `arquivo_origem`) e grava como tabela Delta, sem transformar os dados de negócio.
4. **Transformação Silver** — segundo notebook lê a Bronze, aplica:
   - padronização de tipos (datas, valores monetários, IDs)
   - remoção de duplicatas
   - tratamento de nulos
   - regras de negócio (ex.: valor de transação > 0, conta existente)
   - marcação de registros inválidos (quarentena) vs. válidos
5. **Agregação Gold** — terceiro notebook lê a Silver e constrói tabelas de negócio: volume de transações por cliente/dia, ticket médio por tipo de transação, ranking de clientes por movimentação, sinalização de transações atípicas (outliers estatísticos).
6. **Orquestração** — Databricks Workflows (ou Azure Data Factory) agenda a execução sequencial Bronze → Silver → Gold, com dependências e alertas de falha.
7. **Consumo** — Power BI (ou notebook com gráficos) conecta na camada Gold via Databricks SQL Warehouse para montar dashboards.

---

## 4. Estrutura de pastas no Data Lake

```
adls-container/
├── landing/
│   └── transacoes/ano=2026/mes=09/dia=04/
├── bronze/
│   ├── clientes/
│   ├── contas/
│   └── transacoes/
├── silver/
│   ├── clientes/
│   ├── contas/
│   └── transacoes/
└── gold/
    ├── metricas_cliente_dia/
    ├── ranking_clientes/
    └── transacoes_atipicas/
```

---

## 5. Estrutura do repositório (Git / VS Code)

```
pipeline-bancario/
├── src/
│   ├── geracao_dados/        # scripts Faker (Python puro)
│   ├── ingestao/              # upload para ADLS
│   ├── bronze/                # notebooks/jobs PySpark
│   ├── silver/
│   ├── gold/
│   └── utils/                 # funções compartilhadas (schemas, validações)
├── tests/                     # testes unitários (pytest) das transformações
├── notebooks/                 # notebooks exploratórios do Databricks
├── config/                    # parâmetros de ambiente (dev/prod)
├── docs/
│   └── arquitetura-pipeline-bancario.md
├── requirements.txt
└── README.md
```

---

## 6. Tecnologias por etapa

| Etapa | Tecnologia |
|---|---|
| Geração e testes locais | Python, Faker, pandas, VS Code |
| Armazenamento | Azure Data Lake Storage Gen2 (containers landing/bronze/silver/gold) |
| Processamento | Databricks (clusters PySpark), Delta Lake |
| Orquestração | Databricks Workflows ou Azure Data Factory |
| Governança/catálogo | Unity Catalog (controle de acesso por camada, bem próximo do que um banco real exige) |
| Versionamento | Git + GitHub, integrado ao Databricks Repos |
| Consumo | Power BI ou Databricks SQL Dashboards |

---

## 7. Pontos de atenção específicos de dados bancários

- **Qualidade e integridade:** toda transação deve referenciar uma conta e cliente existentes (chave estrangeira validada na Silver).
- **Mascaramento de dados sensíveis:** mesmo sendo dados sintéticos, é uma boa prática simular mascaramento de CPF/número de conta como se fizesse em produção.
- **Auditoria:** manter colunas técnicas de rastreabilidade (quando o dado entrou, de onde veio) em todas as camadas.
- **Idempotência:** os jobs de Bronze/Silver/Gold devem poder rodar novamente sem duplicar dados (usar `MERGE INTO` do Delta Lake).

---

## 8. Roadmap sugerido

1. Script de geração de dados sintéticos
2. Upload automatizado para o ADLS Gen2 (landing)
3. Notebook Bronze (ingestão bruta)
4. Notebook Silver (limpeza e regras)
5. Notebook Gold (métricas de negócio)
6. Orquestração com Databricks Workflows
7. Dashboard de consumo
8. (Opcional) Testes automatizados + CI no GitHub Actions
