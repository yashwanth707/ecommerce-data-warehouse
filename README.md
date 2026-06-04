<p align="center">
  <h1 align="center">☁️ Modern Cloud Data Warehouse for E-Commerce Analytics</h1>
  <p align="center">
    <strong>ELT analytics platform built on the Medallion Architecture</strong>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white" alt="Snowflake"/>
    <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt"/>
    <img src="https://img.shields.io/badge/SQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="SQL"/>
    <img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white" alt="Airflow"/>
    <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
    <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white" alt="Streamlit"/>
    <img src="https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black" alt="Power BI"/>
    <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"/>
    <img src="https://img.shields.io/badge/Ollama-black?style=for-the-badge&logo=ollama&logoColor=white" alt="Ollama"/>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/build-passing-brightgreen?style=flat-square" alt="Build Status"/>
    <img src="https://img.shields.io/badge/dbt_tests-61%2F64_passing-brightgreen?style=flat-square" alt="dbt Tests"/>
    <img src="https://img.shields.io/badge/Snowpark_ML-4_Models-blueviolet?style=flat-square" alt="ML Models"/>
    <img src="https://img.shields.io/badge/license-academic-lightgrey?style=flat-square" alt="License"/>
  </p>
</p>


---

## 📖 Overview

An end-to-end cloud data warehouse built for the **Brazilian E-Commerce (Olist)** dataset, demonstrating data engineering practices including:

- **Medallion Architecture** (Bronze → Silver → Gold) with ELT paradigm
- **Incremental processing** via dbt merge strategies with partition pruning
- **Self-healing pipelines** in Apache Airflow with SLA monitoring
- **4-Model ML Pipeline** using Snowpark (Behavioral, Delivery Delay, Seller Risk, Satisfaction)
- **Natural Language to SQL** querying via Ollama (Mistral-7B)
- **Interactive Dashboards** built with both Streamlit and Power BI for deep business intelligence insights

## 🏗️ Architecture

```
┌─────────────┐     ┌─────────────────────────────────────────────────────┐
│   AWS S3    │     │                  SNOWFLAKE                         │
│  (Raw Data) │────▶│  Bronze (VARIANT) → Silver (Cleansed) → Gold (★)  │
└─────────────┘     │       ▲                                     │      │
                    │       │         Snowpark ML                 │      │
                    │       │     (4-Model ML Pipeline)           ▼      │
                    └───────┼─────────────────────────────────────┬──────┘
                            │                                     │
                    ┌───────┴───────┐                    ┌────────▼───────┐
                    │   Airflow     │                    │ Streamlit &    │
                    │ (Orchestrate) │                    │  Power BI      │
                    └───────────────┘                    └────────────────┘
```

> For detailed Mermaid diagrams, see [`docs/architecture_diagrams/`](docs/architecture_diagrams/).

## 📂 Project Structure

```
ecommerce-data-warehouse/
├── src/
│   ├── ingestion/               # S3 → Snowflake Bronze layer
│   ├── dbt/                     # Full dbt project (Silver + Gold)
│   ├── airflow/                 # Production DAGs with SLA & AI monitoring
│   ├── snowpark/                # 4-Model ML Pipeline
│   ├── innovations/             # NL-to-SQL engine & AI Narratives
│   ├── optimization/            # Cost monitoring & budget tracking
│   └── dashboard/               # Streamlit BI application
├── powerbi/                     # DAX measures and .pbix dashboard file
├── docs/
│   └── architecture_diagrams/   # Mermaid architecture docs
├── tests/                       # Unit, integration, performance tests
├── docker-compose.yml           # Local development environment (Airflow + API)
└── requirements.txt             # Python dependencies
```

## 🚀 Quick Start

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.10+ | Runtime |
| Docker & Docker Compose | Latest | Local Airflow & API |
| Snowflake Account | Enterprise | Data warehouse |
| AWS Account | Free tier OK | S3 storage |
| Ollama | Latest | NL-to-SQL (local LLM) |

### 1. Clone & Install

```bash
git clone <repository-url>
cd ecommerce-data-warehouse
python -m venv .venv && source .venv/bin/activate  # Linux/Mac
# OR: python -m venv .venv && .venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your Snowflake, AWS, and Slack credentials
```

### 3. Start Local Airflow

```bash
docker-compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)
```

### 4. Run dbt Models

```bash
cd src/dbt
dbt deps
dbt seed
dbt run --full-refresh   # First run
dbt test                 # Validate
```

### 5. Launch Dashboard

**Streamlit Dashboard:**
```bash
streamlit run src/dashboard/app.py
```

**Power BI Dashboard:**
Open `powerbi/DataWarehouse_Project.pbix` in Power BI Desktop to view the advanced DAX measures and interactive reports.

## 📊 Dataset

**Brazilian E-Commerce Public Dataset by Olist** — 100K orders (2016–2018) across 9 tables.

👉 **[Download Dataset from Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)**

| Table | Records | Description |
|-------|---------|-------------|
| `orders` | 99,441 | Order header with timestamps & status |
| `order_items` | 112,650 | Line items with price & freight |
| `customers` | 99,441 | Customer location data |
| `products` | 32,951 | Product attributes & dimensions |
| `payments` | 103,886 | Payment methods & installments |
| `reviews` | 100,000 | Customer reviews & ratings |
| `sellers` | 3,095 | Seller location data |
| `geolocation` | 1,000,163 | Zip code coordinates |

## 🔑 Key Features

### Self-Healing Pipelines & AI Monitoring
Airflow DAGs automatically detect and repair data quality failures in the Silver layer. Pipeline failures trigger AI-generated plain-English incident summaries via Slack.

### Snowpark ML (4-Model Pipeline)
A robust parallel ML pipeline running natively in Snowflake:
1. **Behavioral Segmentation**: K-Means clustering for customer grouping.
2. **Delivery Delay Prediction**: Random Forest model to flag late shipments.
3. **Seller Risk Clustering**: K-Means evaluation of seller reliability.
4. **Model Registry**: Automated threshold evaluation to retain or drop models.

### AI Dashboards & Natural Language Querying
The project features dual dashboarding solutions:
- **Streamlit**: Includes AI Narrative buttons for instant insights and plain-English business questioning powered by a local Ollama LLM.
- **Power BI**: Provides comprehensive, interactive BI reports built on top of the Gold layer, utilizing advanced DAX measures for operational metrics, Time Intelligence, and CLV Analytics.

### FinOps & Cost Optimization
Integrated cost monitoring via `cost_tracking.db`, surfaced directly in the dashboard. Features one-click Snowflake auto-suspend enforcement (60s idle) and automated resource monitors.

## 📜 License

This project is developed for academic purposes as a Final Year Project.

## 🙏 Acknowledgments

- **Olist** for the public dataset
- **dbt Labs** for the transformation framework
- **Snowflake** for the cloud data platform
