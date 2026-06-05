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
- **5-Model ML Pipeline** using Snowpark (RFM/CLV, Behavioral Segmentation, Delivery Delay, Seller Risk, Review Score — 4 retained, 1 dropped with documented rationale)
- **Natural Language to SQL** querying via local Ollama LLM (qwen2.5-coder:3b)
- **Dual BI Dashboards** — Streamlit (12 sections, AI-powered insights) and Power BI (8 pages, 97 custom DAX measures connected live to Snowflake)

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
Open `powerbi/DataWarehouse_Project.pbix` in Power BI Desktop. The report connects live to Snowflake via Power Query and contains **8 pages** with **97 custom DAX measures**:

| Page | Content |
|------|---------|
| Executive Overview | Revenue KPIs, monthly trends, top-10 states |
| Customer Intelligence | CLV segments, cohort analysis, RFM scatter |
| ML Model Performance | Normalized strip plots, model summary |
| Operational Performance | Filled map, order funnel, payment analysis |
| behavioral_segmentation | Segment treemap, freight/installments scatter, revenue bars |
| Delivery Risk Prediction | Risk-tier donut, probability histogram, order drillthrough |
| Seller Risk Management | Risk scatter, delay bars, seller drillthrough table |
| ML Model Registry | Live KPI cards (4 run · 3 retained · 1 dropped), registry table |

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

### Snowpark ML (5-Model Pipeline, 4 Retained)
A robust parallel ML pipeline running natively in Snowflake, governed by a shared `ML_MODEL_REGISTRY` table with threshold-based retention decisions:
1. **RFM / CLV Analysis**: K-Means clustering with Elbow + Silhouette sweep, dynamic segment labeling (Champions, Loyal, Potential, At Risk, Hibernating).
2. **Behavioral Segmentation**: K-Means (K=3, Silhouette=0.33) on 6 behavioral features — overcomes the single-purchase limitation of traditional RFM.
3. **Delivery Delay Prediction**: Random Forest (F1=0.87, ROC-AUC=0.999, Accuracy=98%) with `class_weight=balanced` for the 6.8% late-delivery class imbalance.
4. **Seller Risk Clustering**: K-Means (K=2, Silhouette=0.84) with composite risk score formula.
5. **Review Score Prediction**: ❌ Dropped — F1 Macro=0.45 (threshold ≥0.60). NEUTRAL class precision was 11%; demonstrates rigorous model evaluation.

### AI Dashboards & Natural Language Querying
The project features dual dashboarding solutions:
- **Streamlit** (1,587 lines): 12 dashboard sections including AI Executive Summary, ML Insights Panel (4 tabs), Black Friday Simulator, NL-to-SQL interface, and real-time cost monitoring — all with live Snowflake connectivity and glassmorphism UI.
- **Power BI** (8 pages, 97 DAX measures): 4 core analytics pages (Executive Overview, Customer Intelligence, ML Model Performance, Operational Performance) and 4 dedicated ML deep-dive pages (Behavioral Segmentation, Delivery Risk Prediction, Seller Risk Management, ML Model Registry). Advanced DAX includes time intelligence (MTD/QTD/YTD, MoM/YoY growth), Pareto/ABC analysis, Z-score anomaly detection, TREATAS virtual relationships for ML table integration, What-If CLV parameters, and RANKX geographic ranking.

### FinOps & Cost Optimization
Integrated cost monitoring via `cost_tracking.db`, surfaced directly in the dashboard. Features one-click Snowflake auto-suspend enforcement (60s idle) and automated resource monitors.

## 📜 License

This project is developed for academic purposes as a Final Year Project.

## 🙏 Acknowledgments

- **Olist** for the public dataset
- **dbt Labs** for the transformation framework
- **Snowflake** for the cloud data platform
