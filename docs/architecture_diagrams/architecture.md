# Architecture Documentation

## 1. End-to-End Data Flow (Medallion Architecture)

```mermaid
flowchart LR
    subgraph Sources["📥 Data Sources"]
        S3["AWS S3\n(Raw CSV/JSON)"]
    end

    subgraph Bronze["🥉 Bronze Layer"]
        direction TB
        B_Orders["orders\n(VARIANT)"]
        B_Customers["customers\n(VARIANT)"]
        B_Products["products\n(VARIANT)"]
        B_Items["order_items\n(VARIANT)"]
        B_Payments["payments\n(VARIANT)"]
        B_Geo["geolocation\n(VARIANT)"]
        B_Reviews["reviews\n(VARIANT)"]
        B_Sellers["sellers\n(VARIANT)"]
    end

    subgraph Silver["🥈 Silver Layer"]
        direction TB
        S_Orders["silver_orders\n(Cleansed, Typed)"]
        S_Customers["silver_customers\n(Standardized)"]
        S_Products["silver_products\n(Enriched)"]
        S_Items["silver_order_items\n(Calculated)"]
        S_Payments["silver_payments\n(Validated)"]
        S_Reviews["silver_reviews\n(Deduplicated)"]
    end

    subgraph Gold["🥇 Gold Layer — Star Schema"]
        direction TB
        DimC["dim_customers\n(SCD Type 2)"]
        DimP["dim_products\n(Hierarchy)"]
        DimT["dim_time\n(Date Spine)"]
        DimS["dim_sellers\n(Location)"]
        Fact["fact_orders\n(Measures)"]
    end

    subgraph ML["🤖 Innovation Layer"]
        CLV["Snowpark\nCLV Model"]
        NL["NL-to-SQL\n(Ollama)"]
    end

    subgraph BI["📊 BI Layer"]
        Dash["Streamlit\nDashboard"]
    end

    S3 -->|"Python Ingestion\n(Schema-on-Read)"| Bronze
    Bronze -->|"dbt Incremental\n(MERGE)"| Silver
    Silver -->|"dbt Materialized\n(Star Schema)"| Gold
    Gold --> CLV
    Gold --> Dash
    CLV -->|"customer_clv"| Dash
    NL -->|"Dynamic SQL"| Dash

    style Sources fill:#232F3E,stroke:#FF9900,color:#fff
    style Bronze fill:#CD7F32,stroke:#8B5A2B,color:#fff
    style Silver fill:#C0C0C0,stroke:#808080,color:#000
    style Gold fill:#FFD700,stroke:#DAA520,color:#000
    style ML fill:#7B2FBE,stroke:#5B1F8E,color:#fff
    style BI fill:#FF4B4B,stroke:#CC3333,color:#fff
```

## 2. Tool Integration & Orchestration

```mermaid
flowchart TB
    subgraph Orchestration["⚙️ Apache Airflow"]
        Sensor["S3 File Sensor"]
        Ingest["Bronze Ingestion\n(PythonOperator)"]
        DBT_S["dbt run --select silver\n(BashOperator)"]
        DBT_G["dbt run --select gold\n(BashOperator)"]
        CLV_Task["CLV Calculation\n(SnowflakeOperator)"]
        DQ["Data Quality\n(TaskGroup)"]
        BI_Refresh["BI Cache Refresh\n(PythonOperator)"]

        Sensor --> Ingest --> DBT_S --> DBT_G --> CLV_Task --> DQ --> BI_Refresh
    end

    subgraph SelfHeal["🔧 Self-Healing"]
        Repair["Auto-Repair SQL"]
        Alert["Slack / PagerDuty"]
    end

    subgraph Snowflake["❄️ Snowflake"]
        WH["Warehouse\n(Auto-Suspend 60s)"]
        Stages["Internal Stages"]
        SP["Stored Procedures"]
    end

    subgraph Monitoring["📈 Observability"]
        Cost["Credit Tracking\n(SQLite)"]
        SLA["SLA Monitor\n(2h threshold)"]
    end

    DQ -->|"Test Failure"| SelfHeal
    Repair -->|"Fix & Retry"| DBT_S
    Alert -->|"Notify"| SLA
    Orchestration --> Snowflake
    Orchestration --> Monitoring

    style Orchestration fill:#017CEE,stroke:#0056A3,color:#fff
    style SelfHeal fill:#28A745,stroke:#1E7E34,color:#fff
    style Snowflake fill:#29B5E8,stroke:#1A8CB8,color:#fff
    style Monitoring fill:#FFC107,stroke:#D39E00,color:#000
```

## 3. Cost Optimization Strategy

```mermaid
mindmap
  root((Cost<br/>Optimization))
    Compute
      Auto-suspend warehouse after 60s
      Right-size warehouse per task
      Pool concurrent queries in Airflow
    Storage
      Transient tables for Bronze/Silver
      Automatic clustering on Gold
      Time Travel reduced to 1 day for staging
    Processing
      Incremental dbt models
      Partition pruning via merge keys
      Result caching enabled
    Monitoring
      Per-task credit tracking
      Daily cost reports
      Anomaly alerting on spend spikes
```

## 4. Dimensional Model (Star Schema)

```mermaid
erDiagram
    FACT_ORDERS ||--o{ DIM_CUSTOMERS : "customer_key"
    FACT_ORDERS ||--o{ DIM_PRODUCTS : "product_key"
    FACT_ORDERS ||--o{ DIM_TIME : "order_date_key"
    FACT_ORDERS ||--o{ DIM_SELLERS : "seller_key"

    DIM_CUSTOMERS {
        string customer_key PK
        string customer_id
        string customer_unique_id
        string customer_city
        string customer_state
        string customer_zip_code
        date first_order_date
        date last_order_date
        int total_orders
        float total_spent
        int recency_days
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }

    DIM_PRODUCTS {
        string product_key PK
        string product_id
        string category_name
        string category_name_english
        float avg_review_score
        int total_sold_quantity
        float product_weight_g
        float product_length_cm
        float product_height_cm
        float product_width_cm
    }

    DIM_TIME {
        date date_key PK
        int day_of_month
        int day_of_week
        string day_name
        int week_of_year
        int month_number
        string month_name
        int quarter
        int year
        boolean is_weekend
        boolean is_holiday
        string fiscal_period
    }

    DIM_SELLERS {
        string seller_key PK
        string seller_id
        string seller_city
        string seller_state
        string seller_zip_code
    }

    FACT_ORDERS {
        string order_key PK
        string order_id
        string customer_key FK
        string product_key FK
        date order_date_key FK
        string seller_key FK
        string order_status
        string payment_type
        int total_items
        float total_value
        float total_freight
        float payment_value
        int delivery_days
        string delivery_performance
        float profit_margin
        timestamp order_purchase_timestamp
        timestamp order_delivered_timestamp
    }
```
