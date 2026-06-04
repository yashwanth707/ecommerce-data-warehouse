"""
Natural Language to SQL — Ollama-Powered Query Engine
=====================================================
Features:
- Schema-aware context injection
- Few-shot prompting with 5 examples
- Safety guardrails (read-only, row limit)
- Query validation and sanitization
- FastAPI endpoint for dashboard integration
"""

import os
import re
import json
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from loguru import logger

# Load .env from project root so credentials are available when run natively
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(_env_path)
except ImportError:
    pass  # Running inside Docker where env vars are injected directly


# Configuration

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:3b")
MAX_ROWS = 1000
QUERY_TIMEOUT = 300  # seconds — 3B model on CPU can take 60-120s cold-start

# Schema Context

SCHEMA_CONTEXT = """
You are a SQL expert for a Snowflake data warehouse. Generate ONLY valid Snowflake SQL queries.
All tables are in the ANALYTICS schema of the ECOMMERCE_DW database.

## Available Tables

### ANALYTICS.FACT_ORDERS (Fact table — grain: one row per ORDER)
- ORDER_KEY (VARCHAR, PK): Surrogate key
- ORDER_ID (VARCHAR): Business order ID
- CUSTOMER_KEY (VARCHAR, FK → DIM_CUSTOMERS.CUSTOMER_KEY)
- ORDER_DATE_KEY (DATE, FK → DIM_TIME.DATE_KEY): Use this to join with DIM_TIME
- ORDER_STATUS (VARCHAR): 'delivered', 'shipped', 'canceled', 'unavailable', 'processing', 'invoiced', 'approved'
- PAYMENT_TYPE (VARCHAR): 'credit_card', 'boleto', 'voucher', 'debit_card' (may contain comma-separated values)
- ORDER_PURCHASE_TIMESTAMP (TIMESTAMP): Full timestamp of order placement
- ORDER_PURCHASE_DATE (DATE): Date-only version of order timestamp
- ORDER_APPROVED_AT (TIMESTAMP): When order was approved
- ORDER_DELIVERED_CUSTOMER_DATE (TIMESTAMP): When order was delivered
- ORDER_ESTIMATED_DELIVERY_DATE (TIMESTAMP): Estimated delivery date
- TOTAL_ITEMS (INT): Number of line items in order
- DISTINCT_PRODUCTS (INT): Distinct products in the order
- DISTINCT_SELLERS (INT): Distinct sellers in the order
- TOTAL_PRODUCT_VALUE (FLOAT): Sum of all item prices in BRL
- TOTAL_FREIGHT (FLOAT): Total shipping cost in BRL
- TOTAL_VALUE (FLOAT): TOTAL_PRODUCT_VALUE + TOTAL_FREIGHT
- PAYMENT_VALUE (FLOAT): Actual amount paid by customer in BRL (USE THIS for revenue)
- PAYMENT_COUNT (INT): Number of payment transactions
- MAX_INSTALLMENTS (INT): Max installments used for payment
- DELIVERY_DAYS (INT): Actual days from purchase to delivery (NULL if not delivered)
- ESTIMATED_DELIVERY_DAYS (INT): Estimated delivery days
- DELIVERY_PERFORMANCE (VARCHAR): 'ON_TIME', 'LATE', or NULL
- AVG_REVIEW_SCORE (FLOAT): Average review rating for the order (1.0-5.0)
- REVIEW_COUNT (INT): Number of reviews for the order
- PROFIT_MARGIN (FLOAT): PAYMENT_VALUE - TOTAL_PRODUCT_VALUE - TOTAL_FREIGHT in BRL
- PROFIT_MARGIN_PCT (FLOAT): Profit margin as percentage
- FREIGHT_RATIO_PCT (FLOAT): Freight as percentage of product value

NOTE: FACT_ORDERS has NO PRODUCT_KEY — it cannot be joined to DIM_PRODUCTS directly.
NOTE: For revenue always use PAYMENT_VALUE. For product price use TOTAL_PRODUCT_VALUE.

### ANALYTICS.DIM_CUSTOMERS (Dimension — grain: one row per customer)
- CUSTOMER_KEY (VARCHAR, PK): Surrogate key
- CUSTOMER_ID (VARCHAR): Original business customer ID
- CUSTOMER_UNIQUE_ID (VARCHAR): De-duplicated customer ID (use for unique customer counts)
- CUSTOMER_ZIP_CODE_PREFIX (VARCHAR): 5-digit zip code prefix
- CUSTOMER_CITY (VARCHAR): City name
- CUSTOMER_STATE (VARCHAR): Two-letter state code (SP, RJ, MG, etc.)
- CUSTOMER_STATE_NAME (VARCHAR): Full state name (e.g. 'Sao Paulo', 'Rio de Janeiro')
- FIRST_ORDER_DATE (TIMESTAMP): Date of customer's first order
- LAST_ORDER_DATE (TIMESTAMP): Date of customer's most recent order
- TOTAL_ORDERS (INT): Lifetime total orders placed
- TOTAL_SPENT (FLOAT): Lifetime total payment value in BRL
- RECENCY_DAYS (INT): Days since last order (as of today)
- AVG_ORDER_VALUE (FLOAT): TOTAL_SPENT / TOTAL_ORDERS in BRL
- CUSTOMER_LIFETIME_DAYS (INT): Days between first and last order

### ANALYTICS.DIM_PRODUCTS (Dimension — grain: one row per product)
- PRODUCT_KEY (VARCHAR, PK): Surrogate key
- PRODUCT_ID (VARCHAR): Business product ID
- PRODUCT_CATEGORY_NAME (VARCHAR): Original Portuguese category name
- CATEGORY_NAME_ENGLISH (VARCHAR): Translated English category name
- PRODUCT_WEIGHT_G (FLOAT): Product weight in grams
- PRODUCT_LENGTH_CM / PRODUCT_HEIGHT_CM / PRODUCT_WIDTH_CM (FLOAT): Dimensions
- PRODUCT_VOLUME_CM3 (FLOAT): Volume in cubic cm
- WEIGHT_CATEGORY (VARCHAR): Light/Medium/Heavy classification
- PRODUCT_PHOTOS_QTY (INT): Number of product photos
- TOTAL_ORDERS (INT): How many orders included this product
- TOTAL_SOLD_QUANTITY (INT): Total units sold
- AVG_PRICE (FLOAT): Average selling price in BRL
- AVG_FREIGHT (FLOAT): Average freight cost in BRL
- AVG_REVIEW_SCORE (FLOAT): Average review score (1-5)
- TOTAL_REVIEWS (INT): Number of reviews
- POSITIVE_REVIEWS (INT): Count of positive reviews
- NEGATIVE_REVIEWS (INT): Count of negative reviews

NOTE: DIM_PRODUCTS has NO TOTAL_REVENUE column. Estimate revenue as: TOTAL_SOLD_QUANTITY * AVG_PRICE

### ANALYTICS.DIM_SELLERS (Dimension — grain: one row per seller)
- SELLER_KEY (VARCHAR, PK): Surrogate key
- SELLER_ID (VARCHAR): Business seller ID
- SELLER_ZIP_CODE_PREFIX (VARCHAR): Seller's zip code prefix
- SELLER_CITY (VARCHAR): Seller's city
- SELLER_STATE (VARCHAR): Two-letter state code
- SELLER_STATE_NAME (VARCHAR): Full state name
- TOTAL_ORDERS (INT): Total orders fulfilled by this seller
- TOTAL_ITEMS_SOLD (INT): Total items sold
- TOTAL_REVENUE (FLOAT): Total revenue generated by this seller in BRL
- AVG_ITEM_PRICE (FLOAT): Average price per item sold in BRL
- FIRST_SALE_DATE (TIMESTAMP): Date of first sale
- LAST_SALE_DATE (TIMESTAMP): Date of most recent sale

NOTE: DIM_SELLERS is standalone — it cannot be directly joined to FACT_ORDERS.

### ANALYTICS.DIM_TIME (Dimension — grain: one row per calendar day, 2016-2025)
- DATE_KEY (DATE, PK): Calendar date — join to FACT_ORDERS.ORDER_DATE_KEY
- DAY_OF_MONTH (INT): 1-31
- DAY_OF_WEEK (INT): 0=Sunday, 6=Saturday
- DAY_NAME (VARCHAR): 'Monday', 'Tuesday', etc.
- DAY_OF_YEAR (INT): 1-366
- WEEK_OF_YEAR (INT): ISO week number
- WEEK_START_DATE (DATE): Monday of the week
- MONTH_NUMBER (INT): 1-12
- MONTH_NAME (VARCHAR): 'January', 'February', etc.
- MONTH_START_DATE (DATE): First day of month
- MONTH_END_DATE (DATE): Last day of month
- QUARTER_NUMBER (INT): 1-4
- QUARTER_NAME (VARCHAR): 'Q1', 'Q2', 'Q3', 'Q4'
- YEAR_NUMBER (INT): Calendar year (2016, 2017, 2018, etc.)
- FISCAL_PERIOD (VARCHAR): e.g. 'FY2018-Q1'
- IS_WEEKEND (BOOLEAN): TRUE if Saturday or Sunday
- IS_HOLIDAY (BOOLEAN): TRUE if Brazilian public holiday
- IS_TODAY (BOOLEAN): TRUE if today's date
- IS_CURRENT_MTD (BOOLEAN): TRUE if within current month to date

### ANALYTICS.CUSTOMER_CLV (ML Results — grain: one row per customer)
- CUSTOMER_KEY (VARCHAR, FK → DIM_CUSTOMERS.CUSTOMER_KEY)
- RECENCY (INT): Days since last purchase at time of model run
- FREQUENCY (INT): Number of purchases
- MONETARY (FLOAT): Total spend in BRL
- SEGMENT (VARCHAR): Numeric cluster label (0, 1, 2, 3)
- SEGMENT_LABEL (VARCHAR): 'Champions', 'Loyal', 'Potential', 'At Risk'
- PREDICTED_CLV (FLOAT): Predicted Customer Lifetime Value in BRL

### ANALYTICS.CUSTOMER_SEGMENTS (ML Model 1 — Customer Behavioral Segmentation)
- CUSTOMER_KEY (VARCHAR, FK → DIM_CUSTOMERS.CUSTOMER_KEY)
- CLUSTER_ID (INT): Numeric cluster ID
- SEGMENT_LABEL (VARCHAR): e.g. 'Premium Cash Buyer', 'Satisfied Credit Shopper', 'Detractor / Poor Experience'
- AVG_ORDER_VALUE (FLOAT): Average ticket size
- AVG_INSTALLMENTS (FLOAT): Average number of installments used
- FREIGHT_RATIO (FLOAT): Freight as a % of total price
- AVG_REVIEW_SCORE (FLOAT): Average satisfaction score
- AVG_DELIVERY_DELTA (FLOAT): Average days late (or early)
- ORDER_COUNT (INT): Number of orders
- PREDICTED_CLV (FLOAT): Behavioral CLV prediction

### ANALYTICS.DELIVERY_RISK (ML Model 2 — Delivery Delay Prediction)
- ORDER_ID (VARCHAR, FK → FACT_ORDERS.ORDER_ID)
- IS_LATE_ACTUAL (INT): 1 if actually late, 0 if on time
- IS_LATE_PREDICTED (INT): Model's binary prediction (1=Late, 0=On Time)
- DELAY_PROBABILITY (FLOAT): Model's confidence score (0.0 to 1.0)
- RISK_TIER (VARCHAR): 'LOW RISK', 'MEDIUM RISK', 'HIGH RISK'

### ANALYTICS.SELLER_SEGMENTS (ML Model 3 — Seller Risk Clustering)
- SELLER_ID (VARCHAR, FK → DIM_SELLERS.SELLER_ID)
- CLUSTER_ID (INT): Numeric cluster ID
- SEGMENT_LABEL (VARCHAR): e.g. 'Top-Tier Reliable Seller', 'High-Risk / Poor Quality'
- TOTAL_REVENUE (FLOAT): Seller's total GMV
- TOTAL_ORDERS (INT): Number of orders fulfilled
- AVG_REVIEW_SCORE (FLOAT): Customer satisfaction
- AVG_DELIVERY_DELAY (FLOAT): Average delivery delay in days
- CANCELLATION_RATE (FLOAT): Percentage of orders cancelled
- RISK_SCORE (FLOAT): Composite risk score (lower is better/safer)

### ANALYTICS.ML_MODEL_REGISTRY (MLOps — Model Tracking)
- MODEL_NAME (VARCHAR): Name of the model
- MODEL_VERSION (VARCHAR): Timestamped version
- RUN_TIMESTAMP (TIMESTAMP): When it was executed
- PRIMARY_METRIC (VARCHAR): 'silhouette_score', 'f1_score', etc.
- METRIC_VALUE (FLOAT): The evaluation score
- THRESHOLD (FLOAT): The passing score required
- STATUS (VARCHAR): 'RETAINED' or 'DROPPED'
- RECORDS_SCORED (INT): Number of rows processed
- NOTES (VARCHAR): Description of features or results

## Join Reference
| From Table | To Table | Join Condition |
|------------|----------|----------------|
| FACT_ORDERS | DIM_CUSTOMERS | FACT_ORDERS.CUSTOMER_KEY = DIM_CUSTOMERS.CUSTOMER_KEY |
| FACT_ORDERS | DIM_TIME | FACT_ORDERS.ORDER_DATE_KEY = DIM_TIME.DATE_KEY |
| CUSTOMER_CLV | DIM_CUSTOMERS | CUSTOMER_CLV.CUSTOMER_KEY = DIM_CUSTOMERS.CUSTOMER_KEY |
| CUSTOMER_SEGMENTS | DIM_CUSTOMERS | CUSTOMER_SEGMENTS.CUSTOMER_KEY = DIM_CUSTOMERS.CUSTOMER_KEY |
| DELIVERY_RISK | FACT_ORDERS | DELIVERY_RISK.ORDER_ID = FACT_ORDERS.ORDER_ID |
| SELLER_SEGMENTS | DIM_SELLERS | SELLER_SEGMENTS.SELLER_ID = DIM_SELLERS.SELLER_ID |
| DIM_PRODUCTS | (standalone) | Cannot join to FACT_ORDERS directly |
| DIM_SELLERS | (standalone) | Cannot join to FACT_ORDERS directly |

## Important Rules
1. ALWAYS prefix tables with schema: ANALYTICS.TABLE_NAME
2. Use short aliases: f=FACT_ORDERS, c=DIM_CUSTOMERS, t=DIM_TIME, p=DIM_PRODUCTS, s=DIM_SELLERS, clv=CUSTOMER_CLV
3. All column names are UPPERCASE
4. Currency is BRL (Brazilian Real). Always use ROUND(..., 2) for money values
5. ALWAYS add LIMIT {max_rows} to prevent large result sets
6. ONLY generate SELECT or WITH (CTE) queries — NEVER INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE
7. Data date range: September 2016 to October 2018
8. ORDER_STATUS values use UPPERCASE: 'DELIVERED', 'SHIPPED', 'CANCELED'
9. DELIVERY_PERFORMANCE values use UPPERCASE: 'ON_TIME', 'LATE', 'PENDING'
10. PAYMENT_TYPE values use lowercase: 'credit_card', 'boleto', 'voucher', 'debit_card'
11. When asked about revenue, use PAYMENT_VALUE from FACT_ORDERS (not TOTAL_VALUE)
188: """

# Few-Shot Examples

FEW_SHOT_EXAMPLES = [
    {
        "question": "What are the top 5 product categories by estimated revenue?",
        "sql": """SELECT
    CATEGORY_NAME_ENGLISH AS category,
    SUM(TOTAL_SOLD_QUANTITY) AS total_units_sold,
    ROUND(SUM(TOTAL_SOLD_QUANTITY * AVG_PRICE), 2) AS estimated_revenue
FROM ANALYTICS.DIM_PRODUCTS
WHERE CATEGORY_NAME_ENGLISH IS NOT NULL
GROUP BY CATEGORY_NAME_ENGLISH
ORDER BY estimated_revenue DESC
LIMIT 5""",
    },
    {
        "question": "Show me the monthly revenue trend for 2018",
        "sql": """SELECT
    t.MONTH_NUMBER,
    t.MONTH_NAME,
    COUNT(DISTINCT f.ORDER_ID) AS order_count,
    ROUND(SUM(f.PAYMENT_VALUE), 2) AS revenue
FROM ANALYTICS.FACT_ORDERS f
INNER JOIN ANALYTICS.DIM_TIME t ON f.ORDER_DATE_KEY = t.DATE_KEY
WHERE t.YEAR_NUMBER = 2018
  AND f.ORDER_STATUS = 'DELIVERED'
GROUP BY t.MONTH_NUMBER, t.MONTH_NAME
ORDER BY t.MONTH_NUMBER
LIMIT 12""",
    },
    {
        "question": "Which states have the highest average order value?",
        "sql": """SELECT
    c.CUSTOMER_STATE,
    c.CUSTOMER_STATE_NAME,
    ROUND(AVG(f.PAYMENT_VALUE), 2) AS avg_order_value,
    COUNT(DISTINCT f.ORDER_ID) AS total_orders
FROM ANALYTICS.FACT_ORDERS f
INNER JOIN ANALYTICS.DIM_CUSTOMERS c ON f.CUSTOMER_KEY = c.CUSTOMER_KEY
WHERE f.ORDER_STATUS = 'DELIVERED'
GROUP BY c.CUSTOMER_STATE, c.CUSTOMER_STATE_NAME
HAVING COUNT(DISTINCT f.ORDER_ID) > 100
ORDER BY avg_order_value DESC
LIMIT 10""",
    },
    {
        "question": "Show me top 5 customers by predicted CLV in Sao Paulo",
        "sql": """SELECT
    c.CUSTOMER_UNIQUE_ID,
    c.CUSTOMER_CITY,
    c.TOTAL_SPENT,
    clv.SEGMENT_LABEL,
    ROUND(clv.PREDICTED_CLV, 2) AS predicted_clv
FROM ANALYTICS.CUSTOMER_CLV clv
INNER JOIN ANALYTICS.DIM_CUSTOMERS c ON clv.CUSTOMER_KEY = c.CUSTOMER_KEY
WHERE c.CUSTOMER_STATE = 'SP'
ORDER BY clv.PREDICTED_CLV DESC
LIMIT 5""",
    },
    {
        "question": "What is the total revenue by payment method?",
        "sql": """SELECT
    PAYMENT_TYPE,
    COUNT(DISTINCT ORDER_ID) AS total_orders,
    ROUND(SUM(PAYMENT_VALUE), 2) AS total_revenue,
    ROUND(AVG(PAYMENT_VALUE), 2) AS avg_order_value
FROM ANALYTICS.FACT_ORDERS
WHERE PAYMENT_TYPE IS NOT NULL
GROUP BY PAYMENT_TYPE
ORDER BY total_revenue DESC
LIMIT 10""",
    },
    {
        "question": "What is the late delivery rate by state?",
        "sql": """SELECT
    c.CUSTOMER_STATE,
    COUNT(*) AS total_delivered_orders,
    SUM(CASE WHEN f.DELIVERY_PERFORMANCE = 'LATE' THEN 1 ELSE 0 END) AS late_orders,
    ROUND(SUM(CASE WHEN f.DELIVERY_PERFORMANCE = 'LATE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_pct
FROM ANALYTICS.FACT_ORDERS f
INNER JOIN ANALYTICS.DIM_CUSTOMERS c ON f.CUSTOMER_KEY = c.CUSTOMER_KEY
WHERE f.ORDER_STATUS = 'DELIVERED'
  AND f.DELIVERY_PERFORMANCE IS NOT NULL
GROUP BY c.CUSTOMER_STATE
ORDER BY late_pct DESC
LIMIT 15""",
    },
    {
        "question": "How many customers are in each CLV segment?",
        "sql": """SELECT
    SEGMENT_LABEL,
    COUNT(*) AS customer_count,
    ROUND(AVG(PREDICTED_CLV), 2) AS avg_predicted_clv,
    ROUND(AVG(MONETARY), 2) AS avg_total_spend,
    ROUND(AVG(RECENCY), 0) AS avg_recency_days
FROM ANALYTICS.CUSTOMER_CLV
GROUP BY SEGMENT_LABEL
ORDER BY avg_predicted_clv DESC
LIMIT 10""",
    },
    {
        "question": "Show total revenue and orders by quarter",
        "sql": """SELECT
    t.YEAR_NUMBER,
    t.QUARTER_NAME,
    COUNT(DISTINCT f.ORDER_ID) AS total_orders,
    ROUND(SUM(f.PAYMENT_VALUE), 2) AS total_revenue,
    ROUND(AVG(f.PAYMENT_VALUE), 2) AS avg_order_value
FROM ANALYTICS.FACT_ORDERS f
INNER JOIN ANALYTICS.DIM_TIME t ON f.ORDER_DATE_KEY = t.DATE_KEY
WHERE f.ORDER_STATUS = 'DELIVERED'
GROUP BY t.YEAR_NUMBER, t.QUARTER_NUMBER, t.QUARTER_NAME
ORDER BY t.YEAR_NUMBER, t.QUARTER_NUMBER
LIMIT 20""",
    },
    {
        "question": "Which are the top 10 sellers by revenue?",
        "sql": """SELECT
    SELLER_ID,
    SELLER_CITY,
    SELLER_STATE_NAME,
    TOTAL_ORDERS,
    TOTAL_ITEMS_SOLD,
    ROUND(TOTAL_REVENUE, 2) AS total_revenue,
    ROUND(AVG_ITEM_PRICE, 2) AS avg_item_price
FROM ANALYTICS.DIM_SELLERS
ORDER BY TOTAL_REVENUE DESC
LIMIT 10""",
    },
    {
        "question": "What is the average review score by order status?",
        "sql": """SELECT
    ORDER_STATUS,
    COUNT(DISTINCT ORDER_ID) AS total_orders,
    ROUND(AVG(AVG_REVIEW_SCORE), 2) AS avg_review_score,
    ROUND(AVG(DELIVERY_DAYS), 1) AS avg_delivery_days
FROM ANALYTICS.FACT_ORDERS
WHERE AVG_REVIEW_SCORE > 0
GROUP BY ORDER_STATUS
ORDER BY avg_review_score DESC
LIMIT 10""",
    },
    {
        "question": "How many high risk delivery orders do we have and what is their average delay probability?",
        "sql": """SELECT
    RISK_TIER,
    COUNT(*) AS total_orders,
    ROUND(AVG(DELAY_PROBABILITY) * 100, 2) AS avg_probability_pct,
    SUM(IS_LATE_ACTUAL) AS actual_late_orders
FROM ANALYTICS.DELIVERY_RISK
GROUP BY RISK_TIER
ORDER BY avg_probability_pct DESC
LIMIT 10""",
    },
    {
        "question": "Show me the top 5 high risk sellers by revenue",
        "sql": """SELECT
    s.SELLER_ID,
    s.SELLER_CITY,
    s.SELLER_STATE,
    ss.SEGMENT_LABEL,
    ss.RISK_SCORE,
    ROUND(ss.TOTAL_REVENUE, 2) AS total_revenue
FROM ANALYTICS.SELLER_SEGMENTS ss
INNER JOIN ANALYTICS.DIM_SELLERS s ON ss.SELLER_ID = s.SELLER_ID
WHERE ss.SEGMENT_LABEL LIKE '%High-Risk%' OR ss.RISK_SCORE > 0.5
ORDER BY ss.TOTAL_REVENUE DESC
LIMIT 5""",
    },
]


# NL-to-SQL Engine

class NLToSQLEngine:
    """
    Converts natural language questions to SQL queries
    using Ollama (local LLM).
    """

    def __init__(
        self,
        ollama_host: str = OLLAMA_HOST,
        model: str = OLLAMA_MODEL,
    ):
        self.ollama_host = ollama_host
        self.model = model
        self.client = httpx.Client(timeout=QUERY_TIMEOUT)

    def _call_ollama(self, prompt: str, temperature: float = 0.1) -> str:
        """Helper to call Ollama generate API."""
        try:
            response = self.client.post(
                f"{self.ollama_host}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "top_p": 0.9,
                        "num_predict": 500,
                    },
                },
                timeout=QUERY_TIMEOUT,
            )
            response.raise_for_status()
            result = response.json()
            return result.get("response", "")
        except Exception as e:
            logger.error(f"Ollama API error: {e}")
            raise RuntimeError(f"Failed to generate from Ollama: {e}")

    def generate_sql(self, question: str) -> str:
        """
        Convert a natural language question to a SQL query.

        Args:
            question: Natural language question about the data

        Returns:
            Validated SQL query string
        """
        prompt = self._build_prompt(question)
        raw_sql = self._call_ollama(prompt, temperature=0.1)

        # Extract SQL from response
        sql = self._extract_sql(raw_sql)

        # Validate safety
        sql = self._validate_sql(sql)

        logger.info(f"Generated SQL for: '{question}'")
        return sql

    def _build_prompt(self, question: str) -> str:
        """Build the full prompt with schema context and few-shot examples."""
        examples_text = "\n\n".join(
            [
                f"Question: {ex['question']}\nSQL:\n```sql\n{ex['sql']}\n```"
                for ex in FEW_SHOT_EXAMPLES
            ]
        )

        return f"""{SCHEMA_CONTEXT.format(max_rows=MAX_ROWS)}

## Examples

{examples_text}

## Your Task

Convert the following question to a valid Snowflake SQL query.
Return ONLY the SQL query, no explanations.

Question: {question}
SQL:
"""

    def generate_executive_summary(self, kpi_data: dict) -> str:
        """Generate a 3-bullet executive summary from KPIs."""
        prompt = f"""You are a senior data analyst summarizing e-commerce performance for a CEO.
Based on the following KPIs, write a professional 3-bullet point executive summary.
Do not include any introductory or concluding text, just the 3 bullet points.

KPIs:
{json.dumps(kpi_data, indent=2)}
"""
        return self._call_ollama(prompt, temperature=0.3)

    def generate_churn_explanation(self, customer_data: dict) -> str:
        """Generate churn reasons and retention recommendations."""
        prompt = f"""You are an expert retention strategist.
Review this "At-Risk" customer profile who hasn't purchased recently.
Provide exactly two sections:
1. Likely Churn Reasons: 2 short bullet points explaining why they might be churning based on the data.
2. Retention Recommendations: 2 specific, actionable marketing actions to win them back.
Do not include any introductory or concluding text. Be concise and professional.

Customer Profile:
{json.dumps(customer_data, indent=2)}
"""
        return self._call_ollama(prompt, temperature=0.3)

    def explain_sql(self, sql: str, original_question: str = "") -> str:
        """Explain what a SQL query does in plain English."""
        context = f'The user originally asked: "{original_question}"\n' if original_question else ""
        prompt = f"""You are a data analyst explaining a SQL query to a non-technical business stakeholder.
{context}
Explain the following Snowflake SQL query in plain English.
Keep it to 3-4 sentences maximum. Cover:
- What data it retrieves
- Any filters or conditions applied
- How the results are sorted or aggregated
Do NOT repeat the SQL. Do NOT include any code.

SQL Query:
{sql}
"""
        return self._call_ollama(prompt, temperature=0.2)

    def narrate_results(self, question: str, sql: str, results: list, row_count: int) -> str:
        """Narrate key insights from query results in plain English."""
        # Limit results sent to model to avoid huge prompts
        sample = results[:10]
        prompt = f"""You are a senior data analyst presenting findings to a business executive.
A user asked: "{question}"
The query returned {row_count} rows. Here are the top results:

{json.dumps(sample, indent=2)}

Write a concise 3-bullet narrative of the key insights from these results.
Focus on the most interesting patterns, outliers, or business implications.
Do NOT include any introductory or concluding text. Start directly with the bullet points.
Always mention specific numbers from the data.
"""
        return self._call_ollama(prompt, temperature=0.4)

    def summarize_dq_failures(self, failures: list, pipeline_run: str) -> str:
        """Summarize data quality failures in plain English for non-technical stakeholders."""
        prompt = f"""You are a data operations lead explaining pipeline quality issues to a business manager.
The pipeline run '{pipeline_run}' encountered the following data quality failures:

{json.dumps(failures, indent=2)}

Write a plain-English summary covering:
1. What went wrong (1 sentence per failure — non-technical language)
2. Business impact (what data might be missing or wrong)
3. Recommended next step (2-3 words: e.g. 'Re-upload source data', 'Check S3 bucket')

Keep it under 100 words total. Do NOT include any code or SQL.
"""
        return self._call_ollama(prompt, temperature=0.3)

    def _extract_sql(self, raw_response: str) -> str:
        """Extract SQL from LLM response (may be wrapped in markdown)."""
        # Try to extract from code block
        sql_match = re.search(
            r"```(?:sql)?\s*\n?(.*?)\n?```",
            raw_response,
            re.DOTALL | re.IGNORECASE,
        )
        if sql_match:
            return sql_match.group(1).strip()

        # Try to find SELECT statement directly
        select_match = re.search(
            r"(SELECT\s+.*)",
            raw_response,
            re.DOTALL | re.IGNORECASE,
        )
        if select_match:
            return select_match.group(1).strip()

        raise ValueError(
            "Could not extract SQL from LLM response"
        )

    def _validate_sql(self, sql: str) -> str:
        """
        Validate SQL for safety:
        - Must be a SELECT statement
        - No DDL/DML commands
        - Must have a LIMIT clause
        """
        sql_upper = sql.upper().strip()

        # Must start with SELECT or WITH (CTE)
        if not (
            sql_upper.startswith("SELECT")
            or sql_upper.startswith("WITH")
        ):
            raise ValueError(
                "Only SELECT queries are allowed"
            )

        # Block dangerous operations
        blocked_keywords = [
            "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
            "CREATE", "TRUNCATE", "GRANT", "REVOKE", "EXEC",
            "EXECUTE", "MERGE",
        ]
        for keyword in blocked_keywords:
            # Check for keyword as a standalone word
            if re.search(
                rf"\b{keyword}\b", sql_upper
            ):
                raise ValueError(
                    f"Blocked operation detected: {keyword}"
                )

        # Ensure LIMIT clause exists
        if "LIMIT" not in sql_upper:
            sql = sql.rstrip(";") + f"\nLIMIT {MAX_ROWS}"

        # Ensure semicolon at end
        if not sql.rstrip().endswith(";"):
            sql = sql.rstrip() + ";"

        return sql

    def close(self):
        """Close HTTP client."""
        self.client.close()


# FastAPI Application

app = FastAPI(
    title="E-Commerce NL-to-SQL API",
    description="Convert natural language questions to SQL queries",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = NLToSQLEngine()


class QueryRequest(BaseModel):
    """Request body for NL-to-SQL conversion."""
    question: str


class QueryResponse(BaseModel):
    """Response with generated SQL query."""
    question: str
    sql: str
    status: str = "success"


class ErrorResponse(BaseModel):
    """Error response."""
    question: str
    error: str
    status: str = "error"


class SummaryRequest(BaseModel):
    kpis: dict


class SummaryResponse(BaseModel):
    summary: str
    status: str = "success"


class ChurnRequest(BaseModel):
    customer_data: dict


class ChurnResponse(BaseModel):
    explanation: str
    status: str = "success"


class ExplainSQLRequest(BaseModel):
    sql: str
    question: str = ""  # original NL question (optional context)


class ExplainSQLResponse(BaseModel):
    explanation: str
    status: str = "success"


class NarrateRequest(BaseModel):
    question: str
    sql: str
    results: list       # list of row dicts
    row_count: int


class NarrateResponse(BaseModel):
    narrative: str
    status: str = "success"


class DQSummaryRequest(BaseModel):
    failures: list   # list of failure dicts e.g. [{"table": "SILVER_PAYMENTS", "actual": 3, "threshold": 100}]
    pipeline_run: str = "latest"


class DQSummaryResponse(BaseModel):
    summary: str
    status: str = "success"


@app.post("/api/query", response_model=QueryResponse)
async def generate_query(request: QueryRequest):
    """
    Convert a natural language question to a SQL query.

    Example:
        POST /api/query
        {"question": "Top 5 customers by spend in São Paulo"}
    """
    try:
        sql = engine.generate_sql(request.question)
        return QueryResponse(
            question=request.question,
            sql=sql,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/api/summary", response_model=SummaryResponse)
async def generate_summary(request: SummaryRequest):
    """Generate executive summary from KPIs."""
    try:
        summary = engine.generate_executive_summary(request.kpis)
        return SummaryResponse(summary=summary)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/api/churn_explain", response_model=ChurnResponse)
async def generate_churn(request: ChurnRequest):
    """Generate churn anomaly explanation."""
    try:
        explanation = engine.generate_churn_explanation(request.customer_data)
        return ChurnResponse(explanation=explanation)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/api/explain_sql", response_model=ExplainSQLResponse)
async def explain_sql(request: ExplainSQLRequest):
    """Explain a SQL query in plain English."""
    try:
        explanation = engine.explain_sql(request.sql, request.question)
        return ExplainSQLResponse(explanation=explanation)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/api/narrate", response_model=NarrateResponse)
async def narrate_results(request: NarrateRequest):
    """Narrate key insights from query results."""
    try:
        narrative = engine.narrate_results(
            request.question, request.sql, request.results, request.row_count
        )
        return NarrateResponse(narrative=narrative)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/api/dq_summary", response_model=DQSummaryResponse)
async def summarize_dq(request: DQSummaryRequest):
    """Summarize data quality failures in plain English."""
    try:
        summary = engine.summarize_dq_failures(request.failures, request.pipeline_run)
        return DQSummaryResponse(summary=summary)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/api/examples")
async def get_examples():
    """Return the few-shot examples for reference."""
    return {"examples": FEW_SHOT_EXAMPLES}


@app.get("/api/schema")
async def get_schema():
    """Return the database schema context."""
    return {"schema": SCHEMA_CONTEXT.format(max_rows=MAX_ROWS)}


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    # Check Ollama connectivity
    try:
        response = httpx.get(
            f"{OLLAMA_HOST}/api/tags", timeout=5
        )
        ollama_status = "connected" if response.status_code == 200 else "error"
    except Exception:
        ollama_status = "disconnected"

    return {
        "status": "healthy",
        "ollama": ollama_status,
        "model": OLLAMA_MODEL,
    }


# CLI Entry Point

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "nl_to_sql:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
