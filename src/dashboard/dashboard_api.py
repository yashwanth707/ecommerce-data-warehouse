"""
FastAPI Backend for React Dashboard
===================================
Serves real-time data from Snowflake Gold Layer to the React frontend.
Enhanced with dynamic filtering.
"""

import os
from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import pandas as pd
import snowflake.connector

# Import NL-to-SQL logic if available
try:
    import sys
    sys.path.append(str(Path(__file__).resolve().parents[1] / "innovations"))
    from nl_to_sql import generate_sql
    HAS_NL = True
except ImportError:
    HAS_NL = False

# Load .env
env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(env_path)

app = FastAPI(title="E-Commerce Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def execute_query(query: str) -> pd.DataFrame:
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ECOMMERCE_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DW"),
            schema="ANALYTICS",
        )
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            df = pd.read_sql(query, conn)
            
        conn.close()
        df.columns = [c.lower() for c in df.columns]
        return df
    except Exception as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def _build_filter_sql(state: Optional[str] = None, status: Optional[str] = None) -> tuple[str, str]:
    """Helper to build dynamic SQL JOINs and WHERE clauses depending on requested filters."""
    joins = []
    wheres = []

    if state and state.lower() not in ['all', '']:
        joins.append("INNER JOIN ANALYTICS.DIM_CUSTOMERS C ON O.CUSTOMER_KEY = C.CUSTOMER_KEY")
        # clean input roughly
        safe_state = "".join(x for x in state if x.isalnum())
        wheres.append(f"C.CUSTOMER_STATE = '{safe_state}'")

    if status and status.lower() not in ['all', '']:
        safe_status = "".join(x for x in status if x.isalnum() or x in [" ", "_"]).upper()
        wheres.append(f"O.ORDER_STATUS = '{safe_status}'")

    join_sql = "\n".join(joins)
    where_sql = (" AND " + " AND ".join(wheres)) if wheres else ""
    return join_sql, where_sql

@app.get("/api/health")
def health_check():
    return {"status": "healthy"}

@app.get("/api/kpis")
def get_kpis(state: Optional[str] = Query(None), status: Optional[str] = Query(None)):
    join_sql, where_sql = _build_filter_sql(state, status)
    
    query = f"""
    SELECT 
        SUM(O.TOTAL_VALUE) AS total_revenue,
        COUNT(DISTINCT O.ORDER_ID) AS total_orders,
        COUNT(DISTINCT O.CUSTOMER_KEY) AS total_customers,
        AVG(O.AVG_REVIEW_SCORE) AS avg_review_score
    FROM ANALYTICS.FACT_ORDERS O
    {join_sql}
    WHERE 1=1 {where_sql}
    """
    df = execute_query(query)
    if df.empty:
        return {"total_revenue": 0, "total_orders": 0, "total_customers": 0, "avg_review_score": 0}

    row = df.iloc[0]
    return {
        "total_revenue": float(row["total_revenue"] or 0),
        "total_orders": int(row["total_orders"] or 0),
        "total_customers": int(row["total_customers"] or 0),
        "avg_review_score": float(row["avg_review_score"] or 0)
    }

@app.get("/api/revenue-trend")
def get_revenue_trend(state: Optional[str] = Query(None), status: Optional[str] = Query(None)):
    join_sql, where_sql = _build_filter_sql(state, status)
    
    query = f"""
    SELECT 
        T.MONTH_NAME AS month,
        T.MONTH_NUMBER,
        ROUND(SUM(O.TOTAL_VALUE), 2) AS revenue,
        COUNT(DISTINCT O.ORDER_ID) AS orders
    FROM ANALYTICS.FACT_ORDERS O
    INNER JOIN ANALYTICS.DIM_TIME T ON O.ORDER_DATE_KEY = T.DATE_KEY
    {join_sql}
    WHERE T.YEAR_NUMBER = 2018 {where_sql}
    GROUP BY T.MONTH_NAME, T.MONTH_NUMBER
    ORDER BY T.MONTH_NUMBER
    """
    df = execute_query(query)
    df['month'] = df['month'].astype(str).str[:3]
    return df.to_dict(orient="records")

@app.get("/api/states-revenue")
def get_states_revenue(status: Optional[str] = Query(None)):
    """New endpoint for geographical bar chart, ignores state filter to show comparison"""
    _, where_sql = _build_filter_sql(None, status)
    query = f"""
    SELECT 
        C.CUSTOMER_STATE AS state,
        ROUND(SUM(O.TOTAL_VALUE), 2) AS revenue
    FROM ANALYTICS.FACT_ORDERS O
    INNER JOIN ANALYTICS.DIM_CUSTOMERS C ON O.CUSTOMER_KEY = C.CUSTOMER_KEY
    WHERE 1=1 {where_sql}
    GROUP BY C.CUSTOMER_STATE
    ORDER BY revenue DESC
    LIMIT 10
    """
    df = execute_query(query)
    return df.to_dict(orient="records")

@app.get("/api/top-categories")
def get_top_categories():
    """Static top products (cross-join mockup logic simplified)"""
    query = """
    SELECT 
        P.CATEGORY_NAME_ENGLISH AS name,
        TOTAL_SOLD_QUANTITY AS sales,
        TOTAL_SOLD_QUANTITY AS growth
    FROM ANALYTICS.DIM_PRODUCTS P
    WHERE P.CATEGORY_NAME_ENGLISH IS NOT NULL
    ORDER BY TOTAL_SOLD_QUANTITY DESC NULLS LAST
    LIMIT 8
    """
    df = execute_query(query)
    if df.empty: return []
    records = df.to_dict(orient="records")
    return records

@app.get("/api/customer-segments")
def get_customer_segments():
    query = """
    SELECT 
        SEGMENT_LABEL AS name,
        COUNT(DISTINCT CUSTOMER_KEY) AS value
    FROM ANALYTICS.CUSTOMER_CLV
    GROUP BY SEGMENT_LABEL
    ORDER BY value DESC
    """
    df = execute_query(query)
    total = df['value'].sum()
    if total > 0:
        df['value'] = (df['value'] / total * 100).round(1)
        
    colors = {'Champions': '#10b981', 'Loyal': '#3b82f6', 'Potential': '#8b5cf6', 'At Risk': '#ef4444'}
    records = df.to_dict(orient="records")
    for r in records: r['color'] = colors.get(r['name'], '#6b7280')
    return records

class QueryRequest(BaseModel):
    question: str

@app.post("/api/query")
def process_nl_query(request: QueryRequest):
    if not HAS_NL:
        raise HTTPException(status_code=501, detail="NL-to-SQL module not configured")
    try:
        sql = generate_sql(request.question)
        if not sql: raise HTTPException(status_code=500, detail="Failed to generate SQL")
        return {"sql": sql}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard_api:app", host="0.0.0.0", port=8000, reload=True)
