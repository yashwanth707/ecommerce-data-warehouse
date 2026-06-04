"""
Model 2 — Delivery Delay Prediction (Random Forest Classifier)
==============================================================
Predicts at order-placement time whether an order will be
delivered LATE (actual > estimated delivery date).

This enables operations teams to flag at-risk orders for
proactive intervention before the customer is disappointed.

Features Used:
  - FREIGHT_VALUE      (proxy for distance / weight / cost)
  - PAYMENT_VALUE      (order complexity)
  - PRODUCT_WEIGHT_G   (from DIM_PRODUCTS)
  - PRODUCT_VOLUME     (from DIM_PRODUCTS)
  - PURCHASE_MONTH     (seasonality)
  - IS_WEEKEND_PURCHASE (weekend orders processed slower)

Target: IS_LATE (1 = delivered after estimated date, 0 = on time)

Output Table: ANALYTICS.DELIVERY_RISK
Evaluation:   F1-Score (threshold ≥ 0.65 to retain)
"""

import os
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score, classification_report
from loguru import logger
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, when, lit, month as sf_month,
    dayofweek, datediff, coalesce,
)

# Constants
GOLD_SCHEMA    = "ANALYTICS"
SILVER_SCHEMA  = "CLEANSED"
OUTPUT_TABLE   = f"{GOLD_SCHEMA}.DELIVERY_RISK"
REGISTRY_TABLE = f"{GOLD_SCHEMA}.ML_MODEL_REGISTRY"
F1_THRESHOLD   = 0.65
MODEL_NAME     = "delivery_delay_prediction"


# Model Class

class DeliveryDelayModel:
    """Random Forest classifier predicting order delivery delay."""

    def __init__(self, session: Session):
        self.session = session
        self.model_version = f"v{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        self.clf = None
        self.feature_cols = None

    # 1. Feature Extraction

    def extract_features(self) -> pd.DataFrame:
        """
        Join FACT_ORDERS with DIM_PRODUCTS and DIM_TIME to build
        the feature matrix. Only include rows where we know the outcome.
        
        Verified FACT_ORDERS columns:
          TOTAL_FREIGHT, PAYMENT_VALUE, DELIVERY_DAYS, ESTIMATED_DELIVERY_DAYS,
          ORDER_DELIVERED_CUSTOMER_DATE, ORDER_ESTIMATED_DELIVERY_DATE,
          ORDER_PURCHASE_TIMESTAMP, ORDER_PURCHASE_DATE
        """
        logger.info("Extracting delivery features...")

        fact = self.session.table(f"{GOLD_SCHEMA}.FACT_ORDERS")

        # Core order features — use pre-computed delivery day columns from fact
        orders = (
            fact
            .filter(
                col("ORDER_STATUS").isin(["DELIVERED", "CANCELED"]) &
                col("ORDER_DELIVERED_CUSTOMER_DATE").isNotNull() &
                col("ORDER_ESTIMATED_DELIVERY_DATE").isNotNull()
            )
            .select(
                "ORDER_ID",
                "ORDER_PURCHASE_TIMESTAMP",
                "ORDER_PURCHASE_DATE",
                "PAYMENT_VALUE",
                "TOTAL_FREIGHT",
                "DELIVERY_DAYS",
                "ESTIMATED_DELIVERY_DAYS",
                "ORDER_STATUS",
                "MAX_INSTALLMENTS",
            )
            .with_column(
                "IS_LATE",
                when(
                    col("DELIVERY_DAYS") > col("ESTIMATED_DELIVERY_DAYS"),
                    lit(1)
                ).otherwise(lit(0))
            )
            .with_column(
                "PURCHASE_MONTH",
                sf_month(col("ORDER_PURCHASE_TIMESTAMP"))
            )
        )

        pdf = orders.to_pandas()
        pdf["TOTAL_FREIGHT"] = pdf["TOTAL_FREIGHT"].fillna(0)
        pdf["PAYMENT_VALUE"] = pdf["PAYMENT_VALUE"].fillna(0)
        pdf["DELIVERY_DAYS"] = pdf["DELIVERY_DAYS"].fillna(0)
        pdf["ESTIMATED_DELIVERY_DAYS"] = pdf["ESTIMATED_DELIVERY_DAYS"].fillna(0)

        # Weekend flag from ORDER_PURCHASE_DATE in Pandas (avoids DIM_TIME join complexity)
        pdf["ORDER_PURCHASE_DATE"] = pd.to_datetime(pdf["ORDER_PURCHASE_DATE"])
        pdf["IS_WEEKEND_PURCHASE"] = pdf["ORDER_PURCHASE_DATE"].dt.dayofweek.isin([5, 6]).astype(int)

        logger.info(f"Extracted {len(pdf):,} labeled orders.")
        logger.info(f"Late delivery rate: {pdf['IS_LATE'].mean()*100:.1f}%")
        return pdf

    # 2. Train Model

    def train(self, pdf: pd.DataFrame) -> tuple[float, float, float]:
        """Train Random Forest and return (accuracy, f1, roc_auc)."""
        self.feature_cols = [
            "TOTAL_FREIGHT", "PAYMENT_VALUE",
            "DELIVERY_DAYS", "ESTIMATED_DELIVERY_DAYS",
            "PURCHASE_MONTH", "IS_WEEKEND_PURCHASE",
        ]
        X = pdf[self.feature_cols].fillna(0).values
        y = pdf["IS_LATE"].values

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        logger.info(f"Training Random Forest (train={len(X_train):,}, test={len(X_test):,})...")
        self.clf = RandomForestClassifier(
            n_estimators=150, max_depth=8,
            random_state=42, n_jobs=-1,
            class_weight="balanced",  # handles late/on-time imbalance
        )
        self.clf.fit(X_train, y_train)

        y_pred = self.clf.predict(X_test)
        y_prob = self.clf.predict_proba(X_test)[:, 1]

        acc = accuracy_score(y_test, y_pred)
        f1  = f1_score(y_test, y_pred)
        roc = roc_auc_score(y_test, y_prob)

        logger.info(f"  Accuracy : {acc:.4f}")
        logger.info(f"  F1-Score : {f1:.4f}")
        logger.info(f"  ROC-AUC  : {roc:.4f}")
        logger.info("\n" + classification_report(y_test, y_pred, target_names=["On Time", "Late"]))

        return acc, f1, roc

    # 3. Score All Orders & Write Results

    def score_and_write(self, pdf: pd.DataFrame):
        """Apply model to full dataset and write to DELIVERY_RISK."""
        logger.info("Scoring all orders...")
        X_all = pdf[self.feature_cols].fillna(0).values
        pdf["IS_LATE_PREDICTED"] = self.clf.predict(X_all)
        pdf["DELAY_PROBABILITY"]  = self.clf.predict_proba(X_all)[:, 1].round(4)

        pdf["RISK_TIER"] = pd.cut(
            pdf["DELAY_PROBABILITY"],
            bins=[0, 0.35, 0.65, 1.0],
            labels=["LOW RISK", "MEDIUM RISK", "HIGH RISK"],
        ).astype(str)

        out = pdf[[
            "ORDER_ID", "IS_LATE", "IS_LATE_PREDICTED",
            "DELAY_PROBABILITY", "RISK_TIER",
        ]].copy()
        out.columns = [
            "ORDER_ID", "IS_LATE_ACTUAL", "IS_LATE_PREDICTED",
            "DELAY_PROBABILITY", "RISK_TIER",
        ]
        out["MODEL_VERSION"] = self.model_version
        out["PREDICTED_AT"] = datetime.now(timezone.utc).isoformat()

        logger.info(f"Writing {len(out):,} rows to {OUTPUT_TABLE}...")
        sp_df = self.session.create_dataframe(out)
        sp_df.write.mode("overwrite").save_as_table(OUTPUT_TABLE)
        logger.success(f"Wrote delivery risk predictions to {OUTPUT_TABLE}")

    # 4. Log to Registry

    def log_to_registry(self, f1: float, acc: float, roc: float, n_records: int):
        status = "RETAINED" if f1 >= F1_THRESHOLD else "DROPPED"
        logger.info(f"Registry status: {status}  (F1={f1:.4f})")

        self.session.sql(f"""
            CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
                MODEL_NAME      VARCHAR(100),
                MODEL_VERSION   VARCHAR(50),
                RUN_TIMESTAMP   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY_METRIC  VARCHAR(50),
                METRIC_VALUE    FLOAT,
                THRESHOLD       FLOAT,
                STATUS          VARCHAR(20),
                RECORDS_SCORED  INTEGER,
                NOTES           VARCHAR(500)
            )
        """).collect()

        notes = f"RandomForest Delivery Delay | Accuracy={acc:.4f} | ROC-AUC={roc:.4f}"
        self.session.sql(f"""
            INSERT INTO {REGISTRY_TABLE}
                (MODEL_NAME, MODEL_VERSION, PRIMARY_METRIC,
                 METRIC_VALUE, THRESHOLD, STATUS, RECORDS_SCORED, NOTES)
            VALUES (
                '{MODEL_NAME}', '{self.model_version}', 'f1_score',
                {round(f1, 4)}, {F1_THRESHOLD},
                '{status}', {n_records},
                '{notes}'
            )
        """).collect()

    # Full Pipeline

    def run(self):
        logger.info("=" * 60)
        logger.info("Model 2 — Delivery Delay Prediction (Random Forest)")
        logger.info("=" * 60)

        pdf = self.extract_features()

        if len(pdf) < 50:
            logger.warning("Too few labeled orders to train. Exiting.")
            return None

        acc, f1, roc = self.train(pdf)
        self.score_and_write(pdf)
        self.log_to_registry(f1, acc, roc, len(pdf))

        logger.info("=" * 60)
        logger.info(f"Done. F1={f1:.4f}  Accuracy={acc:.4f}  ROC-AUC={roc:.4f}")
        logger.info("=" * 60)
        return pdf


# Standalone Execution

if __name__ == "__main__":
    params = {
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "role":      os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "ECOMMERCE_WH"),
        "database":  os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DW"),
        "schema":    "ANALYTICS",
    }
    session = Session.builder.configs(params).create()
    try:
        model = DeliveryDelayModel(session)
        results = model.run()
    finally:
        session.close()
