"""
Model 4 — Review Score Prediction (Random Forest Classifier)
============================================================
Predicts what review score category a customer would have given
(POSITIVE / NEUTRAL / NEGATIVE) based on their order experience.

Solves a real data quality problem: ~40K orders in the Olist dataset
never received a review. This model fills in the blanks using the
actual delivery experience as a proxy for satisfaction.

Features Used:
  - DELIVERY_DELTA       (actual - estimated delivery days)
  - FREIGHT_RATIO        (freight / payment value)
  - MAX_INSTALLMENTS     (credit reliance / order stress)
  - PAYMENT_VALUE        (order size)

Target:
  REVIEW_CATEGORY = POSITIVE (4-5 stars), NEUTRAL (3), NEGATIVE (1-2)

Output Table: ANALYTICS.PREDICTED_SATISFACTION
Evaluation:   Macro F1-Score (threshold ≥ 0.60 to retain)
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
from sklearn.metrics import f1_score, accuracy_score, classification_report
from loguru import logger
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit, when, datediff, coalesce,
)

# Constants
GOLD_SCHEMA    = "ANALYTICS"
OUTPUT_TABLE   = f"{GOLD_SCHEMA}.PREDICTED_SATISFACTION"
REGISTRY_TABLE = f"{GOLD_SCHEMA}.ML_MODEL_REGISTRY"
F1_THRESHOLD   = 0.60
MODEL_NAME     = "review_score_prediction"


# Model Class

class SatisfactionPredictionModel:
    """
    3-class Random Forest classifier:
    POSITIVE / NEUTRAL / NEGATIVE based on order experience features.
    """

    def __init__(self, session: Session):
        self.session = session
        self.model_version = f"v{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        self.clf = None
        self.feature_cols = None

    # 1. Extract Features

    def extract_features(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Returns two DataFrames:
          - labeled_pdf: rows with known review scores (training set)
          - all_pdf: all delivered orders (for scoring)
        """
        logger.info("Extracting order experience features from FACT_ORDERS...")

        fact = self.session.table(f"{GOLD_SCHEMA}.FACT_ORDERS")

        base = (
            fact
            .filter(col("ORDER_STATUS") == "DELIVERED")
            .select(
                "ORDER_ID",
                "PAYMENT_VALUE",
                "TOTAL_FREIGHT",
                "MAX_INSTALLMENTS",
                "AVG_REVIEW_SCORE",
                "DELIVERY_DAYS",
                "ESTIMATED_DELIVERY_DAYS",
            )
            .with_column(
                "DELIVERY_DELTA",
                when(
                    col("DELIVERY_DAYS").isNotNull() & col("ESTIMATED_DELIVERY_DAYS").isNotNull(),
                    col("DELIVERY_DAYS") - col("ESTIMATED_DELIVERY_DAYS")
                ).otherwise(lit(0))
            )
        )

        all_pdf = base.to_pandas()

        # Derived: freight ratio using TOTAL_FREIGHT
        all_pdf["FREIGHT_RATIO"] = (
            all_pdf["TOTAL_FREIGHT"] /
            all_pdf["PAYMENT_VALUE"].replace(0, 1)
        ).clip(0, 1).round(4)

        all_pdf["MAX_INSTALLMENTS"] = all_pdf["MAX_INSTALLMENTS"].fillna(1)
        all_pdf["DELIVERY_DELTA"]   = all_pdf["DELIVERY_DELTA"].fillna(0)
        all_pdf["FREIGHT_RATIO"]    = all_pdf["FREIGHT_RATIO"].fillna(0)

        # Only rows with a known review score can be used for training
        labeled_pdf = all_pdf[all_pdf["AVG_REVIEW_SCORE"].notna()].copy()

        # Map review score to category
        def map_category(score):
            if score >= 4:
                return "POSITIVE"
            elif score == 3:
                return "NEUTRAL"
            else:
                return "NEGATIVE"

        labeled_pdf["REVIEW_CATEGORY"] = labeled_pdf["AVG_REVIEW_SCORE"].apply(map_category)

        logger.info(f"All delivered orders: {len(all_pdf):,}")
        logger.info(f"Labeled (with review): {len(labeled_pdf):,}")
        logger.info(f"Class distribution:\n{labeled_pdf['REVIEW_CATEGORY'].value_counts().to_string()}")
        return labeled_pdf, all_pdf

    # 2. Train

    def train(self, labeled_pdf: pd.DataFrame) -> tuple[float, float]:
        """Train 3-class Random Forest. Returns (accuracy, macro_f1)."""
        self.feature_cols = [
            "DELIVERY_DELTA", "FREIGHT_RATIO",
            "MAX_INSTALLMENTS", "PAYMENT_VALUE",
        ]
        X = labeled_pdf[self.feature_cols].fillna(0).values
        y = labeled_pdf["REVIEW_CATEGORY"].values

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        logger.info(f"Training Random Forest (train={len(X_train):,}, test={len(X_test):,})...")
        self.clf = RandomForestClassifier(
            n_estimators=200, max_depth=10,
            random_state=42, n_jobs=-1,
            class_weight="balanced",
        )
        self.clf.fit(X_train, y_train)

        y_pred = self.clf.predict(X_test)
        acc    = accuracy_score(y_test, y_pred)
        f1     = f1_score(y_test, y_pred, average="macro")

        logger.info(f"  Accuracy    : {acc:.4f}")
        logger.info(f"  F1 (macro)  : {f1:.4f}")
        logger.info(
            "\n" + classification_report(y_test, y_pred,
                                         target_names=["NEGATIVE", "NEUTRAL", "POSITIVE"])
        )
        return acc, f1

    # 3. Score All & Write

    def score_and_write(self, all_pdf: pd.DataFrame):
        """Apply model to all delivered orders and write to output table."""
        logger.info("Scoring all delivered orders...")
        X_all = all_pdf[self.feature_cols].fillna(0).values

        all_pdf["PREDICTED_CATEGORY"] = self.clf.predict(X_all)

        # Confidence = probability of predicted class
        proba = self.clf.predict_proba(X_all)
        all_pdf["CONFIDENCE"] = proba.max(axis=1).round(4)

        out = all_pdf[[
            "ORDER_ID",
            "AVG_REVIEW_SCORE",
            "PREDICTED_CATEGORY",
            "CONFIDENCE",
        ]].copy()
        out.columns = [
            "ORDER_ID", "ACTUAL_REVIEW_SCORE",
            "PREDICTED_CATEGORY", "CONFIDENCE",
        ]
        out["MODEL_VERSION"] = self.model_version
        out["PREDICTED_AT"] = datetime.now(timezone.utc).isoformat()

        logger.info(f"Writing {len(out):,} rows to {OUTPUT_TABLE}...")
        sp_df = self.session.create_dataframe(out)
        sp_df.write.mode("overwrite").save_as_table(OUTPUT_TABLE)
        logger.success(f"Wrote satisfaction predictions to {OUTPUT_TABLE}")

    # 4. Log to Registry

    def log_to_registry(self, f1: float, acc: float, n_records: int):
        status = "RETAINED" if f1 >= F1_THRESHOLD else "DROPPED"
        logger.info(f"Registry status: {status}  (F1 macro={f1:.4f})")

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

        notes = f"RandomForest 3-class | Accuracy={acc:.4f} | Features: delivery_delta, freight_ratio, installments, payment_value"
        self.session.sql(f"""
            INSERT INTO {REGISTRY_TABLE}
                (MODEL_NAME, MODEL_VERSION, PRIMARY_METRIC,
                 METRIC_VALUE, THRESHOLD, STATUS, RECORDS_SCORED, NOTES)
            VALUES (
                '{MODEL_NAME}', '{self.model_version}', 'f1_macro',
                {round(f1, 4)}, {F1_THRESHOLD},
                '{status}', {n_records},
                '{notes}'
            )
        """).collect()

    # Full Pipeline

    def run(self):
        logger.info("=" * 60)
        logger.info("Model 4 — Review Score Prediction (Random Forest)")
        logger.info("=" * 60)

        labeled_pdf, all_pdf = self.extract_features()

        if len(labeled_pdf) < 50:
            logger.warning("Too few labeled reviews to train. Exiting.")
            return None

        acc, f1 = self.train(labeled_pdf)
        self.score_and_write(all_pdf)
        self.log_to_registry(f1, acc, len(all_pdf))

        logger.info("=" * 60)
        logger.info(f"Done. F1 (macro)={f1:.4f}  Accuracy={acc:.4f}")
        logger.info("=" * 60)
        return all_pdf


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
        model = SatisfactionPredictionModel(session)
        results = model.run()
    finally:
        session.close()
