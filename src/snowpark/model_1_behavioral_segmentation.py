"""
Model 1 — Customer Behavioral Segmentation (K-Means)
=====================================================
Segments all customers by HOW they buy rather than HOW OFTEN they buy.
Solves the core limitation of RFM on the Olist dataset (~97% single-purchase
customers), where Frequency and Lifespan are almost always 1 and 0.

Features Used:
  - PAYMENT_VALUE      (ticket size / monetary weight)
  - MAX_INSTALLMENTS   (credit reliance proxy)
  - FREIGHT_RATIO      (price sensitivity: freight / total)
  - AVG_REVIEW_SCORE   (satisfaction level)
  - DELIVERY_DELTA     (actual - estimated delivery days)

Output Table: ANALYTICS.CUSTOMER_SEGMENTS
Evaluation:   Silhouette Score (threshold ≥ 0.30 to retain)
"""

import os
import json
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from loguru import logger
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit, avg, sum as sf_sum, count,
    datediff, when, coalesce, max as sf_max,
)


# Constants
GOLD_SCHEMA   = "ANALYTICS"
OUTPUT_TABLE  = f"{GOLD_SCHEMA}.CUSTOMER_SEGMENTS"
REGISTRY_TABLE = f"{GOLD_SCHEMA}.ML_MODEL_REGISTRY"
SILHOUETTE_THRESHOLD = 0.30
MODEL_NAME = "customer_behavioral_segmentation"


# Model Class

class BehavioralSegmentationModel:
    """K-Means customer segmentation using behavioral (not frequency) features."""

    def __init__(self, session: Session):
        self.session = session
        self.model_version = f"v{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    # 1. Feature Extraction

    def extract_features(self) -> pd.DataFrame:
        """Pull behavioral features from FACT_ORDERS."""
        logger.info("Extracting behavioral features from ANALYTICS.FACT_ORDERS...")

        fact = self.session.table(f"{GOLD_SCHEMA}.FACT_ORDERS")

        agg = (
            fact
            .filter(col("ORDER_STATUS") == "DELIVERED")
            .group_by("CUSTOMER_KEY")
            .agg(
                sf_sum("TOTAL_VALUE").alias("TOTAL_PAYMENT_VALUE"),
                sf_sum("TOTAL_FREIGHT").alias("TOTAL_FREIGHT"),
                avg("AVG_REVIEW_SCORE").alias("AVG_REVIEW_SCORE"),
                avg("MAX_INSTALLMENTS").alias("AVG_INSTALLMENTS"),
                # delivery_days = actual, estimated_delivery_days = estimated (both pre-computed in fact)
                avg(
                    when(
                        col("DELIVERY_DAYS").isNotNull() & col("ESTIMATED_DELIVERY_DAYS").isNotNull(),
                        col("DELIVERY_DAYS") - col("ESTIMATED_DELIVERY_DAYS")
                    ).otherwise(lit(0))
                ).alias("AVG_DELIVERY_DELTA"),
                count("ORDER_ID").alias("ORDER_COUNT"),
                avg("PAYMENT_VALUE").alias("AVG_ORDER_VALUE"),
            )
        )

        pdf = agg.to_pandas()

        # Derived feature: freight ratio
        pdf["FREIGHT_RATIO"] = (
            pdf["TOTAL_FREIGHT"] / pdf["TOTAL_PAYMENT_VALUE"].replace(0, 1)
        ).clip(0, 1).round(4)

        # Fill any remaining nulls with neutral values
        pdf["AVG_REVIEW_SCORE"] = pdf["AVG_REVIEW_SCORE"].fillna(3.0)
        pdf["AVG_INSTALLMENTS"] = pdf["AVG_INSTALLMENTS"].fillna(1.0)
        pdf["AVG_DELIVERY_DELTA"] = pdf["AVG_DELIVERY_DELTA"].fillna(0.0)

        logger.info(f"Extracted features for {len(pdf):,} customers.")
        return pdf

    # 2. Auto-Tune K

    def find_optimal_k(self, X_scaled: np.ndarray, k_range=range(2, 9)) -> int:
        """Find K with best silhouette score."""
        logger.info("Auto-tuning K...")
        best_k, best_sil = 3, -1
        for k in k_range:
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = km.fit_predict(X_scaled)
            sil = silhouette_score(X_scaled, labels, sample_size=min(5000, len(X_scaled)))
            logger.info(f"  K={k}  Silhouette={sil:.4f}")
            if sil > best_sil:
                best_sil, best_k = sil, k
        logger.info(f"Optimal K={best_k}  (Silhouette={best_sil:.4f})")
        return best_k

    # 3. Cluster & Label

    def _derive_segment_label(self, centroid: dict) -> str:
        """
        Assign a business-friendly label based on cluster centroid values.
        Dimensions used: avg_order_value, avg_installments, avg_review_score,
                         avg_delivery_delta, freight_ratio.
        """
        score      = centroid.get("AVG_REVIEW_SCORE", 3.0)
        install    = centroid.get("AVG_INSTALLMENTS", 1.0)
        aov        = centroid.get("AVG_ORDER_VALUE", 0)
        delta      = centroid.get("AVG_DELIVERY_DELTA", 0)
        freight    = centroid.get("FREIGHT_RATIO", 0)

        if score >= 4.0 and install <= 2:
            return "Premium Cash Buyer"
        elif score >= 4.0 and install > 3:
            return "Satisfied Credit Shopper"
        elif score < 2.5 or delta > 10:
            return "Detractor / Poor Experience"
        elif aov > 300 and install > 4:
            return "High-Value Installment Buyer"
        elif freight > 0.25:
            return "Remote / High-Freight Buyer"
        elif score >= 3.5 and aov < 100:
            return "Convenience Budget Buyer"
        else:
            return "Average Buyer"

    def cluster(self, pdf: pd.DataFrame) -> tuple[pd.DataFrame, float]:
        """Run K-Means and assign segment labels."""
        feature_cols = [
            "AVG_ORDER_VALUE", "AVG_INSTALLMENTS",
            "FREIGHT_RATIO", "AVG_REVIEW_SCORE", "AVG_DELIVERY_DELTA",
        ]

        X = pdf[feature_cols].fillna(0).values
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        k = self.find_optimal_k(X_scaled)
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        pdf["CLUSTER_ID"] = km.fit_predict(X_scaled)

        sil = silhouette_score(X_scaled, pdf["CLUSTER_ID"].values,
                               sample_size=min(5000, len(pdf)))
        logger.info(f"Final Silhouette Score: {sil:.4f}")

        # Build centroid dict per cluster for labelling
        centroid_df = pdf.groupby("CLUSTER_ID")[feature_cols].mean()
        label_map = {
            cid: self._derive_segment_label(row.to_dict())
            for cid, row in centroid_df.iterrows()
        }
        pdf["SEGMENT_LABEL"] = pdf["CLUSTER_ID"].map(label_map)

        # Simple predicted CLV: AOV × order count (single-purchase proxy)
        pdf["PREDICTED_CLV"] = (
            pdf["AVG_ORDER_VALUE"] * pdf["ORDER_COUNT"]
        ).round(2)

        return pdf, sil

    # 4. Write Results

    def write_results(self, pdf: pd.DataFrame):
        """Overwrite ANALYTICS.CUSTOMER_SEGMENTS."""
        logger.info(f"Writing {len(pdf):,} rows to {OUTPUT_TABLE}...")
        out = pdf[[
            "CUSTOMER_KEY", "CLUSTER_ID", "SEGMENT_LABEL",
            "AVG_ORDER_VALUE", "AVG_INSTALLMENTS",
            "FREIGHT_RATIO", "AVG_REVIEW_SCORE",
            "AVG_DELIVERY_DELTA", "ORDER_COUNT", "PREDICTED_CLV",
        ]].copy()
        out["MODEL_VERSION"] = self.model_version
        out["PREDICTED_AT"] = datetime.now(timezone.utc).isoformat()

        sp_df = self.session.create_dataframe(out)
        sp_df.write.mode("overwrite").save_as_table(OUTPUT_TABLE)
        logger.success(f"Wrote results to {OUTPUT_TABLE}")

    # 5. Log to Registry

    def log_to_registry(self, sil: float, n_records: int):
        """Write performance record to ML_MODEL_REGISTRY."""
        status = "RETAINED" if sil >= SILHOUETTE_THRESHOLD else "DROPPED"
        logger.info(f"Registry status: {status}  (Silhouette={sil:.4f})")

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

        self.session.sql(f"""
            INSERT INTO {REGISTRY_TABLE}
                (MODEL_NAME, MODEL_VERSION, PRIMARY_METRIC,
                 METRIC_VALUE, THRESHOLD, STATUS, RECORDS_SCORED, NOTES)
            VALUES (
                '{MODEL_NAME}', '{self.model_version}', 'silhouette_score',
                {round(sil, 4)}, {SILHOUETTE_THRESHOLD},
                '{status}', {n_records},
                'Behavioral K-Means: ticket size, installments, freight, satisfaction, delivery delta'
            )
        """).collect()

    # Full Pipeline

    def run(self):
        logger.info("=" * 60)
        logger.info("Model 1 — Customer Behavioral Segmentation")
        logger.info("=" * 60)

        pdf = self.extract_features()

        if len(pdf) < 10:
            logger.warning("Too few customers to cluster. Exiting.")
            return None

        pdf, sil = self.cluster(pdf)
        self.write_results(pdf)
        self.log_to_registry(sil, len(pdf))

        logger.info("=" * 60)
        logger.info(f"Done. Silhouette={sil:.4f}  |  {len(pdf):,} customers segmented.")
        logger.info(f"Segment distribution:\n{pdf['SEGMENT_LABEL'].value_counts().to_string()}")
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
        model = BehavioralSegmentationModel(session)
        results = model.run()
    finally:
        session.close()
