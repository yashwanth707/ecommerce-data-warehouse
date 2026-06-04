"""
Model 3 — Seller Risk & Performance Clustering (K-Means)
=========================================================
Segments the 3,095 Olist sellers into performance tiers to identify
who is driving marketplace growth vs. who is harming it.

Features Used:
  - TOTAL_REVENUE       (seller's total GMV)
  - AVG_REVIEW_SCORE    (customer satisfaction for this seller)
  - AVG_DELIVERY_DELAY  (mean actual - estimated delivery days)
  - ORDER_COUNT         (seller volume)
  - CANCELLATION_RATE   (cancelled / total orders)

Output Table: ANALYTICS.SELLER_SEGMENTS
Evaluation:   Silhouette Score (threshold ≥ 0.35 to retain)
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
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from loguru import logger
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit, avg, sum as sf_sum, count, when,
    datediff, coalesce,
)

# Constants
GOLD_SCHEMA   = "ANALYTICS"
OUTPUT_TABLE  = f"{GOLD_SCHEMA}.SELLER_SEGMENTS"
REGISTRY_TABLE = f"{GOLD_SCHEMA}.ML_MODEL_REGISTRY"
SILHOUETTE_THRESHOLD = 0.35
MODEL_NAME = "seller_risk_clustering"


# Model Class

class SellerRiskClusteringModel:
    """K-Means seller segmentation by revenue, quality, speed and reliability."""

    def __init__(self, session: Session):
        self.session = session
        self.model_version = f"v{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    # 1. Feature Extraction

    def extract_features(self) -> pd.DataFrame:
        """Aggregate order-level data to seller-level features.
        
        Strategy: DIM_SELLERS already has total_revenue, total_orders, avg_item_price.
        We enrich with avg_review_score + avg_delivery_delay from FACT_ORDERS via
        a raw SQL aggregation grouped by seller_id (via FACT_ORDER_ITEMS join).
        """
        logger.info("Extracting seller-level features...")

        # DIM_SELLERS has pre-computed seller metrics
        dim_sellers = self.session.table(f"{GOLD_SCHEMA}.DIM_SELLERS")
        seller_base = dim_sellers.select(
            "SELLER_ID",
            "SELLER_CITY",
            "SELLER_STATE",
            "TOTAL_REVENUE",
            "TOTAL_ORDERS",
            "TOTAL_ITEMS_SOLD",
            "AVG_ITEM_PRICE",
        )

        # Aggregate quality metrics per seller from FACT_ORDER_ITEMS + FACT_ORDERS
        quality_sql = """
            SELECT
                oi.seller_id,
                AVG(fo.avg_review_score)                            AS avg_review_score,
                AVG(
                    CASE
                        WHEN fo.delivery_days IS NOT NULL
                          AND fo.estimated_delivery_days IS NOT NULL
                        THEN fo.delivery_days - fo.estimated_delivery_days
                        ELSE 0
                    END
                )                                                   AS avg_delivery_delay,
                SUM(CASE WHEN fo.order_status IN ('CANCELED','UNAVAILABLE')
                         THEN 1.0 ELSE 0.0 END)
                    / NULLIF(COUNT(fo.order_id), 0) * 100           AS cancellation_rate
            FROM ANALYTICS.FACT_ORDER_ITEMS oi
            INNER JOIN ANALYTICS.FACT_ORDERS fo ON oi.order_id = fo.order_id
            GROUP BY oi.seller_id
        """
        quality_df = self.session.sql(quality_sql)

        # Join using seller_id (string key, no surrogate needed)
        joined = seller_base.join(
            quality_df,
            on="SELLER_ID",
            how="left",
        )

        pdf = joined.to_pandas()
        pdf["AVG_REVIEW_SCORE"] = pdf["AVG_REVIEW_SCORE"].fillna(3.0)
        pdf["AVG_DELIVERY_DELAY"] = pdf["AVG_DELIVERY_DELAY"].fillna(0.0)
        pdf["CANCELLATION_RATE"] = pdf["CANCELLATION_RATE"].fillna(0.0)

        logger.info(f"Extracted features for {len(pdf):,} sellers.")
        return pdf

    # 2. Auto-Tune K

    def find_optimal_k(self, X_scaled: np.ndarray, k_range=range(2, 7)) -> int:
        """Smaller k_range since sellers are fewer than customers."""
        logger.info("Auto-tuning K for seller clustering...")
        best_k, best_sil = 3, -1
        for k in k_range:
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = km.fit_predict(X_scaled)
            sil = silhouette_score(X_scaled, labels)
            logger.info(f"  K={k}  Silhouette={sil:.4f}")
            if sil > best_sil:
                best_sil, best_k = sil, k
        logger.info(f"Optimal K={best_k}  (Silhouette={best_sil:.4f})")
        return best_k

    # 3. Cluster & Label

    def _derive_segment_label(self, centroid: dict) -> str:
        """Assign risk/performance label from centroid."""
        revenue = centroid.get("TOTAL_REVENUE", 0)
        score   = centroid.get("AVG_REVIEW_SCORE", 3.0)
        delay   = centroid.get("AVG_DELIVERY_DELAY", 0)
        volume  = centroid.get("TOTAL_ORDERS", 0)
        cancel  = centroid.get("CANCELLATION_RATE", 0)

        if score >= 4.0 and delay <= 2 and cancel < 3:
            return "Top-Tier Reliable Seller"
        elif volume > 100 and delay > 7:
            return "High-Volume / Chronically Delayed"
        elif score < 2.5 or cancel > 10:
            return "High-Risk / Poor Quality"
        elif revenue > 50000 and score >= 3.5:
            return "Established High-Revenue Seller"
        elif volume < 10:
            return "Low-Activity Seller"
        else:
            return "Average Seller"

    def cluster(self, pdf: pd.DataFrame) -> tuple[pd.DataFrame, float]:
        """Run K-Means clustering on seller features."""
        feature_cols = [
            "TOTAL_REVENUE", "AVG_REVIEW_SCORE",
            "AVG_DELIVERY_DELAY", "TOTAL_ORDERS", "CANCELLATION_RATE",
        ]
        X = pdf[feature_cols].fillna(0).values
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        k = self.find_optimal_k(X_scaled)
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        pdf["CLUSTER_ID"] = km.fit_predict(X_scaled)

        sil = silhouette_score(X_scaled, pdf["CLUSTER_ID"].values)
        logger.info(f"Final Silhouette Score: {sil:.4f}")

        centroid_df = pdf.groupby("CLUSTER_ID")[feature_cols].mean()
        label_map = {
            cid: self._derive_segment_label(row.to_dict())
            for cid, row in centroid_df.iterrows()
        }
        pdf["SEGMENT_LABEL"] = pdf["CLUSTER_ID"].map(label_map)

        # Composite risk score (lower = safer / better)
        pdf["RISK_SCORE"] = (
            (5 - pdf["AVG_REVIEW_SCORE"].clip(1, 5)) * 0.4
            + pdf["AVG_DELIVERY_DELAY"].clip(0, 30) / 30 * 0.35
            + pdf["CANCELLATION_RATE"].clip(0, 100) / 100 * 0.25
        ).round(4)

        return pdf, sil

    # 4. Write Results

    def write_results(self, pdf: pd.DataFrame):
        logger.info(f"Writing {len(pdf):,} rows to {OUTPUT_TABLE}...")
        out = pdf[[
            "SELLER_ID", "CLUSTER_ID", "SEGMENT_LABEL",
            "TOTAL_REVENUE", "TOTAL_ORDERS",
            "AVG_REVIEW_SCORE", "AVG_DELIVERY_DELAY",
            "CANCELLATION_RATE", "RISK_SCORE",
        ]].copy()
        out["MODEL_VERSION"] = self.model_version
        out["PREDICTED_AT"] = datetime.now(timezone.utc).isoformat()

        sp_df = self.session.create_dataframe(out)
        sp_df.write.mode("overwrite").save_as_table(OUTPUT_TABLE)
        logger.success(f"Wrote results to {OUTPUT_TABLE}")

    # 5. Log to Registry

    def log_to_registry(self, sil: float, n_records: int):
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
                'Seller K-Means: revenue, review score, delivery delay, volume, cancellation rate'
            )
        """).collect()

    # Full Pipeline

    def run(self):
        logger.info("=" * 60)
        logger.info("Model 3 — Seller Risk & Performance Clustering")
        logger.info("=" * 60)

        pdf = self.extract_features()

        if len(pdf) < 5:
            logger.warning("Too few sellers to cluster. Exiting.")
            return None

        pdf, sil = self.cluster(pdf)
        self.write_results(pdf)
        self.log_to_registry(sil, len(pdf))

        logger.info("=" * 60)
        logger.info(f"Done. Silhouette={sil:.4f}  |  {len(pdf):,} sellers segmented.")
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
        model = SellerRiskClusteringModel(session)
        results = model.run()
    finally:
        session.close()
