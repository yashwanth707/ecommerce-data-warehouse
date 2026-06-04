"""
Snowpark-based Customer Lifetime Value (CLV) prediction.
Calculates RFM metrics, engineers features, and runs K-Means clustering.
"""

import json
from datetime import datetime, timezone
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Running inside Docker/Airflow where env vars are already set

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit, avg, sum as sf_sum, count, countDistinct,
    datediff, current_timestamp, max as sf_max, min as sf_min,
    when, round as sf_round, month,
)
from snowflake.snowpark.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, TimestampType,
)

import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from loguru import logger


class CLVModel:
    """
    Predicts Customer Lifetime Value and groups users into segments.
    Results are written back to Snowflake (CUSTOMER_CLV).
    """

    def __init__(self, session: Session, gold_schema: str = "ANALYTICS"):
        self.session = session
        self.gold_schema = gold_schema
        self.model_version: Optional[str] = None
        self.k_optimal: Optional[int] = None

    # 1. RFM Feature Extraction

    def extract_rfm_features(self):
        """
        Calculate Recency, Frequency, and Monetary values
        for each customer from the Gold layer.
        """
        logger.info("Loading orders and customers data...")

        fact_orders = self.session.table("ANALYTICS.fact_orders")
        dim_customers = self.session.table("ANALYTICS.dim_customers")

        rfm = (
            fact_orders
            .filter(col("ORDER_STATUS") == "DELIVERED")
            .group_by("CUSTOMER_KEY")
            .agg(
                sf_max("ORDER_PURCHASE_TIMESTAMP").alias("LAST_PURCHASE"),
                count("ORDER_ID").alias("FREQUENCY"),
                sf_sum("PAYMENT_VALUE").alias("MONETARY"),
                avg("PAYMENT_VALUE").alias("AVG_ORDER_VALUE"),
                sf_min("ORDER_PURCHASE_TIMESTAMP").alias("FIRST_PURCHASE"),
                countDistinct("ORDER_DATE_KEY").alias("DISTINCT_PURCHASE_DAYS"),
            )
        )

        # Calculate recency as days since last purchase
        rfm = rfm.with_column(
            "RECENCY",
            datediff("day", col("LAST_PURCHASE"), current_timestamp()),
        )

        # Customer lifespan in days
        rfm = rfm.with_column(
            "LIFESPAN_DAYS",
            datediff("day", col("FIRST_PURCHASE"), col("LAST_PURCHASE")),
        )

        # Join with customer dimension for location
        rfm = rfm.join(
            dim_customers.select(
                "CUSTOMER_KEY", "CUSTOMER_STATE", "CUSTOMER_CITY"
            ),
            on="CUSTOMER_KEY",
            how="inner",
        )

        logger.info(f"RFM features extracted for {rfm.count()} customers")
        return rfm

    # 2. Feature Engineering

    def engineer_features(self, rfm_df):
        """
        Add advanced features:
        - Seasonality (peak month purchases)
        - Purchase regularity
        """
        logger.info("Engineering additional features...")

        fact_orders = self.session.table("ANALYTICS.fact_orders")

        # Monthly purchase distribution per customer
        monthly = (
            fact_orders
            .filter(col("ORDER_STATUS") == "DELIVERED")
            .with_column("PURCHASE_MONTH", month("ORDER_PURCHASE_TIMESTAMP"))
            .group_by("CUSTOMER_KEY", "PURCHASE_MONTH")
            .agg(count("ORDER_ID").alias("MONTH_ORDERS"))
        )

        # Peak month (mode of purchase months)
        peak_month = (
            monthly
            .group_by("CUSTOMER_KEY")
            .agg(sf_max("MONTH_ORDERS").alias("PEAK_MONTH_ORDERS"))
        )

        # Join features
        enriched = rfm_df.join(
            peak_month,
            on="CUSTOMER_KEY",
            how="left",
        )

        # Purchase regularity = distinct purchase days / lifespan
        enriched = enriched.with_column(
            "PURCHASE_REGULARITY",
            when(
                col("LIFESPAN_DAYS") > 0,
                sf_round(
                    col("DISTINCT_PURCHASE_DAYS") / col("LIFESPAN_DAYS") * 100,
                    2,
                ),
            ).otherwise(lit(0)),
        )

        return enriched

    # 3. Auto-Tune K (Elbow + Silhouette)

    def find_optimal_k(
        self, features: np.ndarray, k_range: range = range(2, 11)
    ) -> int:
        """
        Find optimal K for K-Means using Elbow method + Silhouette score.

        Args:
            features: Scaled feature matrix
            k_range: Range of K values to evaluate

        Returns:
            Optimal K value
        """
        logger.info("Finding optimal K...")

        inertias = []
        silhouettes = []

        for k in k_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(features)
            inertias.append(kmeans.inertia_)
            sil = silhouette_score(features, labels, sample_size=min(5000, len(features)))
            silhouettes.append(sil)
            logger.info(
                f"  K={k}: Inertia={kmeans.inertia_:.2f}, "
                f"Silhouette={sil:.4f}"
            )

        # Pick K with best silhouette score
        best_idx = np.argmax(silhouettes)
        self.k_optimal = list(k_range)[best_idx]
        logger.info(
            f"Optimal K={self.k_optimal} "
            f"(Silhouette={silhouettes[best_idx]:.4f})"
        )
        return self.k_optimal

    # 4. Segmentation & CLV Prediction

    def predict_clv(self, enriched_df, k: Optional[int] = None):
        """
        Segment customers via K-Means and predict CLV.

        CLV = Avg Order Value × Purchase Frequency × Predicted Lifespan

        Args:
            enriched_df: Snowpark DataFrame with RFM + engineered features
            k: Number of clusters. If None, auto-tune.

        Returns:
            Snowpark DataFrame with CLV predictions
        """
        logger.info("Running CLV prediction...")

        # Grab features into pandas for sklearn
        feature_cols = [
            "RECENCY", "FREQUENCY", "MONETARY",
            "AVG_ORDER_VALUE", "LIFESPAN_DAYS",
            "PURCHASE_REGULARITY",
        ]
        pdf = (
            enriched_df
            .select(["CUSTOMER_KEY"] + feature_cols)
            .to_pandas()
        )

        # Handle nulls
        pdf[feature_cols] = pdf[feature_cols].fillna(0)

        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(pdf[feature_cols].values)

        # Auto-tune K if not specified
        if k is None:
            k = self.find_optimal_k(X_scaled)

        # Fit final model
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        pdf["SEGMENT"] = kmeans.fit_predict(X_scaled)

        # Predict CLV
        # CLV = AOV × Frequency × (Lifespan / 365)
        pdf["PREDICTED_CLV"] = (
            pdf["AVG_ORDER_VALUE"]
            * pdf["FREQUENCY"]
            * np.maximum(pdf["LIFESPAN_DAYS"] / 365.0, 1.0 / 12.0)
        ).round(2)

        # Segment labels
        segment_labels = {
            0: "Champions",
            1: "Loyal",
            2: "Potential",
            3: "At Risk",
            4: "Hibernating",
        }
        if k <= len(segment_labels):
            # Order segments by CLV
            segment_avg_clv = pdf.groupby("SEGMENT")["PREDICTED_CLV"].mean()
            ranked_segments = segment_avg_clv.sort_values(ascending=False).index
            label_map = {
                seg: list(segment_labels.values())[i]
                for i, seg in enumerate(ranked_segments)
            }
        else:
            label_map = {i: f"Segment_{i}" for i in range(k)}

        pdf["SEGMENT_LABEL"] = pdf["SEGMENT"].map(label_map)

        logger.info(f"CLV predictions generated for {len(pdf)} customers")
        return pdf, kmeans, scaler

    # 5. Write Results to Gold Layer

    def write_results(self, clv_pdf):
        """Write CLV predictions to GOLD.CUSTOMER_CLV table."""
        logger.info("Writing CLV results to Snowflake...")

        # Select output columns
        output_cols = [
            "CUSTOMER_KEY", "RECENCY", "FREQUENCY", "MONETARY",
            "AVG_ORDER_VALUE", "LIFESPAN_DAYS", "PURCHASE_REGULARITY",
            "SEGMENT", "SEGMENT_LABEL", "PREDICTED_CLV",
        ]
        output_pdf = clv_pdf[output_cols].copy()
        output_pdf["MODEL_VERSION"] = self.model_version
        output_pdf["PREDICTED_AT"] = datetime.now(timezone.utc).isoformat()

        # Write to Snowflake
        sp_df = self.session.create_dataframe(output_pdf)
        sp_df.write.mode("overwrite").save_as_table(
            f"{self.gold_schema}.CUSTOMER_CLV"
        )

        logger.success(
            f"Wrote {len(output_pdf)} CLV predictions to "
            f"{self.gold_schema}.CUSTOMER_CLV"
        )

    # 6. Model Versioning

    def version_model(self, kmeans, scaler, metrics: dict):
        """Track model version in metadata table."""
        self.model_version = (
            f"v{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        )

        metadata = {
            "version": self.model_version,
            "k": int(kmeans.n_clusters),
            "silhouette_score": metrics.get("silhouette_score", 0),
            "n_customers": metrics.get("n_customers", 0),
            "feature_columns": [
                "RECENCY", "FREQUENCY", "MONETARY",
                "AVG_ORDER_VALUE", "LIFESPAN_DAYS",
                "PURCHASE_REGULARITY",
            ],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Create metadata table if not exists
        self.session.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.gold_schema}.CLV_MODEL_VERSIONS (
                model_version VARCHAR(50) PRIMARY KEY,
                metadata VARIANT,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()

        self.session.sql(f"""
            INSERT INTO {self.gold_schema}.CLV_MODEL_VERSIONS
                (model_version, metadata)
            SELECT '{self.model_version}',
                   PARSE_JSON($${json.dumps(metadata)}$$)
        """).collect()

        logger.info(f"Model version {self.model_version} recorded")

    # Full Pipeline

    def run(self, k: Optional[int] = None):
        """Execute the full CLV prediction pipeline."""
        logger.info("Starting CLV Prediction Pipeline...")

        # Step 1: Extract RFM
        rfm_df = self.extract_rfm_features()

        # Safety guard: skip if no delivered orders found
        rfm_count = rfm_df.count()
        if rfm_count == 0:
            logger.warning("No delivered orders found - skipping CLV calculation")
            logger.warning("This is expected for small demo batches without delivered orders")
            logger.info("CLV Pipeline Complete - No customers to segment")
            return None

        # Step 2: Engineer features
        enriched_df = self.engineer_features(rfm_df)

        # Step 3-4: Segment & Predict
        clv_pdf, kmeans, scaler = self.predict_clv(enriched_df, k=k)

        # Step 5: Version model
        n_customers = len(clv_pdf)
        if n_customers < 2:
            logger.warning(f"Only {n_customers} customer(s) found - too few to segment")
            logger.info("CLV Pipeline Complete - Insufficient data for clustering")
            return None

        sil = silhouette_score(
            StandardScaler().fit_transform(
                clv_pdf[
                    ["RECENCY", "FREQUENCY", "MONETARY",
                     "AVG_ORDER_VALUE", "LIFESPAN_DAYS",
                     "PURCHASE_REGULARITY"]
                ].fillna(0).values
            ),
            clv_pdf["SEGMENT"].values,
            sample_size=min(5000, n_customers),
        )
        self.version_model(
            kmeans, scaler,
            {"silhouette_score": round(sil, 4), "n_customers": n_customers},
        )

        # Step 6: Write results
        self.write_results(clv_pdf)

        # Summary
        logger.info("=" * 60)
        logger.info(f"CLV Pipeline Complete - {self.model_version}")
        logger.info(f"  Customers: {n_customers}")
        logger.info(f"  Segments: {kmeans.n_clusters}")
        logger.info(f"  Silhouette: {sil:.4f}")
        logger.info("=" * 60)

        return clv_pdf


# Standalone Execution

if __name__ == "__main__":
    import os

    connection_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "ECOMMERCE_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DW"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
    }

    session = Session.builder.configs(connection_params).create()

    try:
        model = CLVModel(session)
        # Force K=4 segments as requested by the user
        results = model.run(k=4)
        print(f"\nCLV Segment Distribution:")
        print(results.groupby("SEGMENT_LABEL")["PREDICTED_CLV"].describe())
    finally:
        session.close()
