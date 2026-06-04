# Use the official Airflow image with Python 3.11
FROM apache/airflow:2.8.1-python3.11

# Set permissions for the root user to install system packages if needed
USER root

# Install any system dependencies (git is useful for dbt)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user for pip installs
USER airflow

# Copy the requirements file into the image
COPY requirements-airflow.txt /requirements-airflow.txt

# Install dependencies into the image (this happens at build time)
# --no-cache-dir saves space in the image
RUN pip install --no-cache-dir --user -r /requirements-airflow.txt
