FROM bitnamilegacy/spark:3.5.0

# Install pip & PySpark (matching Spark version) + any extras
USER root
RUN apt-get update && apt-get install -y python3-pip && \
    pip install --no-cache-dir pyspark==3.5.0 && \
    rm -rf /var/lib/apt/lists/*

# Optional: install Python packages from requirements.txt
#COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt || true

USER 1001
