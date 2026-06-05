# ============================================================
# AIS Vessel Collision Detector
# Base image: Apache Spark
# ============================================================
FROM apache/spark:3.5.1

# Switch to root to install Python packages
USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    folium==0.16.0 \
    pandas==2.0.3 \
    pyarrow==16.0.0

# Create working directories
RUN mkdir -p /app /data /output

# Copy the detection script
COPY collision_detector.py /app/collision_detector.py

# Data and output dirs will be mounted as volumes at runtime
VOLUME ["/data", "/output"]

# Environment variables (can be overridden with -e on docker run)
ENV DATA_PATH=/data
ENV OUTPUT_DIR=/output
ENV PYSPARK_PYTHON=python3

WORKDIR /app

# Entry point: run the PySpark job via the Spark install path
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
            "--master", "local[*]", \
            "--driver-memory", "4g", \
            "/app/collision_detector.py"]
