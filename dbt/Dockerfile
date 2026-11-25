ARG SPARK_IMAGE_TAG=3.5.6-python3
FROM apache/spark:${SPARK_IMAGE_TAG}

USER root

# dbt-spark "session" method runs dbt in-process with the image's own PySpark,
# so dbt uses exactly the same Spark + Iceberg runtime as the ingestion job.
# The image ships Python 3.8 => dbt-core 1.8 is the newest compatible line.
# pyspark is NOT installed from pip: the one in /opt/spark/python is used.
RUN python3 -m pip install --no-cache-dir --upgrade "pip<25" \
 && python3 -m pip install --no-cache-dir "dbt-core~=1.8.0" "dbt-spark~=1.8.0"

ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    SPARK_HOME=/opt/spark \
    DBT_PROFILES_DIR=/dbt \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false

WORKDIR /dbt/audit_lakehouse
ENTRYPOINT []
