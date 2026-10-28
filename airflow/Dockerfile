FROM apache/airflow:2.11.0-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk --no-install-recommends curl && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install apache-airflow apache-airflow-providers-apache-spark mysql-connector-python pyspark==3.5.0 apache-airflow-providers-common-io apache-airflow-providers-openlineage