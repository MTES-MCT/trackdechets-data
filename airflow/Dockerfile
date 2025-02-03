ARG AIRFLOW_VERSION
FROM apache/airflow:${AIRFLOW_VERSION}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential gcc \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ADD requirements.txt .

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
