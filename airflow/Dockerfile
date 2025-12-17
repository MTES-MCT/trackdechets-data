ARG AIRFLOW_VERSION
FROM apache/airflow:${AIRFLOW_VERSION}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential gcc git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# install nodejs following https://github.com/nodesource/distributions
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg

# install scalingo
RUN curl -O https://cli-dl.scalingo.com/install && bash install && chmod +x $(which scalingo)

ENV NODE_MAJOR=18
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list
RUN apt-get update
RUN command apt-get install nodejs -y

USER airflow
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
