FROM apache/airflow:2.9.1
ADD requirements.txt .
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
RUN mkdir -m777 tmp
RUN python -m venv dbt-venv
RUN source ./dbt-venv/bin/activate
RUN pip install --no-cache-dir dbt-bigquery
