FROM apache/airflow:3.1.1

COPY requirements.txt .
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"
RUN pip install --no-cache-dir -r requirements.txt
