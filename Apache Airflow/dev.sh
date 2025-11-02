export AIRFLOW_HOME="$(pwd)"
export AIRFLOW_CONFIG=./config/airflow.cfg
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////$(pwd)/config/airflow.db
export AIRFLOW__LOGGING__BASE_LOG_FOLDER=$(pwd)/logs
export AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY=$(pwd)/logs/dags_processor
export AIRFLOW__API_AUTH__JWT_SECRET='IR2yxRiYxjHiAWJF7IhE5Q=='

airflow $@
