
from __future__ import annotations

import pendulum
import requests
import duckdb
import os

from airflow.sdk import dag, task
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


from pathlib import Path
from utils.scraper import scrape_download_links

# Define the base path to the dbt project using pathlib for dynamic resolution
# This navigates up from the DAG file to the root of the 'Repo' directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent # Apache Airflow
REPO_ROOT = PROJECT_ROOT.parent
DBT_PROJECT_PATH = str(REPO_ROOT / "Data Build Tool" / "nyc_taxi_project")
DBT_PROFILES_DIR = str(REPO_ROOT / "Data Build Tool" / "nyc_taxi_project")
DUCKDB_FILE = str(REPO_ROOT / "Data Build Tool" / "nyc_taxi_project" / "dev.duckdb")
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
ZONES_FILE_NAME = "taxi_zone_lookup.csv"
TABLE_NAME = "raw_yellow_tripdata"
NYC_TLC_DATA_PAGE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
RAW_DATA_PATH = str(PROJECT_ROOT / "data" / "raw")


@dag(
    dag_id="nyc_taxi_ingestion",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@monthly",
    catchup=False,
    params={
        "months": ["2023-01", "2023-02", "2023-03"]
    },
    doc_md="""
    ### NYC Taxi Ingestion DAG

    This DAG downloads NYC taxi data for specified months, validates it, and loads it into DuckDB.
    You can specify the months to be processed by providing a list of strings in the format `YYYY-MM`
    to the `months` parameter when triggering the DAG.
    """
)
def nyc_taxi_ingestion_dag():
    """
    DAG to ingest NYC taxi data for specified months.
    """

    @task
    def get_filtered_download_info(page_url: str, months_to_filter: list) -> list[dict]:
        """
        Scrapes the NYC TLC data page for download links and filters them by specified months.
        Returns a list of dictionaries with 'url' and 'file_name'.
        """
        all_links = scrape_download_links(page_url)
        filtered_info = []
        for link_info in all_links:
            full_url = f"{link_info['base_url']}{link_info['path']}"
            file_name = os.path.basename(link_info['path'])
            # Extract YYYY-MM from file_name, e.g., yellow_tripdata_2023-01.parquet -> 2023-01
            month_in_file = "-".join(file_name.split('_')[-1].split('.')[0].split('-')[-2:])

            if month_in_file in months_to_filter:
                filtered_info.append({'url': full_url, 'file_name': file_name})
        return filtered_info

    @task
    def download_data(infos: list) -> str:
        """
        Downloads data from a given URL and saves it locally.
        """
        paths = []
        for info in infos:
            file_path = f"{RAW_DATA_PATH}/{info['file_name']}"
            if os.path.exists(file_path):
                print(f"File already downloaded {file_path}")
            else:
                response = requests.get(info['url'])
                response.raise_for_status()
            
                with open(file_path, "wb") as f:
                    f.write(response.content)
            paths.append(file_path)
        return paths

    @task
    def validate_data(file_paths: list) -> str:
        """
        Validates the downloaded data. (Placeholder)
        """
        for path in file_paths:
            if not os.path.exists(path) or not os.path.getsize(path) > 0:
                raise ValueError(f"Data validation failed for {path}")
        return file_paths

    @task
    def load_to_duckdb(file_paths: list, table_name: str, db_file: str):
        """
        Loads data from a list of parquet files into a DuckDB table.
        """
        con = duckdb.connect(db_file)
        # DuckDB's read_parquet can take a list of files
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet({file_paths});")
        con.close()

    @task
    def send_success_notification(months: list):
        """
        Sends a success notification.
        """
        print(f"Data ingestion successful for months: {months}")


    #email_notification = EmailOperator(
    #    task_id='send_email_notification',
    #    to='aldoridhoni@gmail.com',
    #    subject='Airflow DAG Success: NYC Taxi Ingestion',
    #    html_content='<h3>The NYC Taxi Ingestion DAG completed successfully for months: {{ params.months }}.</h3>', 
    #)

    # Get the list of months from the DAG parameters
    months_to_process = "{{ params.months }}"

    # Scrape and filter download info
    filtered_download_info = get_filtered_download_info(NYC_TLC_DATA_PAGE_URL, months_to_process)

    # Dynamically map tasks for each month
    downloaded_files = download_data(filtered_download_info)
    validated_files = validate_data(downloaded_files)

    # Load all validated files at once and then send notifications
    load_task = load_to_duckdb(validated_files, TABLE_NAME, DUCKDB_FILE)
    notification_task = send_success_notification(months=months_to_process)
    
    load_task >> notification_task #>> email_notification

# Instantiate the DAG
nyc_taxi_ingestion_dag_instance = nyc_taxi_ingestion_dag()

@dag(
    dag_id="nyc_taxi_zones_seed",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@once",
    catchup=False,
    doc_md="""
    ### NYC Taxi Zones Seed DAG

    This DAG downloads the taxi zone lookup data and loads it into the seeds directory.
    """
)
def nyc_taxi_zones_seed_dag():
    """
    DAG to seed the taxi zones data.
    """
    @task
    def download_zones_data(url: str, file_name: str) -> str:
        """
        Downloads the taxi zone lookup data.
        """
        response = requests.get(url)
        response.raise_for_status()
        file_path = f"{DBT_PROJECT_PATH}/seeds/{file_name}"
        with open(file_path, "wb") as f:
            f.write(response.content)
        return file_path

    download_zones_data(ZONES_URL, ZONES_FILE_NAME)

nyc_taxi_zones_seed_dag_instance = nyc_taxi_zones_seed_dag()


@dag(
    dag_id="nyc_taxi_transform",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 6 * * *", # Daily at 6 AM
    catchup=False,
    doc_md="""
    ### NYC Taxi Transformation DAG

    This DAG runs dbt transformations.
    """
)
def nyc_taxi_transform_dag():
    """
    DAG to run dbt transformations.
    """
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"dbt deps --profiles-dir '{DBT_PROFILES_DIR}' --project-dir '{DBT_PROJECT_PATH}'",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed --profiles-dir '{DBT_PROFILES_DIR}' --project-dir '{DBT_PROJECT_PATH}'",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir '{DBT_PROFILES_DIR}' --project-dir '{DBT_PROJECT_PATH}'",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir '{DBT_PROFILES_DIR}' --project-dir '{DBT_PROJECT_PATH}'",
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"dbt docs generate --profiles-dir '{DBT_PROFILES_DIR}' --project-dir '{DBT_PROJECT_PATH}'",
    )

    dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> dbt_docs_generate

# Instantiate the DAG
nyc_taxi_transform_dag_instance = nyc_taxi_transform_dag()


@dag(
    dag_id="nyc_taxi_monitor",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 */6 * * *", # Every 6 hours
    catchup=False,
    doc_md="""
    ### NYC Taxi Monitoring DAG

    This DAG monitors the data warehouse for data freshness, quality, and anomalies.
    It also generates daily summary reports.
    """
)
def nyc_taxi_monitor_dag():
    """
    DAG to monitor the data warehouse.
    """
    @task
    def check_data_freshness(db_file: str, table_name: str = 'fct_trips', freshness_threshold_hours: int = 24):
        """
        Checks if the data in the specified table is fresh enough.
        """
        con = duckdb.connect(db_file, read_only=True)
        try:
            latest_trip_date_result = con.execute(f"SELECT MAX(pickup_datetime) FROM {table_name}").fetchone()
            if latest_trip_date_result and latest_trip_date_result[0]:
                latest_trip_date = latest_trip_date_result[0]
                # Convert pendulum datetime to standard datetime for comparison
                if isinstance(latest_trip_date, pendulum.DateTime):
                    latest_trip_date = latest_trip_date.in_timezone('UTC').replace(tzinfo=None)
                
                time_diff_hours = (datetime.now() - latest_trip_date).total_seconds() / 3600
                if time_diff_hours > freshness_threshold_hours:
                    return f"Data freshness alert: Data is {time_diff_hours:.2f} hours old, which exceeds the threshold of {freshness_threshold_hours} hours."
        finally:
            con.close()
        return "Data freshness check passed."

    @task
    def validate_row_counts(db_file: str, table_name: str = 'fct_trips', days_to_compare: int = 7, anomaly_threshold_percentage: float = 20.0):
        """
        Validates the row count of the last day against the average of previous days.
        """
        con = duckdb.connect(db_file, read_only=True)
        try:
            # Get row count for the most recent day
            today_count_result = con.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE DATE(pickup_datetime) = (SELECT MAX(DATE(pickup_datetime)) FROM {table_name})
            """).fetchone()
            today_count = today_count_result[0] if today_count_result else 0

            # Get average row count for the previous N days
            avg_count_result = con.execute(f"""
                SELECT AVG(daily_count) FROM (
                    SELECT COUNT(*) as daily_count
                    FROM {table_name}
                    WHERE DATE(pickup_datetime) BETWEEN (SELECT MAX(DATE(pickup_datetime)) - INTERVAL '{days_to_compare} days' FROM {table_name}) AND (SELECT MAX(DATE(pickup_datetime)) - INTERVAL '1 day' FROM {table_name})
                    GROUP BY DATE(pickup_datetime)
                )
            """).fetchone()
            avg_count = avg_count_result[0] if avg_count_result and avg_count_result[0] is not None else 0
            
            if avg_count > 0:
                percentage_diff = abs(today_count - avg_count) / avg_count * 100
                if percentage_diff > anomaly_threshold_percentage:
                    return f"Row count anomaly alert: Today's row count ({today_count}) deviates by {percentage_diff:.2f}% from the average of the last {days_to_compare} days ({avg_count:.2f})."
        finally:
            con.close()
        return "Row count validation passed."

    @task
    def check_for_anomalies(db_file: str, table_name: str = 'fct_trips', days_to_compare: int = 7, anomaly_threshold_percentage: float = 15.0):
        """
        Checks for anomalies in key business metrics like average fare amount.
        """
        con = duckdb.connect(db_file, read_only=True)
        try:
            # Get average fare for the most recent day
            today_avg_fare_result = con.execute(f"""
                SELECT AVG(fare_amount) FROM {table_name}
                WHERE DATE(pickup_datetime) = (SELECT MAX(DATE(pickup_datetime)) FROM {table_name})
            """).fetchone()
            today_avg_fare = today_avg_fare_result[0] if today_avg_fare_result and today_avg_fare_result[0] is not None else 0

            # Get average fare for the previous N days
            historical_avg_fare_result = con.execute(f"""
                SELECT AVG(fare_amount) FROM {table_name}
                WHERE DATE(pickup_datetime) BETWEEN (SELECT MAX(DATE(pickup_datetime)) - INTERVAL '{days_to_compare} days' FROM {table_name}) AND (SELECT MAX(DATE(pickup_datetime)) - INTERVAL '1 day' FROM {table_name})
            """).fetchone()
            historical_avg_fare = historical_avg_fare_result[0] if historical_avg_fare_result and historical_avg_fare_result[0] is not None else 0

            if historical_avg_fare > 0:
                percentage_diff = abs(today_avg_fare - historical_avg_fare) / historical_avg_fare * 100
                if percentage_diff > anomaly_threshold_percentage:
                    return f"Fare amount anomaly alert: Today's average fare ({today_avg_fare:.2f}) deviates by {percentage_diff:.2f}% from the historical average ({historical_avg_fare:.2f})."
        finally:
            con.close()
        return "Anomaly check passed."

    @task
    def generate_summary_report(db_file: str, table_name: str = 'agg_daily_stats'):
        """
        Generates a summary report of the latest daily statistics.
        """
        con = duckdb.connect(db_file, read_only=True)
        try:
            report_data = con.execute(f"""
                SELECT * FROM {table_name}
                ORDER BY trip_date DESC
                LIMIT 1
            """).fetchdf() # fetchdf returns a pandas DataFrame
            if not report_data.empty:
                report = f"""
                --- Daily Summary Report ---
                Date: {report_data['trip_date'].iloc[0].strftime('%Y-%m-%d')}
                Total Trips: {report_data['total_trips'].iloc[0]}
                Total Revenue: ${report_data['total_revenue'].iloc[0]:,.2f}
                Average Fare: ${report_data['average_fare'].iloc[0]:,.2f}
                Average Distance: {report_data['average_distance'].iloc[0]:.2f} miles
                Average Duration: {report_data['average_duration'].iloc[0]:.2f} minutes
                --------------------------
                """
                print(report)
                return report
        finally:
            con.close()
        return "Could not generate summary report: No data found."

    @task
    def trigger_alert(alerts: list):
        """
        Receives alerts from check tasks and triggers a notification.
        """
        actual_alerts = [alert for alert in alerts if "alert" in alert.lower()]
        if actual_alerts:
            alert_message = "\n".join(actual_alerts)
            print(f"MONITORING ALERT:\n{alert_message}")
            # In a real scenario, you would use an EmailOperator, SlackOperator, etc.
            # For this example, we'll just print to logs.
            # You could also raise an exception to make the task fail.
            # from airflow.exceptions import AirflowException
            # raise AirflowException(f"Monitoring checks failed: {alert_message}")
        else:
            print("All monitoring checks passed.")

    # Define task dependencies
    freshness_check_result = check_data_freshness(DUCKDB_FILE)
    row_count_check_result = validate_row_counts(DUCKDB_FILE)
    anomaly_check_result = check_for_anomalies(DUCKDB_FILE)

    alerting_task = trigger_alert(alerts=[freshness_check_result, row_count_check_result, anomaly_check_result])
    reporting_task = generate_summary_report(DUCKDB_FILE)

    # Set up dependencies
    [freshness_check_result, row_count_check_result, anomaly_check_result] >> alerting_task
    # Reporting task can run in parallel
    reporting_task

# Instantiate the DAG
nyc_taxi_monitor_dag_instance = nyc_taxi_monitor_dag()
