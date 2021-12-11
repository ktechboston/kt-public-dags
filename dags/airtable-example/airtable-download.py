from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

with DAG(dag_id="airtable-download",
         start_date=datetime(2021, 11, 1),
         concurrency=1) as dag:

    download_time_tracker_base = DummyOperator(task_id="download-time-tracker-base",
                                               pool="airtable-api-pool")
    download_projects_base =     DummyOperator(task_id="download-projects-base",
                                               pool="airtable-api-pool")
    download_crm_base =          DummyOperator(task_id="download-crm-base",
                                               pool="airtable-api-pool")

    notify_failure = DummyOperator(task_id="notify-failure",
                                   trigger_rule="one_failed")

    [download_crm_base, download_projects_base, download_time_tracker_base] >> notify_failure
