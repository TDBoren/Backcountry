## This process is creating dimension tables for S5 interfaces

## import packages
from datetime import timedelta, datetime
import os
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers import bigquery_to_gcs
from airflow.operators.dummy_operator import DummyOperator
import pendulum
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from google.cloud import bigquery
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

## Initialize global variables
env_type = os.environ['env_type']
dataset_id="interface_data"
target_bucket="interface_data_team"
destination_object = "production"

timestamp=datetime.now()
cur_day_format = timestamp.strftime("%Y%m%d%H%M%S")

if env_type == 'dev':
    dataset_id = dataset_id + '_' + env_type
    target_bucket = target_bucket + '_' + env_type
    destination_object="development"

local_tz = pendulum.timezone('America/Denver')

## Representing the dag name
dag_id = 'S5_interface_data_size_group'

## passing the default arguments
default_args = {
    'owner': 'S5',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 4, 7, 25, tzinfo=local_tz),
    #'email': ['datainsights-etl-failure@backcountry.pagerduty.com', 'data@backcountry.com'],
    #'email_on_failure': True,
    #'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

#Phase2 Changes start

def load_table_from_gcs_to_bq(**kwargs):
    client = bigquery.Client()
    project = "backcountry-data-team"
    table_id = kwargs['table_name']

    today = datetime.today()
    table_whole_name='{}.{}.{}'.format(project,dataset_id,table_id)

# Create a permanent table linked to the GCS file
    job_config = bigquery.LoadJobConfig(
    schema = [
    bigquery.SchemaField("AXIS2", "STRING"),
    bigquery.SchemaField("GROUP_NAME", "STRING"),
    bigquery.SchemaField("SIZING_GROUP", "STRING"),
    bigquery.SchemaField("OPTIONAL_BRAND", "STRING"),
    bigquery.SchemaField("OPTIONAL_MC", "STRING"),
    bigquery.SchemaField("OPTIONAL_DISABLE", "STRING"),
    ],

    skip_leading_rows=1,
    #allow_jagged_rows = True,
    source_format=bigquery.SourceFormat.CSV,
    )

    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.field_delimiter =','
    #uri = "gs://{}/s5_source_files/Size Group Table.CSV".format(target_bucket)
    uri = 'gs://interface_data_team_dev/s5_source_files/Size Group Table.csv'
    # load the csv into bigquery
    load_job = client.load_table_from_uri(
    uri,
    table_whole_name,
    job_config=job_config
    )# Make an API request.

    load_job.result() # Waits for the job to complete

 ## Initialize DAG parameters
with DAG(dag_id, schedule_interval=None, 
         max_active_runs=1,
         dagrun_timeout=timedelta(seconds=18000),
         catchup=False,
         default_args=default_args
         ) as dag:

    # Dag Start label
    start_task = DummyOperator(
        task_id='Start',
        trigger_rule='one_success',
    )

## Load S5 tables

    load_size_group_table_to_bq = PythonOperator(
        task_id="load_size_group_table_to_bq",
        trigger_rule= 'all_success',
        python_callable=load_table_from_gcs_to_bq,
        op_kwargs={'table_name': 'size_group'},
        provide_context=True
    )

    # Dag End label
    end_task = DummyOperator(
        task_id='End',
        trigger_rule='none_failed'
    )

## Task Execution Sequence
start_task >> load_size_group_table_to_bq >> end_task