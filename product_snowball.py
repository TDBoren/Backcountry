import datetime
import yaml
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os
from airflow.models import Variable
import pendulum

local_tz = pendulum.timezone('America/Denver')

ps_dag_path = Variable.get("PS_DAG_PATH")

# Read config file
yamlFile = os.path.join(ps_dag_path, "yaml/dags_yaml_ps_config.yaml")

with open(yamlFile, 'r') as key_file:
    configDict = yaml.safe_load(key_file.read())
    pscon = configDict['ps_config']
    json_fname = pscon['json_fname']
    project_id = pscon['gcp_project']

# assing json path
json_path = os.path.join(ps_dag_path, json_fname)


# env_type = os.environ['env_type']

# if env_type == 'dev':
#    bucket_name = bucket_name + '_' + env_type
#    dataset_id = dataset_id + '_' + env_type


def _print_exec_date(**context):
    print("execution_date: ", context["execution_date"])


default_args = {
    'owner': 'Core Daily',
    'start_date': datetime.datetime(2020, 5, 10, tzinfo=local_tz),
    'email': ['datainsights-etl-failure@backcountry.pagerduty.com', 'biteam@backcountry.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'execution_timeout': timedelta(hours=1)
}

with DAG('products_snowball', default_args=default_args
        , max_active_runs=1
        , schedule_interval="15 0 * * *"
        , catchup=False) as dag:
    print_exec_date = PythonOperator(
        task_id="print_exec_date",
        python_callable=_print_exec_date,
        provide_context=True,
    )

    load_stage_product_rootdb_table = BigQueryOperator(
        dag=dag,
        task_id='load_stage_product_rootdb_table',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/create_rootdb_products_stage.sql'
    )

    load_stage_product_netsuite_table = BigQueryOperator(
        dag=dag,
        task_id='load_stage_product_netsuite_table',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/create_netsuite_products_stage.sql'
    )

    source_complete = DummyOperator(task_id='source_complete', dag=dag, trigger_rule='none_failed')

    load_stage_product_snowball_table = BigQueryOperator(
        dag=dag,
        task_id='load_stage_product_snowball_table',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/create_products_snowball_stage.sql'
    )

    load_modified_products_scd = BigQueryOperator(
        dag=dag,
        task_id='load_modified_products_scd',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/modified_products_scd.sql'
    )

    load_modified_products_no_scd = BigQueryOperator(
        dag=dag,
        task_id='load_modified_products_no_scd',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/modified_products_no_scd.sql'
    )

    load_new_products = BigQueryOperator(
        dag=dag,
        task_id='load_new_products',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/insert_new_products.sql'
    )

    update_deleted_products = BigQueryOperator(
        dag=dag,
        task_id='update_deleted_products',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/update_deleted_netsuite_sku.sql'
    )

    post_load_updates = BigQueryOperator(
        dag=dag,
        task_id='post_load_updates',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/product_post_load_updates.sql'
    )

    job_complete = DummyOperator(task_id='job_complete', dag=dag, trigger_rule='none_failed')

    print_exec_date >> load_stage_product_rootdb_table >> source_complete
    print_exec_date >> load_stage_product_netsuite_table >> source_complete
    source_complete >> load_stage_product_snowball_table >> load_modified_products_scd >> load_modified_products_no_scd \
    >> load_new_products >> update_deleted_products >> post_load_updates >> job_complete