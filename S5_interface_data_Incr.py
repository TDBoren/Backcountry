## This process is creating dimension tables for S5 interfaces

## import packages
from datetime import timedelta, datetime
import os
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers import bigquery_to_gcs
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
import pendulum
from airflow.sensors.external_task_sensor import ExternalTaskSensor
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
dag_id = 'S5_interface_data_Incr'

## passing the default arguments
default_args = {
    'owner': 'S5',
    'depends_on_past': False,
    'start_date': datetime(year=2021, month=10, day=14, tzinfo=local_tz),
    'email': ['ayan.das@jadeglobal.com','troy.boren@jadeglobal.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

#Phase2 Changes start

def load_table_from_gcs_to_bq(**kwargs):
    client = bigquery.Client()
    project = "backcountry-data-team"
    table_id1 = kwargs['table_name1']
    table_id2 = kwargs['table_name2']

    today = datetime.today()
    table_whole_name1='{}.{}.{}'.format(project,dataset_id,table_id1)
    table_whole_name2='{}.{}.{}'.format(project,dataset_id,table_id2)

# Create a permanent table linked to the GCS file
    job_config = bigquery.LoadJobConfig(
    schema = [
    bigquery.SchemaField("ZIPCODE", "STRING"),
    bigquery.SchemaField("OPTIMAL_FACILITY", "STRING"),
    bigquery.SchemaField("BACKUP", "STRING"),
    ],

    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
    )

    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.field_delimiter =','
    uri = "gs://{0}/s5_source_files/Optimal Origin Facility for Dest Zip.CSV".format(target_bucket)

    # load the csv into bigquery
    load_job = client.load_table_from_uri(
    uri,
    table_whole_name1,
    job_config=job_config
    )# Make an API request.

    load_job.result() # Waits for the job to complete

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
    uri = "gs://{0}/s5_source_files/Size Group Table.csv".format(target_bucket)
    #uri = 'gs://interface_data_team_dev/s5_source_files/Size Group Table.csv'
    # load the csv into bigquery
    load_job = client.load_table_from_uri(
    uri,
    table_whole_name2,
    job_config=job_config
    )# Make an API request.

    load_job.result() # Waits for the job to complete

#Phase2 Changes end

## function to export feed files
def load_table_from_bq_to_gcs(**kwargs):
    client = bigquery.Client()

    project = "backcountry-data-team"
    table_id = kwargs['table_name']
    #interface_id = kwargs['interface_id']
    exec_dt = kwargs['next_execution_date']
    exec_dt_str=exec_dt.strftime("%Y%m%d%H%M%S")
    interface=table_id.split("_")[1]

    destination_uri = 'gs://{2}/S5/{1}/{0}/S5_{1}_{0}_*.DAT'.format(interface,exec_dt_str,target_bucket)
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.job.ExtractJobConfig()
    job_config.field_delimiter = '|'
    job_config.destination_format = 'CSV'

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="US",
        job_config=job_config
    )
    # API request
    extract_job.result()
    # Waits for job to complete.
    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )
    
    #Query total number of rows
    query = ("create or replace table `{2}.{1}.S5RdyFile_{0}` as select 'S5_{3}_{0}.DAT' as FILENAME, count(*) as COUNT FROM `{2}.{1}.S5_{0}`;".format(interface, dataset_id, project, exec_dt_str))
    
    query_job = client.query(query, location="US")
    results = query_job.result()
    
def load_RDYFile_to_gcs(**kwargs):
    client = bigquery.Client()

    project = "backcountry-data-team"
    table_id = kwargs['table_name']
    #interface_id = kwargs['interface_id']
    exec_dt = kwargs['next_execution_date']
    exec_dt_str=exec_dt.strftime("%Y%m%d%H%M%S")
    interface=table_id.split("_")[1]

    destination_uri = 'gs://{2}/S5/{1}/{0}_{1}.DAT'.format(interface,exec_dt_str,target_bucket)
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.job.ExtractJobConfig()
    job_config.field_delimiter = '|'
    job_config.destination_format = 'CSV'

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="US",
        job_config=job_config
    )
    # API request
    extract_job.result()
    # Waits for job to complete.
    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )

 ## function to check reject records and trigger email
def email_and_reject(**kwargs):
    client = bigquery.Client()
    query = """
        SELECT interface_name
        FROM `backcountry-data-team.{0}.S5_Rejects`
        WHERE REJECT_COUNT>0
        """.format(dataset_id) 
    
    query_job = client.query(query)  # Make an API request.
    results = query_job.result()

    # Row values can be accessed by field name or index.
    print("No. of Reject interfaces: {}".format(results.total_rows))
    n_rows=results.total_rows

    if n_rows==0:
        email_no_rejects = EmailOperator(
        task_id='send_email',
        to='data@backcountry.com',
        subject='S5 Load Completed with no rejects | Execution date: {0}'.format(cur_day_format),
        html_content="""<p> Hi, </p>
                        <p> S5 Load completed succesfully with no rejects.</p>"""
        )
        email_no_rejects.execute(dict())
        
    else:
        email_with_rejects = EmailOperator(
        task_id='send_email',
        to='data@backcountry.com',
        subject='S5 Load Completed with Rejects | Execution date: {0}'.format(cur_day_format),
        html_content="""<p> Hi, </p>
                        <p> S5 Load completed succesfully.</p>
                        <p> For more information on reject records, please run the 
                        below query on BigQuery:
                        <p> Select * from `backcountry-data-team.{0}.S5_Rejects`
                        where reject_count>0; </p>""".format(dataset_id)
        )
        email_with_rejects.execute(dict())

dayofweek = (datetime.today().strftime('%A'))

# Branching Function
def load_brancher(**kwargs):
    if dayofweek != "Sunday":
        return "Daily_tasks"
    else:
        return "Weekly_tasks"

## Initialize DAG parameters
with DAG(dag_id, schedule_interval="45 6 * * 1-7",
         max_active_runs=1,
         dagrun_timeout=timedelta(seconds=18000),
         catchup=False,
         default_args=default_args
         ) as dag:

    ## Dag Start label
    start_task = DummyOperator(
        task_id='Start',
        trigger_rule='one_success',
    )

    Daily_check_brancher = BranchPythonOperator(
        task_id='Daily_check_brancher',
        python_callable=load_brancher
    )

    Daily_tasks = DummyOperator(
        task_id='Daily_tasks'
        )

    Weekly_tasks = DummyOperator(
        task_id='Weekly_tasks'
        )
   
    #Dag Start label
    #sensor = ExternalTaskSensor(
        #task_id = 'sensor',
        #external_dag_id = 'profimetrics_facts_interface',
        #external_task_id = None,
        #timeout=3600,
        #poke_interval = 600,
        #mode="reschedule",
        #execution_delta = timedelta(minutes=45),
        #dag = dag
    #)
       
    # Archive old files
    archive_S5_data = GCSToGCSOperator(
        task_id="archive_S5_data",
        source_bucket="{}".format(target_bucket),
        source_object="S5/",
        destination_bucket="{}".format(target_bucket),
        destination_object="S5_archive/",
        move_object=True,
        replace=True
    )

## Load S5 tables
    # Sprint 1
    load_Prod_Master_table = BigQueryOperator(
        task_id='load_Prod_Master_table',
        use_legacy_sql=False,
        sql='/sql/S5_ProdMaster.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )

    load_Prod_Org_table = BigQueryOperator(
        task_id='load_Prod_Org_table',
        use_legacy_sql=False,
        sql='/sql/S5_ProductOrg.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )

    load_Prod_Attr_SKU_table = BigQueryOperator(
        task_id='load_Prod_Attr_SKU_table',
        use_legacy_sql=False,
        sql='/sql/S5_ProdAttrSKU.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_Prod_Members_table = BigQueryOperator(
        task_id='load_Prod_Members_table',
        use_legacy_sql=False,
        sql='/sql/S5_ProdMembers.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_Product_Hierarchy_table = BigQueryOperator(
        task_id='load_Product_Hierarchy_table',
        use_legacy_sql=False,
        sql='/sql/S5_ProdHierarchy.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_Prod_Attr_Style_table = BigQueryOperator(
        task_id='load_Prod_Attr_Style_table',
        use_legacy_sql=False,
        sql='/sql/S5_ProdAttrStyle.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_Prod_Attr_Style_Clr_table = BigQueryOperator(
        task_id='load_Prod_Attr_Style_Clr_table',
        use_legacy_sql=False,
        sql='/sql/S5_ProdAttrStyleClr.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    # Sprint 2
    load_Location_Attribute_table = BigQueryOperator(
        task_id='load_Location_Attribute_table',
        use_legacy_sql=False,
        sql='/sql/S5_LocAttr.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_ProdAttrSKUArray_table = BigQueryOperator(
        task_id='load_ProdAttrSKUArray_table',
        use_legacy_sql=False,
        sql='/sql/S5_ProdAttrSKUArray.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_ImageURL_table = BigQueryOperator(
        task_id='load_ImageURL_table',
        use_legacy_sql=False,
        sql='/sql/S5_ImageURL.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_LocMembers_table = BigQueryOperator(
        task_id='load_LocMembers_table',
        use_legacy_sql=False,
        sql='/sql/S5_LocMembers.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_Location_Hierarchy_table = BigQueryOperator(
        task_id='load_Location_Hierarchy_table',
        use_legacy_sql=False,
        sql='/sql/S5_location_hierarchy.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_LocMaster_table = BigQueryOperator(
        task_id='load_LocMaster_table',
        use_legacy_sql=False,
        sql='/sql/S5_LocMaster.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    # Sprint 3

    load_backorder_sales_table = BigQueryOperator(
        task_id='load_backorder_sales_table',
        use_legacy_sql=False,
        sql='/sql/stg_backorder_sales.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_SalesDaily_table = BigQueryOperator(
        task_id='load_SalesDaily_table',
        use_legacy_sql=False,
        sql='/sql/S5_SalesDaily_Incr.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_ActualsInventory_table = BigQueryOperator(
        task_id='load_ActualsInventory_table',
        use_legacy_sql=False,
        sql='/sql/S5_ActualsInventory_Incr.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_OOActuals_table = BigQueryOperator(
        task_id='load_OOActuals_table',
        use_legacy_sql=False,
        sql='/sql/S5_OOActuals.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
    
    load_StyleDayLoc_table = BigQueryOperator(
        task_id='load_StyleDayLoc_table',
        use_legacy_sql=False,
        sql='/sql/S5_StyleDayLoc_Incr.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )

## Load table to gcs
    # Sprint 1
    load_Prod_Master_table_to_gcs = PythonOperator(
        task_id="load_Prod_Master_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_ProdMaster'},
        provide_context=True
        )
        
    load_Prod_Attr_SKU_table_to_gcs = PythonOperator(
        task_id="load_Prod_Attr_SKU_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_ProdAttrSKU'},
        provide_context=True
        )
        
    load_Product_Hierarchy_table_to_gcs = PythonOperator(
        task_id="load_Product_Hierarchy_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_ProdHierarchy'},
        provide_context=True
        )

    load_Prod_Attr_Style_table_to_gcs = PythonOperator(
        task_id="load_Prod_Attr_Style_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_ProdAttrStyle'},
        provide_context=True
        )

    load_Prod_Attr_Style_Clr_table_to_gcs = PythonOperator(
        task_id="load_Prod_Attr_Style_Clr_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_ProdAttrStyleClr'},
        provide_context=True
        )
    
    # Sprint 2
    load_Location_Attribute_table_to_gcs = PythonOperator(
        task_id="load_Location_Attribute_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_LocAttr'},
        provide_context=True
        )
        
    load_ProdAttrSKUArray_table_to_gcs = PythonOperator(
        task_id="load_ProdAttrSKUArray_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_ProdAttrSKUArray'},
        provide_context=True
        )
    
    load_ImageURL_table_to_gcs = PythonOperator(
        task_id="load_ImageURL_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_ImageURL'},
        provide_context=True
        )
        
    load_Location_Hierarchy_table_to_gcs = PythonOperator(
        task_id="load_Location_Hierarchy_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_LocHier'},
        provide_context=True
        )
    
    load_LocMaster_table_to_gcs = PythonOperator(
        task_id="load_LocMaster_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_LocMaster'},
        provide_context=True
        )
        
    # Sprint 3
    load_SalesDaily_table_to_gcs = PythonOperator(
        task_id="load_SalesDaily_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_SalesDaily'},
        provide_context=True
        )
        
    load_ActualsInventory_table_to_gcs = PythonOperator(
        task_id="load_ActualsInventory_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_ActualsInventory'},
        provide_context=True
        )
        
    load_OOActuals_table_to_gcs = PythonOperator(
        task_id="load_OOActuals_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_OOActuals'},
        provide_context=True
        )
        
    load_StyleDayLoc_table_to_gcs = PythonOperator(
        task_id="load_StyleDayLoc_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_StyleDayLoc'},
        provide_context=True
        )

    #Phase2 Changes
    load_networkzip_sizegroup_table_to_bq = PythonOperator(
        task_id="load_networkzip_sizegroup_table_to_bq",
        trigger_rule= 'all_success',
        python_callable=load_table_from_gcs_to_bq,
        op_kwargs={'table_name1': 'S5_NetworkZip_Stg', 'table_name2': 'S5_Sizegroup_Stg'},
        provide_context=True
    )

    # Load Size Group Table
    load_size_group_table = BigQueryOperator(
        task_id='load_size_group_table',
        use_legacy_sql=False,
        sql='/sql/S5_SizeGroup.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )

    # Load SKU Size Group Table
    load_SKU_size_group_table = BigQueryOperator(
        task_id='load_SKU_size_group_table',
        use_legacy_sql=False,
        sql='/sql/S5_SKUSizeGroup.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )

    load_NetworkZip_table = BigQueryOperator(
        task_id='load_NetworkZip_table',
        use_legacy_sql=False,
        sql='/sql/S5_NetworkZip.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )

    load_size_group_table_to_gcs = PythonOperator(
        task_id="load_size_group_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_SizeGroup'},
        provide_context=True
        )

    load_SKU_size_group_table_to_gcs = PythonOperator(
        task_id="load_SKU_size_group_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_SKUSizeGroup'},
        provide_context=True
        )
 
    load_NetworkZip_table_to_gcs = PythonOperator(
        task_id="load_NetworkZip_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_NetworkZip'},
        provide_context=True
    )

    load_NetworkLoc_table = BigQueryOperator(
        task_id='load_NetworkLoc_table',
        use_legacy_sql=False,
        sql='/sql/S5_NetworkLoc.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )

    load_NetworkLoc_table_to_gcs = PythonOperator(
        task_id="load_NetworkLoc_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_NetworkLoc'},
        provide_context=True
    )

    # Load Rejects Table
    load_Rejects_table = BigQueryOperator(
        task_id='load_Rejects_table',
        use_legacy_sql=False,
        sql='/sql/S5_Rejects.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
     
    # Load RdyFile Table
    load_RDYFile_table = BigQueryOperator(
        task_id='load_RDYFile_table',
        use_legacy_sql=False,
        sql='/sql/S5_S5RDYFile.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )
        
    # RdyFile    
    load_RDYFile_table_to_gcs = PythonOperator(
        task_id="load_RDYFile_table_to_gcs",
        python_callable=load_RDYFile_to_gcs,
        op_kwargs={'table_name': 'S5_S5RDYFile'},
        provide_context=True
        )
    
    # Final Bucket to Bucket Transfer
    S5_to_S5_transfer = GCSToGCSOperator(
        task_id="S5_to_S5_transfer",
        source_bucket="{}".format(target_bucket),
        source_object="S5/",
        destination_bucket="backcountry-s5-transfer",
        destination_object=f"""{destination_object}/in/""",
        move_object=False,
        replace=True
    )

    # Daily tasks not in weekly schedule

    load_backorder_sales_table_daily = BigQueryOperator(
        task_id='load_backorder_sales_table_daily',
        use_legacy_sql=False,
        sql='/sql/stg_backorder_sales.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )

    load_SalesDaily_Daily_table = BigQueryOperator(
        task_id='load_SalesDaily_Daily_table',
        use_legacy_sql=False,
        sql='/sql/S5_SalesDaily_Daily.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )

    load_ReplannableSKUs_table = BigQueryOperator(
        task_id='load_ReplannableSKUs_table',
        use_legacy_sql=False,
        sql='/sql/S5_ReplannableSKUs.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )

    load_OOActualsDaily_table = BigQueryOperator(
        task_id='load_OOActualsDaily_table',
        use_legacy_sql=False,
        sql='/sql/S5_OOActualsDaily.sql',
        params={'final_dataset_id': dataset_id},
        dag=dag
    )

    load_OOActualsDaily_table_to_gcs = PythonOperator(
        task_id="load_OOActualsDaily_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_OOActualsDaily'},
        provide_context=True
        )

    load_ReplannableSKUs_table_to_gcs = PythonOperator(
        task_id="load_ReplannableSKUs_table_to_gcs",
        python_callable=load_table_from_bq_to_gcs,
        op_kwargs={'table_name': 'S5_ReplannableSKUs'},
        provide_context=True
        )
  
    # End of Daily Only Tasks

    trigger_email = PythonOperator(
        task_id="trigger_email",
        python_callable=email_and_reject,
        provide_context=True
        )
    # Branching end tasks

    end_branching_daily = DummyOperator(
        task_id='end_branching_daily',
        trigger_rule='one_success'
    )
  
    end_branching_weekly = DummyOperator(
        task_id='end_branching_weekly',
        trigger_rule='one_success'
    )

    # Dag End label
    end_task = DummyOperator(
        task_id='End',
        trigger_rule='none_failed'
    )

## Task Execution Sequence
### sensor task is to be placed right after the start task on the next two lines when implemented.

start_task >> archive_S5_data >> load_Prod_Members_table
start_task >> archive_S5_data >> load_LocMembers_table
load_Prod_Members_table >> load_Prod_Master_table >> load_Prod_Master_table_to_gcs >> load_Rejects_table
load_Prod_Members_table >> load_Prod_Org_table >> load_Prod_Attr_SKU_table >> load_Prod_Attr_SKU_table_to_gcs >> load_Rejects_table
load_Prod_Members_table >> load_Prod_Attr_Style_table >> load_Prod_Attr_Style_table_to_gcs >> load_Rejects_table
load_Prod_Members_table >> load_Prod_Attr_Style_Clr_table >> load_Prod_Attr_Style_Clr_table_to_gcs >> load_Rejects_table
load_Prod_Members_table >> load_ProdAttrSKUArray_table >> load_ProdAttrSKUArray_table_to_gcs >> load_Rejects_table
load_Prod_Members_table >> load_ImageURL_table >> load_ImageURL_table_to_gcs >> load_Rejects_table
load_Prod_Members_table >> load_Product_Hierarchy_table >> load_Product_Hierarchy_table_to_gcs >> load_Rejects_table
load_LocMembers_table >> load_Location_Attribute_table >> load_Location_Attribute_table_to_gcs >> Daily_check_brancher
load_LocMembers_table >> load_LocMaster_table >> load_LocMaster_table_to_gcs >> Daily_check_brancher
load_LocMembers_table >> load_Location_Hierarchy_table >> load_Location_Hierarchy_table_to_gcs >> Daily_check_brancher
load_LocMembers_table >> load_networkzip_sizegroup_table_to_bq >> Daily_check_brancher
load_Rejects_table >> load_RDYFile_table >> load_RDYFile_table_to_gcs >> S5_to_S5_transfer >> trigger_email >> end_task

# daily_load:

Daily_check_brancher >> Daily_tasks \
>> load_OOActualsDaily_table >> load_OOActualsDaily_table_to_gcs >> end_branching_daily
Daily_tasks >> load_backorder_sales_table_daily >> load_SalesDaily_Daily_table >> end_branching_daily
Daily_tasks >> load_ReplannableSKUs_table >> load_ReplannableSKUs_table_to_gcs >> end_branching_daily
Daily_tasks >> load_SKU_size_group_table >> load_SKU_size_group_table_to_gcs \
>> load_size_group_table >> load_size_group_table_to_gcs >> end_branching_weekly >> load_Rejects_table
end_branching_daily >> load_SalesDaily_table_to_gcs >> end_branching_weekly >> load_Rejects_table
load_Rejects_table >> load_RDYFile_table >> load_RDYFile_table_to_gcs >>S5_to_S5_transfer >> trigger_email >> end_task

# weekly_load:

Daily_check_brancher >> Weekly_tasks \
>> load_backorder_sales_table >> load_SalesDaily_table >> end_branching_daily >> load_SalesDaily_table_to_gcs \
>> end_branching_weekly >> load_Rejects_table
Weekly_tasks >> load_OOActuals_table >> load_OOActuals_table_to_gcs >> end_branching_weekly >> load_Rejects_table
Weekly_tasks >> load_StyleDayLoc_table >> load_StyleDayLoc_table_to_gcs >> end_branching_weekly >> load_Rejects_table
Weekly_tasks >> load_ActualsInventory_table >> load_ActualsInventory_table_to_gcs >> end_branching_weekly >> load_Rejects_table
Weekly_tasks >> load_NetworkZip_table >> load_NetworkZip_table_to_gcs \
>> load_NetworkLoc_table >> load_NetworkLoc_table_to_gcs >> end_branching_weekly >> load_Rejects_table
load_Rejects_table >> load_RDYFile_table >> load_RDYFile_table_to_gcs >>S5_to_S5_transfer >> trigger_email >> end_task