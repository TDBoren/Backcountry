import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.operators.dummy_operator import DummyOperator
import pendulum
import os
from google.cloud import bigquery
from google.cloud import storage
import csv
import yaml
import pysftp

local_tz = pendulum.timezone('America/Denver')
bigquery_client = bigquery.Client()
storage_client = storage.Client()
yamlfile = ''

env_type = os.environ['env_type']
dataset_id="xstore"
_now = datetime.datetime.today()
now = _now.strftime("%m%d%Y")
now = '_' + now

if env_type == 'dev':
    yamlfile = '/home/airflow/gcs/dags/xstore/yaml/xstore_fact.yaml'

elif env_type == 'prod':
    yamlfile = '/home/airflow/gcs/dags/xstore/yaml/xstore_fact.yaml'

with open(yamlfile, 'r') as key_file:
    configDict = yaml.safe_load(key_file.read())
    config = configDict['xstore']
    sql_inventory_sales_order = config['sql_inventory_sales_order']
    sqlfolder = config['sql_folder']
    sql_store = config['sql_store']
    sql_feed_inventory = config['sql_feed_inventory']
    sql_update_inventory = config['sql_update_inventory']
    inventory_name = config['inventory_name']
    sql_header_inventory = config['sql_header_inventory']
    sql_sequence_inventory = config['sql_sequence_inventory']
    gcp_bucket = config['gcp_bucket']
    sh_host = config['sh_host']
    sh_user = config['sh_user']
    sh_password = config['sh_password']
    sh_path = config['sh_path']
    fact_files = config['fact_files']
    hist_bucket = config['hist_bucket']
    vm_env = config['vm_env']

if env_type == 'dev':
    dataset_id="xstore"+"_"+ env_type
    gcp_bucket=gcp_bucket+"_"+ env_type
    hist_bucket=hist_bucket+"_"+ env_type
    vm_env=vm_env+"-"+ env_type
    
elif env_type == 'prod':
    dataset_id="xstore"
    gcp_bucket=gcp_bucket
    hist_bucket=hist_bucket
    vm_env=vm_env
    
def fn_process_store(**kwargs):
    param_dataset_id = kwargs['key5']
    client = bigquery.Client()

    secuence = ''
    fd = open(sqlfolder + kwargs['key4'], 'r')
    sqlFile = fd.read()
    fd.close()
    sqlFile=sqlFile.replace('final_dataset_id',param_dataset_id)
    query_job = client.query(
        sqlFile
    )
    result = query_job.result()

    for row in result:
        secuence = '0' + str(row[0])
        secuence = secuence[-2:]

    client = bigquery.Client()
    fd = open(sqlfolder + sql_store, 'r')
    sqlFile = fd.read()
    fd.close()
    sqlFile=sqlFile.replace('final_dataset_id',param_dataset_id)
    query_job = client.query(
        sqlFile
    )
    results = query_job.result()

    for row in results:
        fn_feed_to_csv(kwargs['key1'], str(row[0]), kwargs['key2'], kwargs['key3'], secuence, param_dataset_id)


def fn_feed_to_csv(_sql, _id, _name, _sql_header_inventory, _secuence, param_dataset_id):
    # key1 = sql_feed
    # key2 = file_name
    client = bigquery.Client()

    # get header
    fd = open(sqlfolder + _sql_header_inventory, 'r')
    sqlFile = fd.read()
    fd.close()
    sqlFile=sqlFile.replace('final_dataset_id',param_dataset_id)
    query_job = client.query(
        sqlFile
    )
    header = query_job.result()

    client2 = bigquery.Client()
    fd2 = open(sqlfolder + _sql, 'r')
    sqlFile2 = fd2.read()
    fd2.close()
    sqlFile2 = sqlFile2.replace('@st', _id)
    sqlFile2=sqlFile2.replace('final_dataset_id',param_dataset_id)
    query_job2 = client2.query(
        sqlFile2
    )
    # Waits for job to complete.
    results2 = query_job2.result()

    # Row values can be accessed by field name or index.
    print("No. of records in table: {}".format(results2.total_rows))
    n_rows = results2.total_rows

    if n_rows == 0:
        print("No records found")
    else:
        filename = inventory_name + str(_id) + now + '_DELTA_' + _secuence + '.csv'
        filename_done = inventory_name + str(_id) + now + '_DELTA_' + _secuence + '.done'

        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file, delimiter='|')
            writer.writerows(header)
            writer.writerows(results2)

        bucket = storage_client.bucket(gcp_bucket)
        blob = bucket.blob(filename)
        blob.upload_from_filename(filename)
        os.remove(filename)

        with open(filename_done, 'w', newline='') as file:
            writer = csv.writer(file, delimiter='|')

        bucket = storage_client.bucket(gcp_bucket)
        blob = bucket.blob(filename_done)
        blob.upload_from_filename(filename_done)
        os.remove(filename_done)


def fn_send_pgp_files_store_hub(**kwargs):
    # key1 = file name
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    bucket = storage_client.get_bucket(gcp_bucket)
    blobs = bucket.list_blobs()
    fact_files = kwargs['key1'].split(",")
    for blob in blobs:
        if blob.name[-3:] == 'pgp' and 'ASN_STORE' in blob.name:
            print(blob.name)
            x = blob.name.split("_")
            index = x.index('STORE')
            y = x[0:index + 1]
            z = "_".join(y)

            if z in fact_files:
                blobd = bucket.blob(blob.name)
                blobd.download_to_filename(blob.name)
                if os.path.getsize(blob.name) > 0:
                    print("SFTPing the file : ", blob.name)
                    with pysftp.Connection(host=sh_host, username=sh_user,
                                           password=sh_password, cnopts=cnopts) as sftp:
                        sftp.cwd(sh_path)
                        sftp.put(blob.name)
                        os.remove(blob.name)


def fn_send_done_files_store_hub(**kwargs):
    # key1 = file name
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    bucket = storage_client.get_bucket(gcp_bucket)
    blobs = bucket.list_blobs()
    fact_files = kwargs['key1'].split(",")
    for blob in blobs:
        if blob.name[-4:] == 'done' and 'ASN_STORE' in blob.name:
            print(blob.name)
            x = blob.name.split("_")
            index = x.index('STORE')
            y = x[0:index + 1]
            z = "_".join(y)

            if z in fact_files:
                blobd = bucket.blob(blob.name)
                blobd.download_to_filename(blob.name)
                print("SFTPing the file : ", blob.name)
                with pysftp.Connection(host=sh_host, username=sh_user,
                                       password=sh_password, cnopts=cnopts) as sftp:
                    sftp.cwd(sh_path)
                    sftp.put(blob.name)
                    os.remove(blob.name)


default_args = {
    'owner': 'Retail Store',
    'start_date': datetime.datetime(2022, 1, 13, tzinfo=local_tz),
    'email': ['datainsights-etl-failure@backcountry.pagerduty.com', 'biteam@backcountry.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'execution_timeout': timedelta(hours=6)
}

with DAG('XStore_Pos_Fact'
        , default_args=default_args
        , max_active_runs=1
        , schedule_interval="0 */2 * * *"  # "every 2 hours"
        , catchup=False) as dag:

    # Dag Start label
    start_task = DummyOperator(
        task_id='Start',
        trigger_rule='one_success',
    )

    csv_done_ssh_command = """
            ssh_bucket="{{ params.gcp_bucket }}"
            ssh_files="{{ params.fact_files }}"
            ssh_files_c=${ssh_files//","/ }
            echo $ssh_files_c;

            for files in ${ssh_files_c}
                do
                    gsutil -q stat gs://${ssh_bucket}/${files}*.csv;
                    status=$?;

                    if [[ $status == 0 ]] 
                    then
                    echo "Deleting ${files} csv file";
                    gsutil rm gs://${ssh_bucket}/${files}*.csv;

                    echo "Deleting ${files} done file";
                    gsutil rm gs://${ssh_bucket}/${files}*.done;
                    else	
                    echo "Filename containing ${files} does not exist."
                    fi
                done
            """

    delete_csv_done_files = BashOperator(
        task_id='delete_csv_done_files',
        bash_command=csv_done_ssh_command,
        params={'fact_files': fact_files, 'gcp_bucket': gcp_bucket},
        dag=dag
    )

    pgp_ssh_command = """
            ssh_date=`date +%Y%m%d`
            ssh_bucket="{{ params.gcp_bucket }}"
            ssh_hist="{{ params.hist_bucket }}"
            ssh_files="{{ params.fact_files }}"
            ssh_files_c=${ssh_files//","/ }
            echo $ssh_files_c;

            for files in ${ssh_files_c}
                do
                    gsutil -q stat gs://${ssh_bucket}/${files}*.pgp;
                    status=$?;

                    if [[ $status == 0 ]] 
                    then
                    echo "Archiving ${files} pgp files"
                    gsutil mv gs://${ssh_bucket}/${files}*.pgp gs://${ssh_hist}/${ssh_date}/
                    else	
                    echo "Filename containing ${files} does not exist."
                    fi
                done
            """

    move_pgp_files = BashOperator(
        task_id='move_pgp_files',
        bash_command=pgp_ssh_command,
        params={'fact_files': fact_files, 'gcp_bucket': gcp_bucket, 'hist_bucket': hist_bucket},
        dag=dag
    )

    create_sales_order = BigQueryOperator(
        dag=dag,
        task_id='create_sales_order',
        use_legacy_sql=False,
        priority='BATCH',
        sql=sql_inventory_sales_order
    )

    merge_inventory = BigQueryOperator(
        dag=dag,
        task_id='merge_inventory',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/merge_inventory.sql',
        params={'dataset_id': dataset_id}
    )

    hist_inventory = BigQueryOperator(
        dag=dag,
        task_id='hist_inventory',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/hist_inventory.sql',
        params={'dataset_id': dataset_id}
    )

    create_inventory_csv = PythonOperator(
        task_id="create_inventory_csv",
        python_callable=fn_process_store,
        op_kwargs={'key1': sql_feed_inventory, 'key2': inventory_name, 'key3': sql_header_inventory,
                   'key4': sql_sequence_inventory, 'key5': dataset_id},
        provide_context=True,
    )

    update_inventory_transaction_type = BigQueryOperator(
        dag=dag,
        task_id='update_inventory_transaction_type',
        use_legacy_sql=False,
        priority='BATCH',
        sql=sql_update_inventory,
        params={'dataset_id': dataset_id}
    )

    encrypt_files = BashOperator(
        task_id='encrypt_files',
        bash_command="""
            chmod 400 /home/airflow/gcs/plugins/{{ params.vm_env }}; \
            gcloud compute ssh  datauser@{{ params.vm_env }} \
            --zone us-central1-a \
            --ssh-key-file=/home/airflow/gcs/plugins/{{ params.vm_env }} \
            --command='python3 xstore_encrypt.py {{ params.fact_files }}'
        """,
        params={'fact_files': fact_files,'vm_env': vm_env},
        dag=dag
    )

    send_pgp_files_store_hub = PythonOperator(
        task_id="send_pgp_files_store_hub",
        python_callable=fn_send_pgp_files_store_hub,
        op_kwargs={'key1': fact_files},
        provide_context=True,
    )

    send_done_files_store_hub = PythonOperator(
        task_id="send_done_files_store_hub",
        python_callable=fn_send_done_files_store_hub,
        op_kwargs={'key1': fact_files},
        provide_context=True,
    )

    job_complete = DummyOperator(
        task_id='job_complete',
        dag=dag,
        trigger_rule='none_failed'
    )

start_task >> update_inventory_transaction_type >> create_sales_order >> merge_inventory >> create_inventory_csv \
>> hist_inventory >> encrypt_files >> send_pgp_files_store_hub >> send_done_files_store_hub >> delete_csv_done_files >> move_pgp_files >> job_complete