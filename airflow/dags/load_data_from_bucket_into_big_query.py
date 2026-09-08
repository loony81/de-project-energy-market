from airflow.decorators import dag, task
import ast
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

OWNER = Variable.get("owner")
EMAIL = Variable.get("receiver_email")

default_args = {
    'owner': OWNER,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'email': EMAIL,
    'email_on_failure': True
}


@dag(dag_id='load_data_from_bucket_into_big_query', 
     default_args=default_args, 
     start_date=datetime(2026, 4, 2), 
     schedule_interval=None,  # This dag will be triggered by the first DAG
     catchup=False)
def load_data_from_gcs_to_bq():

    # One single task that handles everything in a loop
    @task
    def load_all_data_to_bigquery(**context):
        # 1. Get the data from the first dag
        conf = context['dag_run'].conf.get('resources_to_load', [])
        # If Airflow passed it as a string, convert it back to a list
        if isinstance(conf, str):
            # ast.literal_eval is safer than json.loads for Python-style strings
            conf = ast.literal_eval(conf)
        #  Get the date from the context in YYYY/MM/DD format to match the folder structure and filename inside the bucket
        # Since this dag is triggered by another dag and there is no schedule interval, the logical_date will be the actual date and
        # time when it is triggered, so it must match the date in the first dag where we used datetime.now() to define the path 
        # to a json file in the bucket
        execution_date = context['logical_date']
        partition_path = execution_date.strftime('%Y/%m')
        filename = execution_date.strftime('%d')
        
        hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        project_id = "etl-project-1-483215"
        
        # 2. Loop through each resource
        for r in conf:
            filepath = f"raw/{r['source']}/{r['commodity']}/{partition_path}/{filename}.json"
            print(f"Starting load for {r['table']} from {filepath}")
            # define specific fields before building schema
            if r['commodity'] == 'UREA_USD':
                specific_fields = [                                        
                    {"name": "unit", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "type", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
		            {"name": "formatted", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "as_of", "type": "TIMESTAMP", "mode": "NULLABLE"},
                    {"name": "collected_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                    {"name": "synthetic", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "stale", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "age_days", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "changes_omitted", "type": "STRING", "mode": "NULLABLE"}
                ]
            else:
                specific_fields = [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},           
                    {"name": "change_24h", "type": "STRING", "mode": "NULLABLE"}
                ]
            common_fields = [
                {"name": "source", "type": "STRING", "mode": "NULLABLE"},
                {"name": "code", "type": "STRING", "mode": "NULLABLE"},
                {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "price", "type": "STRING", "mode": "NULLABLE"},# force it to be a STRING to prevent mismatch errors later
                {"name": "currency", "type": "STRING", "mode": "NULLABLE"}
            ]
            # complete the schema by combining specific fields with common fields
            all_fields = specific_fields + common_fields
            # define the BigQuery Load Job configuration
            job_config = {
                "load": {
                    "sourceUris": [f"gs://fuel-and-fertilizers-market-elt/{filepath}"],
                    "destinationTable": {
                        "projectId": project_id,
                        "datasetId": "energy_market_raw",
                        "tableId": r['table']
                    },
                    "sourceFormat": "NEWLINE_DELIMITED_JSON",
                    "writeDisposition": "WRITE_APPEND", # Appends new data to the table (task runs once a day)
                    #"autodetect": False, # prevent BigQuery from guessing the data types                 
                    "schema": {
                        "fields": all_fields
                    }
                }
            }
            
            # 3. Trigger the job and wait for it to finish
            job = hook.insert_job(configuration=job_config)
            job.result() # This waits for the specific load to complete
            print(f"Successfully loaded to {r['table']} table")

    

    load_data = load_all_data_to_bigquery.override(task_id="load_data")()

    notify_failure = EmailOperator(
        task_id="send_failure_email",
        to=EMAIL,
        subject="Fuel and fertilizers Market Analysis. DAG 2 - Loading data from GCS bucket to BigQuery failed",
        html_content="There was an error during data transfer from the bucket to BigQuery. There is no need to trigger the third dag. Aborting. See the logs for more information.",
        trigger_rule=TriggerRule.ONE_FAILED
    )
    #The Trigger for the dbt DAG
    trigger_dag3 = TriggerDagRunOperator(
        task_id='trigger_dbt',
        trigger_dag_id='perform_dbt_transformations',
        wait_for_completion=False
    )
    # proceed to dag3 in case the load task succeeds, or send a failure notification email in case it fails
    load_data >> [trigger_dag3, notify_failure]
dag = load_data_from_gcs_to_bq()
