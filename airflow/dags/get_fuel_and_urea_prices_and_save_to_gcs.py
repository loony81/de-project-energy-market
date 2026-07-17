import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator


OWNER = Variable.get("owner")
EMAIL = Variable.get("receiver_email")
OILPRICE_API_KEY = Variable.get('oilprice_key')

default_args = {
    'owner': OWNER,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'email': EMAIL,
    'email_on_failure': True
}


# 1. Generic Function for getting a commodity from Oil Price API
def fetch_commodity_from_oilprice_api(commodity):
    # first we check if the commodity is urea, if it is then we need to pass the api key with a request,
    # if it's not, then we just send a request to the free demo endpoint that gets prices for oil and gas 
    if commodity == 'UREA_USD':
        url = "https://api.oilpriceapi.com/v1/prices/latest?by_code=UREA_USD"
        headers = {
            'Authorization': f'Token {OILPRICE_API_KEY}',
            'Content-Type': 'application/json'
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            # Check if the key we need exists in payload
            if not data or 'price' not in data['data']:
                print(f"API returned 200 but payload is invalid/empty, trying backup api ...")
                data = try_backup('urea')
                if data == None:
                    raise AirflowException("Couldn't get data from backup api ...")
                return data
            # before sending data to BigQuery, we need to turn regular json format into Newline Delimited JSON
            # Since each task fetches a single price, our json file is technically just a single JSON object. 
            # We just need to discard some unnecessary metadata
            ndjson = data.get('data')
            ndjson.pop('changes', None)
            ndjson.pop('metadata', None)
            ndjson.pop('data_status', None)
            ndjson.pop('freshness', None)
            return ndjson
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}, trying another api ...")  # e.g., 404 or 500
            data = try_backup('urea')
            if data == None:
                raise AirflowException("Couldn't get data from backup api ...")
            return data
        except requests.exceptions.RequestException as e:
            # Network issues, timeouts
            raise AirflowException(f"Connection issues: {e}")
    else:
        url = "https://api.oilpriceapi.com/v1/demo/prices"
        headers = {
            'Content-Type': 'application/json'
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            # Check if the key we need exists in payload
            if not data or 'prices' not in data['data']:
                print(f"API returned 200 but payload is invalid/empty, trying backup api ...")
                data = try_backup(commodity)
                if data == None:
                    raise AirflowException("Couldn't get data from backup api ...")
                return data
            prices = data['data']['prices'] 
            for item in prices:
                if item['code'] == commodity:
                    # before sending data to BigQuery, we need to turn regular json format into Newline Delimited JSON
                    # Since each task fetches a single price, our json file is technically just a single JSON object. 
                    return item
            print(f"API returned 200 but there is no data for {commodity} in the payload, trying backup api ...")
            data = try_backup(commodity)
            if data == None:
                raise AirflowException("Couldn't get data from backup api ...")
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}, trying another api ...")  # e.g., 404 or 500
            data = try_backup(commodity)
            if data == None:
                raise AirflowException("Couldn't get data from backup api ...")
            return data
        except requests.exceptions.RequestException as e:
            # Network issues, timeouts
            raise AirflowException(f"Connection issues: {e}")


# 2. A buckup strategy in case data from Oil Price API is not available (there is a quota limit of 50 requests per months)
def try_backup(commodity):
    # backup strategy hasn't been implemented yet
    return None

# 3. Upload data to GCS bucket
def upload_to_gcs(data,file_path):
    hook = GCSHook()
    hook.upload(
        bucket_name="fuel-and-fertilizers-market-elt",
        object_name=file_path,
        data=json.dumps(data)
    )
    
# 4. We need to know which commodities have been successfully added to the gcs bucket, so that to send this information 
# to the next dag
def get_successful_resources(**context):
    # Map task_ids in this dag to resource configuration to send to the second dag
    resource_map = {
        'fetch_wti': {"source": "oilprice", "commodity": "WTI_USD", "table": "wti_oil"},
        'fetch_brent': {"source": "oilprice", "commodity": "BRENT_CRUDE_USD", "table": "brent_oil"},
        'fetch_nat_gas': {"source": "oilprice", "commodity": "NATURAL_GAS_USD", "table": "natural_gas"},
        'fetch_urea': {"source": "oilprice", "commodity": "UREA_USD", "table": "urea"},
    }
    
    # get a list of TaskInstance objects belonging to this dag
    tis = context['dag_run'].get_task_instances()
    # filter for only the successful ones from the resource_map
    successful_data = [
        resource_map[ti.task_id] 
        for ti in tis 
        if ti.task_id in resource_map and ti.state == 'success'
    ]
    
    # if successful_data list is empty, stop here and prevent the second dag from executing
    # it means no commodity was added to the gcs bucket during this dag run, so there's no point triggering the second dag
    if not successful_data:
        return 'send_failure_email'

    # Push the list to XCom so the TriggerDagRunOperator can grab it
    context['ti'].xcom_push(key='payload', value=successful_data)
    return 'trigger_bigquery_loader'

@dag(dag_id='get_data_from_apis', 
     default_args=default_args, 
     start_date=datetime(2026, 4, 2), 
     schedule_interval='15 23 * * *', # run this dag at 11:15 pm UTC every day
     catchup=False)
def get_data_from_apis():

    @task()
    def upload_commodity_to_gcs(commodity):
        # Define GCS Path: raw/source/commodity/year/month/day.json
        dt = datetime.now()
        file_path = f"raw/oilprice/{commodity}/{dt.strftime('%Y/%m/%d')}.json"
        data = fetch_commodity_from_oilprice_api(commodity)
        upload_to_gcs(data,file_path)
        return file_path

    t1 = upload_commodity_to_gcs.override(task_id="fetch_wti")('WTI_USD')
    t2 = upload_commodity_to_gcs.override(task_id="fetch_brent")('BRENT_CRUDE_USD')
    t3 = upload_commodity_to_gcs.override(task_id="fetch_nat_gas")('NATURAL_GAS_USD')
    t4 = upload_commodity_to_gcs.override(task_id="fetch_urea")('UREA_USD')

    
    # The BranchPythonOperator is used for conditional branching in a DAG, where the execution path depends on the output of a Python callable. 
    # The callable must return the task_id of the downstream task(s) to be executed next.
    branch_operator = BranchPythonOperator(
        task_id='gatekeeper',
        python_callable=get_successful_resources,
        # this task will fire after all parent tasks have finished running regardless of their status
        trigger_rule=TriggerRule.ALL_DONE 
    )

    # The Trigger for the Loader DAG
    trigger_dag2 = TriggerDagRunOperator(
        task_id='trigger_bigquery_loader',
        trigger_dag_id='load_data_from_bucket_into_big_query',
        # Pull the list from the gatekeeper's XCom and pass it as conf so that we can pick it up in the second dag
        conf={"resources_to_load": "{{ ti.xcom_pull(task_ids='gatekeeper', key='payload') }}"},
        wait_for_completion=False
    )

    notify_failure = EmailOperator(
        task_id="send_failure_email",
        to=EMAIL,
        subject="Fuel and fertilizers Market Analysis. DAG 1 - Fetching data from Oil Price API failed",
        html_content="No data was fetched from Oil Price API. There is no need to trigger the second dag. Aborting."
    )
    # Define Dependency Flow
    # The first four tasks will run without any particular order, and because there is no dependency chain, 
    # a failure in one task has zero impact on the others. The gatekeeper task will be triggered 
    # after all the tasks in the list are finished running (we defined it explicitly with the trigger rule), 
    # regardless of their success, and it will decide whether to trigger the second dag or to send a failure notification email,
    # based on the result from the get_successful_resources function
    [t1, t2, t3, t4] >> branch_operator >> [trigger_dag2, notify_failure]
dag = get_data_from_apis()
