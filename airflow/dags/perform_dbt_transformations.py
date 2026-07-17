from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

OWNER = Variable.get("owner")
EMAIL = Variable.get("receiver_email")

# Define the paths
DBT_PROJECT_DIR = "/home/roman/energy_market_elt_proj"
DBT_PROFILES_DIR = "/home/roman/.dbt"
DBT_EXECUTABLE = f"/home/roman/dbt_venv/bin/dbt"

default_args = {
    'owner': OWNER,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'email': EMAIL,
    'email_on_failure': True
}


@dag(dag_id='perform_dbt_transformations', 
     default_args=default_args, 
     description='This is a dag that executes dbt build command',
     start_date=datetime(2026, 4, 2), 
     schedule_interval=None,  # This dag will be triggered by the second DAG
     catchup=False)
def perform_dbt_transformations():

    run_dbt = BashOperator(
        task_id="run_dbt_build",
        bash_command=f"{DBT_EXECUTABLE} build --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
    )
 
    notify_failure = EmailOperator(
        task_id="send_failure_email",
        to=EMAIL,
        subject="Fuel and fertilizers Market Analysis. DAG 3 - Running dbt build failed",
        html_content="There was an error trying to execute the 'dbt build' command. See the logs for more information.",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    run_dbt >> notify_failure
   

dag = perform_dbt_transformations()
